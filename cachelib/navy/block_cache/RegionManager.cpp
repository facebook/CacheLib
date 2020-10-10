#include "cachelib/navy/block_cache/RegionManager.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
RegionManager::RegionManager(uint32_t numRegions,
                             uint64_t regionSize,
                             uint64_t baseOffset,
                             Device& device,
                             uint32_t numCleanRegions,
                             JobScheduler& scheduler,
                             RegionEvictCallback evictCb,
                             std::vector<uint32_t> sizeClasses,
                             std::unique_ptr<EvictionPolicy> policy,
                             uint32_t numInMemBuffers)
    : numRegions_{numRegions},
      regionSize_{regionSize},
      baseOffset_{baseOffset},
      device_{device},
      policy_{std::move(policy)},
      regions_{std::make_unique<std::unique_ptr<Region>[]>(numRegions)},
      numCleanRegions_{numCleanRegions},
      scheduler_{scheduler},
      evictCb_{evictCb},
      sizeClasses_{sizeClasses},
      numInMemBuffers_{numInMemBuffers} {
  XLOGF(INFO, "{} regions, {} bytes each", numRegions_, regionSize_);
  for (uint32_t i = 0; i < numRegions; i++) {
    regions_[i] = std::make_unique<Region>(RegionId{i}, regionSize_);
  }
  if (doesBufferingWrites()) {
    for (uint32_t i = 0; i < numInMemBuffers_; i++) {
      buffers_.push_back(
          std::make_unique<Buffer>(device.makeIOBuffer(regionSize_)));
    }
  }
  resetEvictionPolicy();
}

RegionId RegionManager::evict() {
  auto rid = policy_->evict();
  if (!rid.valid()) {
    XLOG(ERR, "Eviction failed");
  } else {
    auto& region = getRegion(rid);
    XLOGF(DBG, "Evict {} class {}", rid.index(), region.getClassId());
  }
  return rid;
}

void RegionManager::track(RegionId rid) {
  auto& region = getRegion(rid);
  XDCHECK_EQ(rid, region.id());
  policy_->track(region);
}

void RegionManager::reset() {
  for (uint32_t i = 0; i < numRegions_; i++) {
    regions_[i]->reset();
  }
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    // Reset is inherently single threaded. All pending jobs, including
    // reclaims, have to be finished first.
    XDCHECK_EQ(reclaimsScheduled_, 0u);
    cleanRegions_.clear();
  }
  seqNumber_.store(0, std::memory_order_release);

  // Reset eviction policy
  resetEvictionPolicy();
}

// Caller is expected to call flushBuffer until true is returned.
// This routine is idempotent and is safe to call multiple times until
// detachBuffer is done.
bool RegionManager::flushBuffer(const RegionId& rid) {
  auto& region = getRegion(rid);
  auto callBack = [this](RelAddress addr, BufferView view) {
    auto writeBuffer = device_.makeIOBuffer(view.size());
    writeBuffer.copyFrom(0, view);
    if (!deviceWrite(addr, std::move(writeBuffer))) {
      return false;
    }
    numInMemBufWaitingFlush_.dec();
    return true;
  };

  // This is no-op if the buffer is already flushed
  if (!region.flushBuffer(std::move(callBack))) {
    return false;
  }
  // detach buffer can return nullptr if there are active readers
  auto buf = region.detachBuffer();
  if (buf) {
    returnBufferToPool(std::move(buf));
    // Flush completed, track the region if not pinned
    if (!region.isPinned()) {
      track(rid);
    }
    return true;
  }
  return false;
}

OpenStatus RegionManager::assignBufferToRegion(RegionId rid) {
  XDCHECK(rid.valid());
  auto buf = claimBufferFromPool();
  if (!buf) {
    return OpenStatus::Retry;
  }
  auto& region = getRegion(rid);
  region.attachBuffer(std::move(buf));
  return OpenStatus::Ready;
}

std::unique_ptr<Buffer> RegionManager::claimBufferFromPool() {
  std::unique_ptr<Buffer> buf;
  {
    std::lock_guard<std::mutex> bufLock{bufferMutex_};
    if (buffers_.empty()) {
      return nullptr;
    }
    buf = std::move(buffers_.back());
    buffers_.pop_back();
  }
  numInMemBufActive_.inc();
  return buf;
}

OpenStatus RegionManager::getCleanRegion(RegionId& rid) {
  auto status = OpenStatus::Retry;
  uint32_t newSched = 0;
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    if (!cleanRegions_.empty()) {
      rid = cleanRegions_.back();
      cleanRegions_.pop_back();
      status = OpenStatus::Ready;
    } else {
      status = OpenStatus::Retry;
    }
    auto plannedClean = cleanRegions_.size() + reclaimsScheduled_;
    if (plannedClean < numCleanRegions_) {
      newSched = numCleanRegions_ - plannedClean;
      reclaimsScheduled_ += newSched;
    }
  }
  for (uint32_t i = 0; i < newSched; i++) {
    scheduler_.enqueue(
        [this] { return startReclaim(); }, "reclaim", JobType::Reclaim);
  }
  if (doesBufferingWrites() && status == OpenStatus::Ready) {
    status = assignBufferToRegion(rid);
    if (status != OpenStatus::Ready) {
      std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
      cleanRegions_.push_back(rid);
    }
  }
  return status;
}

void RegionManager::doFlush(RegionId rid, bool async) {
  // We're wasiting the remaining bytes of a region, so track it for stats
  externalFragmentation_.add(getRegion(rid).getFragmentationSize());

  // applicable only if configured to use in-memory buffers
  if (!doesBufferingWrites()) {
    // If in-memory buffering is not enabled, nothing to flush and
    // track the region if not pinned. If in-memory buffer is enabled
    // tracking is started after flush is successful.
    if (!getRegion(rid).isPinned()) {
      track(rid);
    }
    return;
  }

  getRegion(rid).setPendingFlush();
  numInMemBufWaitingFlush_.inc();
  if (async) {
    scheduler_.enqueue(
        [this, rid] {
          if (flushBuffer(rid)) {
            return JobExitCode::Done;
          }
          return JobExitCode::Reschedule;
        },
        "flush",
        JobType::Flush);
  } else {
    while (!flushBuffer(rid)) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    return;
  }
}

// Tries to get a free region first, otherwise evicts one and schedules region
// cleanup job (which will add the region to the clean list).
JobExitCode RegionManager::startReclaim() {
  auto rid = evict();
  if (!rid.valid()) {
    return JobExitCode::Reschedule;
  }
  scheduler_.enqueue(
      [this, rid] {
        const auto startTime = getSteadyClock();
        auto& region = getRegion(rid);
        if (!region.readyForReclaim()) {
          // Once a region is set exclusive, all future accesses will be
          // blocked. However there might still be accesses in-flight,
          // so we would retry if that's the case.
          return JobExitCode::Reschedule;
        }
        // We know now we're the only thread working with this region.
        // Hence, it's safe to access @Region without lock.
        if (region.getNumItems() != 0) {
          XDCHECK(!region.hasBuffer());
          auto desc = RegionDescriptor::makeReadDescriptor(
              OpenStatus::Ready, RegionId{rid}, true /* physRead */);
          auto sizeToRead = region.getLastEntryEndOffset();
          auto buffer = read(desc, RelAddress{rid, 0}, sizeToRead);
          if (buffer.size() != sizeToRead) {
            // T68874972: We know this can happen today due to this bug.
            // For now, we will log this error and also bump a stat while we
            // work on a proper fix with proper tests.
            XLOGF(ERR,
                  "Failed to read region {} during reclaim. Region size to "
                  "read: {}, Actually read: {}",
                  rid.index(),
                  sizeToRead,
                  buffer.size());
            reclaimRegionErrors_.inc();
          } else {
            doEviction(rid, buffer.view());
          }
        }
        releaseEvictedRegion(rid, startTime);
        return JobExitCode::Done;
      },
      "reclaim.evict",
      JobType::Reclaim);
  return JobExitCode::Done;
}

RegionDescriptor RegionManager::openForRead(RegionId rid, uint64_t seqNumber) {
  auto& region = getRegion(rid);
  auto desc = region.openForRead();
  if (!desc.isReady()) {
    return desc;
  }

  // << Interaction of Region Lock and Sequence Number >>
  //
  // Reader:
  // 1r. Load seq number
  // 2r. Check index
  // 3r. Open region
  // 4r. Load seq number
  //     If hasn't changed, proceed to read and close region.
  //     Otherwise, abort read and close region.
  //
  // Reclaim:
  // 1x. Mark region ready for reclaim
  // 2x. Reclaim and evict entries from index
  // 3x. Store seq number
  // 4x. Reset region
  //
  // In order for these two sequence of operations to not have data race,
  // we must guarantee the following ordering:
  //   3r -> 4r
  //
  // We know that 3r either happens before 1x or happens after 4x, this
  // means with the above ordering, 4r will either:
  // 1. Read the same seq number and proceed to read
  //    (3r -> 4r -> (read item and close region) -> 1x)
  // 2. Or, read a different seq number and abort read (4x -> 3r -> 4r)
  // Either of the above is CORRECT operation.
  //
  // This means we must prevent 3r from reordered below 4r. We know at
  // the end of 3r we have a mutex::unlock(), but it is only equivalent
  // to a memory_order_release, which means anything above 3r cannot
  // be ordered down, but 4r can be ordered up! So we have to set a full
  // memory barrier here in the form of memory_order_acq_rel to make sure
  // 3r will stay exactly where it is to guarantee 3r -> 4r ordering.
  if (seqNumber_.load(std::memory_order_acq_rel) != seqNumber) {
    region.close(std::move(desc));
    return RegionDescriptor{OpenStatus::Retry};
  }
  return desc;
}

void RegionManager::close(RegionDescriptor&& desc) {
  RegionId rid = desc.id();
  auto& region = getRegion(rid);
  region.close(std::move(desc));
}

void RegionManager::releaseEvictedRegion(RegionId rid,
                                         std::chrono::nanoseconds startTime) {
  auto& region = getRegion(rid);
  // Subtract the wasted bytes in the end since we're reclaiming this region now
  externalFragmentation_.sub(getRegion(rid).getFragmentationSize());

  // Permanent item region (pinned) should not be reclaimed
  XDCHECK(!region.isPinned());
  // Full barrier because is we cannot have seqNumber_.fetch_add() re-ordered
  // below region.reset(). It is similar to the full barrier in openForRead.
  seqNumber_.fetch_add(1, std::memory_order_acq_rel);
  // Reset all region internal state, making it ready to be
  // used by a region allocator.
  region.reset();
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    reclaimsScheduled_--;
    cleanRegions_.push_back(rid);
  }
  reclaimTimeCountUs_.add(toMicros(getSteadyClock() - startTime).count());
  reclaimCount_.inc();
}

void RegionManager::doEviction(RegionId rid, BufferView buffer) const {
  if (buffer.isNull()) {
    XLOGF(ERR, "Error reading region {} on reclamation", rid.index());
  } else {
    const auto evictStartTime = getSteadyClock();
    XLOGF(DBG, "Evict region {} entries", rid.index());
    auto numEvicted = evictCb_(rid, getRegionSlotSize(rid), buffer);
    XLOGF(DBG,
          "Evict region {} entries: {} us",
          rid.index(),
          toMicros(getSteadyClock() - evictStartTime).count());
    evictedCount_.add(numEvicted);
  }
}

void RegionManager::persist(RecordWriter& rw) const {
  serialization::RegionData regionData;
  regionData.regionSize = regionSize_;
  regionData.regions.resize(numRegions_);
  for (uint32_t i = 0; i < numRegions_; i++) {
    auto& regionProto = regionData.regions[i];
    regionProto.regionId = i;
    regionProto.lastEntryEndOffset = regions_[i]->getLastEntryEndOffset();
    regionProto.pinned = regions_[i]->isPinned();
    if (regionProto.pinned) {
      regionProto.classId = 0;
    } else {
      regionProto.classId = regions_[i]->getClassId();
    }
    regionProto.priority_ref() = regions_[i]->getPriority();
    regionProto.numItems = regions_[i]->getNumItems();
  }
  serializeProto(regionData, rw);
}

void RegionManager::recover(RecordReader& rr) {
  auto regionData = deserializeProto<serialization::RegionData>(rr);
  if (regionData.regions.size() != numRegions_ ||
      static_cast<uint32_t>(regionData.regionSize) != regionSize_) {
    throw std::invalid_argument(
        "Could not recover RegionManager. Invalid RegionData.");
  }

  for (auto& regionProto : regionData.regions) {
    uint32_t index = regionProto.regionId;
    if (index >= numRegions_ ||
        static_cast<uint32_t>(regionProto.lastEntryEndOffset) > regionSize_) {
      throw std::invalid_argument(
          "Could not recover RegionManager. Invalid RegionId.");
    }
    regions_[index] = std::make_unique<Region>(
        regionProto,
        RegionId{static_cast<uint32_t>(regionProto.regionId)},
        regionData.regionSize);
    if (regionProto.pinned) {
      pin(*regions_[index]);
    }
  }

  // Reset policy and reinitialize it per the recovered state
  resetEvictionPolicy();
}

void RegionManager::resetEvictionPolicy() {
  XDCHECK_GT(numRegions_, 0u);

  policy_->reset();
  externalFragmentation_.set(0);

  // Go through all the regions, restore fragmentation size, and track all empty
  // regions
  for (uint32_t i = 0; i < numRegions_; i++) {
    externalFragmentation_.add(regions_[i]->getFragmentationSize());
    if (!regions_[i]->isPinned()) {
      if (regions_[i]->getNumItems() == 0) {
        track(RegionId{i});
      }
    }
  }

  // Now track all non-empty regions. This should ensure empty regions are
  // pushed to the bottom for both LRU and FIFO policies.
  for (uint32_t i = 0; i < numRegions_; i++) {
    if (!regions_[i]->isPinned()) {
      if (regions_[i]->getNumItems() != 0) {
        track(RegionId{i});
      }
    }
  }
}

bool RegionManager::isValidIORange(uint32_t offset, uint32_t size) const {
  return static_cast<uint64_t>(offset) + size <= regionSize_;
}

bool RegionManager::deviceWrite(RelAddress addr, Buffer buf) {
  const auto bufSize = buf.size();
  XDCHECK(isValidIORange(addr.offset(), bufSize));
  auto physOffset = physicalOffset(addr);
  if (!device_.write(physOffset, std::move(buf))) {
    return false;
  }
  physicalWrittenCount_.add(bufSize);
  return true;
}

bool RegionManager::write(RelAddress addr, Buffer buf) {
  if (doesBufferingWrites()) {
    auto rid = addr.rid();
    auto& region = getRegion(rid);
    region.writeToBuffer(addr.offset(), buf.view());
    return true;
  }
  return deviceWrite(addr, std::move(buf));
}

Buffer RegionManager::read(const RegionDescriptor& desc,
                           RelAddress addr,
                           size_t size) const {
  auto rid = addr.rid();
  auto& region = getRegion(rid);
  // Do not expect to read beyond what was already written
  XDCHECK_LE(addr.offset() + size, region.getLastEntryEndOffset());
  if (doesBufferingWrites() && !desc.isPhysReadMode()) {
    auto buffer = Buffer(size);
    XDCHECK(region.hasBuffer());
    region.readFromBuffer(addr.offset(), buffer.mutableView());
    return buffer;
  }
  XDCHECK(isValidIORange(addr.offset(), size));

  return device_.read(physicalOffset(addr), size);
}

void RegionManager::flush() { device_.flush(); }

void RegionManager::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_reclaim", reclaimCount_.get());
  visitor("navy_bc_reclaim_time", reclaimTimeCountUs_.get());
  visitor("navy_bc_region_reclaim_errors", reclaimRegionErrors_.get());
  visitor("navy_bc_evicted", evictedCount_.get());
  visitor("navy_bc_num_regions", numRegions_);
  visitor("navy_bc_num_clean_regions", cleanRegions_.size());
  visitor("navy_bc_pinned_regions", pinnedCount_.get());
  visitor("navy_bc_external_fragmentation", externalFragmentation_.get());
  visitor("navy_bc_physical_written", physicalWrittenCount_.get());
  visitor("navy_bc_inmem_active", numInMemBufActive_.get());
  visitor("navy_bc_inmem_waiting_flush", numInMemBufWaitingFlush_.get());
  policy_->getCounters(visitor);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
