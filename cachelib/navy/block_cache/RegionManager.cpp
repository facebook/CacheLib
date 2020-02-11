#include "cachelib/navy/block_cache/RegionManager.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
RegionManager::RegionManager(uint32_t numRegions,
                             uint64_t regionSize,
                             uint64_t baseOffset,
                             uint32_t blockSize,
                             Device& device,
                             uint32_t numCleanRegions,
                             JobScheduler& scheduler,
                             RegionEvictCallback evictCb,
                             std::vector<uint32_t> sizeClasses,
                             std::unique_ptr<EvictionPolicy> policy)
    : numRegions_{numRegions},
      regionSize_{regionSize},
      baseOffset_{baseOffset},
      blockSize_{blockSize},
      device_{device},
      policy_{std::move(policy)},
      regions_{std::make_unique<std::unique_ptr<Region>[]>(numRegions)},
      numFree_{numRegions},
      numCleanRegions_{numCleanRegions},
      scheduler_{scheduler},
      evictCb_{evictCb},
      sizeClasses_{sizeClasses} {
  XDCHECK_EQ(regionSize_ % blockSize_, 0u);
  XLOGF(INFO, "{} regions, {} bytes each", numRegions_, regionSize_);
  for (uint32_t i = 0; i < numRegions; i++) {
    regions_[i] = std::make_unique<Region>(RegionId{i}, regionSize_);
  }
}

RegionId RegionManager::getFree() {
  RegionId rid;
  if (numFree_ > 0) {
    rid = RegionId(numRegions_ - numFree_);
    numFree_--;
  }
  return rid;
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

void RegionManager::reset() {
  policy_->reset();
  for (uint32_t i = 0; i < numRegions_; i++) {
    std::lock_guard<std::mutex> lock{regionMutexes_[i % kNumLocks]};
    regions_[i]->reset();
  }
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    numFree_ = numRegions_;
    // Reset is inherently single threaded. All pending jobs, including
    // reclaims, have to be finished first.
    XDCHECK_EQ(reclaimsScheduled_, 0u);
    cleanRegions_.clear();
  }
  // Will be set back to true if no regions to reclaim found
  outOfRegions_.store(false, std::memory_order_release);
  seqNumber_.store(0, std::memory_order_release);
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
      if (outOfRegions_.load(std::memory_order_acquire)) {
        return OpenStatus::Error;
      }
      status = OpenStatus::Retry;
    }
    auto plannedClean = cleanRegions_.size() + reclaimsScheduled_;
    if (plannedClean < numCleanRegions_) {
      newSched = numCleanRegions_ - plannedClean;
      reclaimsScheduled_ += newSched;
    }
  }
  for (uint32_t i = 0; i < newSched; i++) {
    if (!outOfRegions_.load(std::memory_order_acquire)) {
      scheduler_.enqueue(
          [this] { return startReclaim(); }, "reclaim", JobType::Reclaim);
    }
  }
  return status;
}

// Tries to get a free region first, otherwise evicts one and schedules region
// cleanup job (which will add the region to the clean list).
JobExitCode RegionManager::startReclaim() {
  RegionId rid;
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    rid = getFree();
    if (rid.valid()) {
      reclaimsScheduled_--;
      cleanRegions_.push_back(rid);
      return JobExitCode::Done;
    }
  }
  rid = evict();
  if (!rid.valid()) {
    outOfRegions_.store(true, std::memory_order_release);
    return JobExitCode::Done;
  }
  scheduler_.enqueue(
      [this, rid] {
        const auto startTime = getSteadyClock();
        auto& region = getRegion(rid);
        {
          std::lock_guard<std::mutex> lock{getLock(rid)};
          if (!region.readyForReclaim()) {
            // Once a region is set exclusive, all future accesses will be
            // blocked. However there might still be accesses in-flight,
            // so we would retry if that's the case.
            return JobExitCode::Reschedule;
          }
        }
        // We know now we're the only thread working with this region.
        // Hence, it's safe to access @Region without lock.
        if (region.getNumItems() != 0) {
          auto buffer = makeIOBuffer(regionSize());
          if (!read(RelAddress{rid, 0}, buffer.mutableView())) {
            XLOGF(ERR, "Failed to read region {} during reclaim", rid.index());
            buffer = Buffer{};
          }
          doEviction(rid, buffer.view());
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
  std::lock_guard<std::mutex> lock{getLock(rid)};
  auto desc = region.openForRead();
  if (!desc.isReady()) {
    return desc;
  }
  // Region lock guarantees ordering
  if (seqNumber_.load(std::memory_order_relaxed) != seqNumber) {
    region.close(std::move(desc));
    return RegionDescriptor{OpenStatus::Retry};
  }
  return desc;
}

void RegionManager::close(RegionDescriptor&& desc) {
  RegionId rid = desc.id();
  auto& region = getRegion(rid);
  std::lock_guard<std::mutex> lock{getLock(rid)};
  region.close(std::move(desc));
}

void RegionManager::releaseEvictedRegion(RegionId rid,
                                         std::chrono::nanoseconds startTime) {
  auto& region = getRegion(rid);
  {
    std::lock_guard<std::mutex> lock{getLock(rid)};
    // Permanent item region (pinned) should not be reclaimed
    XDCHECK(!region.isPinned());
    // Region lock guarantees ordering
    seqNumber_.fetch_add(1, std::memory_order_relaxed);
    // Reset all region internal state, making it ready to be
    // used by a region allocator.
    region.reset();
  }
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

  // TODO we don't presereve the eviction policy information across restarts
  // and reinit the tracking.
  detectFree();
}

void RegionManager::detectFree() {
  XDCHECK_GT(numRegions_, 0u);
  std::lock_guard<std::mutex> crlock{cleanRegionsMutex_};
  numFree_ = 0;
  // Increment @numFree_ for every consecutive region starting from the back of
  // the array that has @lastEntryEndOffset == 0.
  for (uint32_t i = numRegions_; i-- > 0;) {
    std::lock_guard<std::mutex> lock{regionMutexes_[i % kNumLocks]};
    if (regions_[i]->getNumItems() == 0) {
      numFree_++;
    } else {
      break;
    }
  }
  // Track all regions that are not free
  for (uint32_t i = 0; i < numRegions_ - numFree_; i++) {
    std::lock_guard<std::mutex> lock{regionMutexes_[i % kNumLocks]};
    if (!regions_[i]->isPinned()) {
      track(RegionId{i});
    }
  }
  // Move all regions with items to the LRU head. Clean regions with 0 items
  // will group in the LRU tail.
  for (uint32_t i = 0; i < numRegions_ - numFree_; i++) {
    std::lock_guard<std::mutex> lock{regionMutexes_[i % kNumLocks]};
    if (!regions_[i]->isPinned()) {
      if (regions_[i]->getNumItems() != 0) {
        touch(RegionId{i});
      }
    }
  }
}

bool RegionManager::isValidIORange(uint32_t offset, uint32_t size) const {
  return offset % blockSize_ == 0 && size % blockSize_ == 0 &&
         uint64_t{offset} + size <= regionSize_;
}

bool RegionManager::write(RelAddress addr, BufferView buf) const {
  XDCHECK(isValidIORange(addr.offset(), buf.size()));
  auto physOffset = physicalOffset(addr);
  if (!device_.write(physOffset, buf.size(), buf.data())) {
    return false;
  }
  physicalWrittenCount_.add(buf.size());
  return true;
}

bool RegionManager::read(RelAddress addr, MutableBufferView buf) const {
  XDCHECK(isValidIORange(addr.offset(), buf.size()));
  auto physOffset = physicalOffset(addr);
  return device_.read(physOffset, buf.size(), buf.data());
}

void RegionManager::flush() const { device_.flush(); }

void RegionManager::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_reclaim", reclaimCount_.get());
  visitor("navy_bc_reclaim_time", reclaimTimeCountUs_.get());
  visitor("navy_bc_evicted", evictedCount_.get());
  visitor("navy_bc_pinned_regions", pinnedCount_.get());
  visitor("navy_bc_physical_written", physicalWrittenCount_.get());
  policy_->getCounters(visitor);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
