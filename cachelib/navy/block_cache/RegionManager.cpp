/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachelib/navy/block_cache/RegionManager.h"

#include "cachelib/common/inject_pause.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook::cachelib::navy {
RegionManager::RegionManager(uint32_t numRegions,
                             uint64_t regionSize,
                             uint64_t baseOffset,
                             Device& device,
                             uint32_t numCleanRegions,
                             uint32_t numWorkers,
                             uint32_t stackSize,
                             RegionEvictCallback evictCb,
                             RegionCleanupCallback cleanupCb,
                             std::unique_ptr<EvictionPolicy> policy,
                             uint32_t numInMemBuffers,
                             uint16_t numPriorities,
                             uint16_t inMemBufFlushRetryLimit)
    : numPriorities_{numPriorities},
      inMemBufFlushRetryLimit_{inMemBufFlushRetryLimit},
      numRegions_{numRegions},
      regionSize_{regionSize},
      baseOffset_{baseOffset},
      device_{device},
      policy_{std::move(policy)},
      regions_{std::make_unique<std::unique_ptr<Region>[]>(numRegions)},
      numCleanRegions_{numCleanRegions},
      evictCb_{evictCb},
      cleanupCb_{cleanupCb},
      numInMemBuffers_{numInMemBuffers},
      placementHandle_{device_.allocatePlacementHandle()} {
  XLOGF(INFO, "{} regions, {} bytes each", numRegions_, regionSize_);
  for (uint32_t i = 0; i < numRegions; i++) {
    regions_[i] = std::make_unique<Region>(RegionId{i}, regionSize_);
  }

  XDCHECK_LT(0u, numInMemBuffers_);

  for (uint32_t i = 0; i < numInMemBuffers_; i++) {
    buffers_.push_back(
        std::make_unique<Buffer>(device.makeIOBuffer(regionSize_)));
  }

  for (uint32_t i = 0; i < numWorkers; i++) {
    auto name = fmt::format("region_manager_{}", i);
    workers_.emplace_back(
        std::make_unique<NavyThread>(name, NavyThread::Options(stackSize)));
    workers_.back()->addTaskRemote(
        [name]() { XLOGF(INFO, "{} started", name); });
  }
  resetEvictionPolicy();
}

RegionId RegionManager::evict() {
  auto rid = policy_->evict();
  if (!rid.valid()) {
    XLOG(ERR, "Eviction failed");
  } else {
    XLOGF(DBG, "Evict {}", rid.index());
  }
  return rid;
}

void RegionManager::touch(RegionId rid) {
  auto& region = getRegion(rid);
  XDCHECK_EQ(rid, region.id());
  if (!region.hasBuffer()) {
    policy_->touch(rid);
  }
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
    std::lock_guard<TimedMutex> lock{cleanRegionsMutex_};
    // Reset is inherently single threaded. All pending jobs, including
    // reclaims, have to be finished first.
    XDCHECK_EQ(reclaimsOutstanding_, 0u);
    cleanRegions_.clear();
    if (cleanRegionsCond_.numWaiters() > 0) {
      cleanRegionsCond_.notifyAll();
    }
  }
  seqNumber_.store(0, std::memory_order_release);

  // Reset eviction policy
  resetEvictionPolicy();
}

Region::FlushRes RegionManager::flushBuffer(const RegionId& rid) {
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
  return region.flushBuffer(std::move(callBack));
}

void RegionManager::detachBuffer(const RegionId& rid) {
  auto& region = getRegion(rid);
  // detach buffer can return nullptr if there are active readers
  auto buf = region.detachBuffer();
  XDCHECK(!!buf);
  returnBufferToPool(std::move(buf));
}

void RegionManager::cleanupBufferOnFlushFailure(const RegionId& regionId) {
  auto& region = getRegion(regionId);
  auto callBack = [this](RegionId rid, BufferView buffer) {
    cleanupCb_(rid, buffer);
    numInMemBufWaitingFlush_.dec();
    numInMemBufFlushFailures_.inc();
  };

  // This is no-op if the buffer is already cleaned up.
  region.cleanupBuffer(std::move(callBack));
  detachBuffer(regionId);
}

void RegionManager::releaseCleanedupRegion(RegionId rid) {
  auto& region = getRegion(rid);
  // Subtract the wasted bytes in the end
  externalFragmentation_.sub(getRegion(rid).getFragmentationSize());

  // Full barrier because we cannot have seqNumber_.fetch_add() re-ordered
  // below region.reset(). It is similar to the full barrier in openForRead.
  seqNumber_.fetch_add(1, std::memory_order_acq_rel);

  // Reset all region internal state, making it ready to be
  // used by a region allocator.
  region.reset();
  {
    std::lock_guard<TimedMutex> lock{cleanRegionsMutex_};
    cleanRegions_.push_back(rid);
    INJECT_PAUSE(pause_blockcache_clean_free_locked);
    if (cleanRegionsCond_.numWaiters() > 0) {
      cleanRegionsCond_.notifyAll();
    }
  }
}

std::pair<OpenStatus, std::unique_ptr<CondWaiter>>
RegionManager::assignBufferToRegion(RegionId rid, bool addWaiter) {
  XDCHECK(rid.valid());
  auto [buf, waiter] = claimBufferFromPool(addWaiter);
  if (!buf) {
    XLOG_EVERY_MS(ERR, 10'000) << fmt::format(
        "Failed to assign buffers. All buffers({}) are being used",
        numInMemBuffers_);
    return {OpenStatus::Retry, std::move(waiter)};
  }

  auto& region = getRegion(rid);
  region.attachBuffer(std::move(buf));
  return {OpenStatus::Ready, std::move(waiter)};
}

std::pair<std::unique_ptr<Buffer>, std::unique_ptr<CondWaiter>>
RegionManager::claimBufferFromPool(bool addWaiter) {
  std::unique_ptr<Buffer> buf;
  {
    std::lock_guard<TimedMutex> bufLock{bufferMutex_};
    if (buffers_.empty()) {
      std::unique_ptr<CondWaiter> waiter;
      if (addWaiter) {
        waiter = std::make_unique<CondWaiter>();
        bufferCond_.addWaiter(waiter.get());
      }
      return {nullptr, std::move(waiter)};
    }
    buf = std::move(buffers_.back());
    buffers_.pop_back();
  }
  numInMemBufActive_.inc();
  return {std::move(buf), nullptr};
}

std::pair<OpenStatus, std::unique_ptr<CondWaiter>>
RegionManager::getCleanRegion(RegionId& rid, bool addWaiter) {
  auto status = OpenStatus::Retry;
  std::unique_ptr<CondWaiter> waiter;
  uint32_t newSched = 0;
  {
    std::lock_guard<TimedMutex> lock{cleanRegionsMutex_};
    if (!cleanRegions_.empty()) {
      rid = cleanRegions_.back();
      cleanRegions_.pop_back();
      INJECT_PAUSE(pause_blockcache_clean_alloc_locked);
      status = OpenStatus::Ready;
    } else {
      if (addWaiter) {
        waiter = std::make_unique<CondWaiter>();
        cleanRegionsCond_.addWaiter(waiter.get());
      }
      status = OpenStatus::Retry;
    }
    auto plannedClean = cleanRegions_.size() + reclaimsOutstanding_;
    if (plannedClean < numCleanRegions_) {
      newSched = numCleanRegions_ - plannedClean;
      reclaimsOutstanding_ += newSched;
    }
  }

  for (uint32_t i = 0; i < newSched; i++) {
    startReclaim();
  }

  if (status == OpenStatus::Ready) {
    XDCHECK(!waiter);
    std::tie(status, waiter) = assignBufferToRegion(rid, addWaiter);
    if (status != OpenStatus::Ready) {
      std::lock_guard<TimedMutex> lock{cleanRegionsMutex_};
      cleanRegions_.push_back(rid);
      INJECT_PAUSE(pause_blockcache_clean_free_locked);
      if (cleanRegionsCond_.numWaiters() > 0) {
        cleanRegionsCond_.notifyAll();
      }
    }
  } else if (status == OpenStatus::Retry) {
    cleanRegionRetries_.inc();
  }

  return {status, std::move(waiter)};
}

void RegionManager::doFlush(RegionId rid, bool async) {
  // We're wasting the remaining bytes of a region, so track it for stats
  externalFragmentation_.add(getRegion(rid).getFragmentationSize());

  getRegion(rid).setPendingFlush();
  numInMemBufWaitingFlush_.inc();

  if (!async || folly::fibers::onFiber()) {
    doFlushInternal(rid);
  } else {
    getNextWorker().addTaskRemote([this, rid]() { doFlushInternal(rid); });
  }
}

void RegionManager::doFlushInternal(RegionId rid) {
  INJECT_PAUSE(pause_flush_begin);
  int retryAttempts = 0;
  while (retryAttempts < inMemBufFlushRetryLimit_) {
    auto res = flushBuffer(rid);
    if (res == Region::FlushRes::kSuccess) {
      break;
    } else if (res == Region::FlushRes::kRetryDeviceFailure) {
      // We have a limited retry limit for flush errors due to device
      retryAttempts++;
      numInMemBufFlushRetries_.inc();
    }

    // Device write failed; retry after 100ms
    folly::fibers::Baton b;
    b.try_wait_for(std::chrono::milliseconds(100));
  }

  if (retryAttempts >= inMemBufFlushRetryLimit_) {
    // Flush failure reaches retry limit, stop flushing and start to
    // clean up the buffer.
    cleanupBufferOnFlushFailure(rid);
    releaseCleanedupRegion(rid);
    INJECT_PAUSE(pause_flush_failure);
    return;
  }

  INJECT_PAUSE(pause_flush_detach_buffer);
  detachBuffer(rid);

  // Flush completed, track the region
  track(rid);
  INJECT_PAUSE(pause_flush_done);
  return;
}

void RegionManager::startReclaim() {
  getNextWorker().addTaskRemote([&]() { doReclaim(); });
}

void RegionManager::doReclaim() {
  RegionId rid;
  INJECT_PAUSE(pause_reclaim_begin);
  while (true) {
    rid = evict();
    // evict() can fail to find a victim, where it needs to be retried
    if (rid.valid()) {
      break;
    }
    // This should never happen
    XDCHECK(false);
  }

  const auto startTime = getSteadyClock();
  auto& region = getRegion(rid);
  bool status = region.readyForReclaim(true);
  XDCHECK(status);

  // We know now we're the only thread working with this region.
  // Hence, it's safe to access @Region without lock.
  if (region.getNumItems() != 0) {
    XDCHECK(!region.hasBuffer());
    auto desc = RegionDescriptor::makeReadDescriptor(
        OpenStatus::Ready, RegionId{rid}, true /* physRead */);
    auto sizeToRead = region.getLastEntryEndOffset();
    auto buffer = read(desc, RelAddress{rid, 0}, sizeToRead);
    if (buffer.size() != sizeToRead) {
      // TODO: remove when we fix T95777575
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
  INJECT_PAUSE(pause_reclaim_done);
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
  // 3r has mutex::lock() at the beginning so, it prevents 4r from being
  // reordered above it.
  //
  // We also need to ensure 3x is not re-ordered below 4x. This is handled
  // by a acq_rel memory order in 3x. See releaseEvictedRegion() for details.
  //
  // Finally, 4r has acquire semantic which will sychronizes-with 3x's acq_rel.
  if (seqNumber_.load(std::memory_order_acquire) != seqNumber) {
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

  // Full barrier because we cannot have seqNumber_.fetch_add() re-ordered
  // below region.reset(). If it is re-ordered then, we can end up with a data
  // race where a read returns stale data. See openForRead() for details.
  seqNumber_.fetch_add(1, std::memory_order_acq_rel);

  // Reset all region internal state, making it ready to be
  // used by a region allocator.
  region.reset();
  {
    std::lock_guard<TimedMutex> lock{cleanRegionsMutex_};
    reclaimsOutstanding_--;
    cleanRegions_.push_back(rid);
    INJECT_PAUSE(pause_blockcache_clean_free_locked);
    if (cleanRegionsCond_.numWaiters() > 0) {
      cleanRegionsCond_.notifyAll();
    }
  }
  reclaimTimeCountUs_.add(toMicros(getSteadyClock() - startTime).count());
  reclaimCount_.inc();
}

void RegionManager::doEviction(RegionId rid, BufferView buffer) const {
  INJECT_PAUSE(pause_do_eviction_start);
  if (buffer.isNull()) {
    XLOGF(ERR, "Error reading region {} on reclamation", rid.index());
  } else {
    const auto evictStartTime = getSteadyClock();
    XLOGF(DBG, "Evict region {} entries", rid.index());
    auto numEvicted = evictCb_(rid, buffer);
    XLOGF(DBG,
          "Evict region {} entries: {} us",
          rid.index(),
          toMicros(getSteadyClock() - evictStartTime).count());
    evictedCount_.add(numEvicted);
  }
  INJECT_PAUSE(pause_do_eviction_done);
}

void RegionManager::persist(RecordWriter& rw) const {
  serialization::RegionData regionData;
  *regionData.regionSize() = regionSize_;
  regionData.regions()->resize(numRegions_);
  for (uint32_t i = 0; i < numRegions_; i++) {
    auto& regionProto = regionData.regions()[i];
    *regionProto.regionId() = i;
    *regionProto.lastEntryEndOffset() = regions_[i]->getLastEntryEndOffset();
    regionProto.priority() = regions_[i]->getPriority();
    *regionProto.numItems() = regions_[i]->getNumItems();
  }
  serializeProto(regionData, rw);
}

void RegionManager::recover(RecordReader& rr) {
  auto regionData = deserializeProto<serialization::RegionData>(rr);
  if (regionData.regions()->size() != numRegions_ ||
      static_cast<uint32_t>(*regionData.regionSize()) != regionSize_) {
    throw std::invalid_argument(
        "Could not recover RegionManager. Invalid RegionData.");
  }

  for (auto& regionProto : *regionData.regions()) {
    uint32_t index = *regionProto.regionId();
    if (index >= numRegions_ ||
        static_cast<uint32_t>(*regionProto.lastEntryEndOffset()) >
            regionSize_) {
      throw std::invalid_argument(
          "Could not recover RegionManager. Invalid RegionId.");
    }
    // To handle compatibility between different priorities. If the current
    // setup has fewer priorities than the last run, automatically downgrade
    // all higher priorties to the current max.
    if (numPriorities_ > 0 && regionProto.priority() >= numPriorities_) {
      regionProto.priority() = numPriorities_ - 1;
    }
    regions_[index] =
        std::make_unique<Region>(regionProto, *regionData.regionSize());
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
    if (regions_[i]->getNumItems() == 0) {
      track(RegionId{i});
    }
  }

  // Now track all non-empty regions. This should ensure empty regions are
  // pushed to the bottom for both LRU and FIFO policies.
  for (uint32_t i = 0; i < numRegions_; i++) {
    if (regions_[i]->getNumItems() != 0) {
      track(RegionId{i});
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
  if (!device_.write(physOffset, std::move(buf), placementHandle_)) {
    return false;
  }
  physicalWrittenCount_.add(bufSize);
  return true;
}

bool RegionManager::deviceWrite(RelAddress addr, BufferView view) {
  const auto bufSize = view.size();
  XDCHECK(isValidIORange(addr.offset(), bufSize));
  auto physOffset = physicalOffset(addr);
  if (!device_.write(physOffset, view, placementHandle_)) {
    return false;
  }
  physicalWrittenCount_.add(bufSize);
  return true;
}

void RegionManager::write(RelAddress addr, Buffer buf) {
  auto rid = addr.rid();
  auto& region = getRegion(rid);
  region.writeToBuffer(addr.offset(), buf.view());
}

Buffer RegionManager::read(const RegionDescriptor& desc,
                           RelAddress addr,
                           size_t size) const {
  auto rid = addr.rid();
  auto& region = getRegion(rid);
  // Do not expect to read beyond what was already written
  XDCHECK_LE(addr.offset() + size, region.getLastEntryEndOffset());
  if (!desc.isPhysReadMode()) {
    auto buffer = Buffer(size);
    XDCHECK(region.hasBuffer());
    region.readFromBuffer(addr.offset(), buffer.mutableView());
    return buffer;
  }
  XDCHECK(isValidIORange(addr.offset(), size));

  return device_.read(physicalOffset(addr), size);
}

void RegionManager::drain() {
  for (auto& worker : workers_) {
    worker->drain();
  }
}

void RegionManager::flush() {
  drain(); // Flush any pending reclaims
  device_.flush();
}

void RegionManager::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_reclaim", reclaimCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_reclaim_time", reclaimTimeCountUs_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_region_reclaim_errors",
          reclaimRegionErrors_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_evictions",
          evictedCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_num_regions", numRegions_);
  visitor("navy_bc_num_clean_regions", cleanRegions_.size());
  visitor("navy_bc_num_clean_region_retries", cleanRegionRetries_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_external_fragmentation", externalFragmentation_.get());
  visitor("navy_bc_physical_written", physicalWrittenCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_inmem_active", numInMemBufActive_.get());
  visitor("navy_bc_inmem_waiting_flush", numInMemBufWaitingFlush_.get());
  visitor("navy_bc_inmem_flush_retries", numInMemBufFlushRetries_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_inmem_flush_failures", numInMemBufFlushFailures_.get(),
          CounterVisitor::CounterType::RATE);
  policy_->getCounters(visitor);
}
} // namespace facebook::cachelib::navy
