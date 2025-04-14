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

#pragma once

#include <folly/memory/Malloc.h>

#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Throttler.h"
#include "cachelib/common/Utils.h"

namespace facebook::cachelib::objcache2 {
template <typename AllocatorT>
class ObjectCache;

enum ObjCacheSizeControlMode { FreeMemory, ResidentMemory, ObjectSize };

// Dynamically adjust the entriesLimit to limit the cache size for
// object-cache.
template <typename AllocatorT>
class ObjectCacheSizeController : public PeriodicWorker {
 public:
  using ObjectCache = ObjectCache<AllocatorT>;
  using GetFreeMemCb = std::function<uint64_t()>;
  using GetRSSMemCb = std::function<uint64_t()>;

  explicit ObjectCacheSizeController(
      ObjectCache& objCache, const util::Throttler::Config& throttlerConfig);
  size_t getCurrentEntriesLimit() const {
    return currentEntriesLimit_.load(std::memory_order_relaxed);
  }

  void getCounters(const util::CounterVisitor& visitor) const;
  size_t getTotalObjSizeBytes() const;

 private:
  void work() override final;
  void shrinkCacheByEntriesNum(size_t entries);
  void expandCacheByEntriesNum(size_t entries);
  int64_t getNewNumEntries();

  std::pair<size_t, size_t> trackJemallocMemStats() const {
    size_t jemallocAllocatedBytes;
    size_t jemallocActiveBytes;
    size_t epoch = 1;
    size_t sz;
    sz = sizeof(size_t);
    mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch));
    mallctl("stats.allocated", &jemallocAllocatedBytes, &sz, nullptr, 0);
    mallctl("stats.active", &jemallocActiveBytes, &sz, nullptr, 0);
    return {jemallocAllocatedBytes, jemallocActiveBytes};
  }

  // threshold in percentage to determine whether the size-controller should
  // do the calculation
  const size_t kSizeControllerThresholdPct = 50;

  const util::Throttler::Config throttlerConfig_;

  // reference to the object cache
  ObjectCache& objCache_;

  ObjCacheSizeControlMode mode_;

  // will be adjusted to control the cache size limit
  std::atomic<size_t> currentEntriesLimit_;

  // callback to get free memory in bytes
  GetFreeMemCb getFreeMem_;

  // callback to get RSS memory in bytes
  GetRSSMemCb getRSSMem_;
};

template <typename AllocatorT>
size_t ObjectCacheSizeController<AllocatorT>::getTotalObjSizeBytes() const {
  auto totalObjSize = objCache_.getTotalObjectSize();
  if (objCache_.config_.fragmentationTrackingEnabled &&
      folly::usingJEMalloc()) {
    auto [jemallocAllocatedBytes, jemallocActiveBytes] =
        trackJemallocMemStats();
    // proportionally add Jemalloc external fragmentation bytes (i.e.
    // jemallocActiveBytes - jemallocAllocatedBytes)
    totalObjSize = static_cast<size_t>(
        1.0 * totalObjSize / jemallocAllocatedBytes * jemallocActiveBytes);
  }
  return totalObjSize;
}

template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::work() {
  // This function adjusts number of entries allowed in the object cache
  // based on internal metrics (average object size, total object size, max
  // entries) and system metrics (free memory and RSS).
  auto totalObjSize = getTotalObjSizeBytes();
  auto currentNumEntries = objCache_.getNumEntries();
  if (currentNumEntries == 0) {
    return;
  }
  auto minEntriesWarmCacheThreshold =
      kSizeControllerThresholdPct * objCache_.config_.l1EntriesLimit / 100;
  auto totalObjectSizeWarmCacheThreshold =
      kSizeControllerThresholdPct * objCache_.config_.totalObjectSizeLimit /
      100;

  // Check if the cache is warming up.
  // A cache is considered to be warming up if:
  // 1. The total size of objects in the cache is less than the warm cache
  // threshold.
  // 2. The current number of entries in the cache is less than the minimum
  // entries warm cache threshold.
  // 3. There is space to grow, i.e., the current number of entries limit is
  // greater than the current number of entries.
  // If 1 and 2 are true but not 3, meaning very little items and bytes are
  // consumed, yet the cache cannot grow since (currentEntriesLimit_ ==
  // currentNumEntries), this could be because the cache shrunk to adjust for
  // large items and then the item size suddenly dropped. Without checking for
  // 3, we would not be stuck in an infinite loop where we always think the
  // cache is warming up.
  if ((totalObjSize < totalObjectSizeWarmCacheThreshold) &&
      (currentNumEntries < minEntriesWarmCacheThreshold) &&
      (currentEntriesLimit_ > currentNumEntries)) {
    // Cache is warming up.
    return;
  }

  // First thing, we want to take care of is to make sure that the total
  // object size and number of objects are within the limit.
  auto averageObjSize = totalObjSize / currentNumEntries;
  XDCHECK_NE(0u, averageObjSize);
  auto newEntriesLimit =
      objCache_.config_.totalObjectSizeLimit / averageObjSize;

  if (newEntriesLimit > objCache_.config_.l1EntriesLimit) {
    XLOGF_EVERY_MS(INFO, 60'000,
                   "CacheLib size-controller: cache size is bound by "
                   "l1EntriesLimit {} desired {}",
                   objCache_.config_.l1EntriesLimit, newEntriesLimit);
  }

  XLOGF_EVERY_MS(INFO, 60'000,
                 "CacheLib size-controller: total object size = {}, current "
                 "entries = {}, average object size = "
                 "{}, new entries limit = {}, current entries limit = {}",
                 totalObjSize, currentNumEntries, averageObjSize,
                 newEntriesLimit, currentEntriesLimit_);

  // We have computed the newEntriesLimit to satisfy the numEntries and
  // totalObjSize limits. If user has specified additional control
  // to prevent OOMs, we take additional measures to make cache size
  // control more OOM safe by considering system metrics (free memory and
  // RSS) along with object size when adjusting the entries limit. It makes
  // size control more OOM safe by:
  // 1. shrinking based on system stats even if expanding based on
  // totalObjSize.
  // 2. shrinking more based on system stats if shrinking based on
  // totalObjSize.
  // 3. expanding less based on system stats if expanding based on
  // totalObjSize.
  if (mode_ == FreeMemory) {
    auto freeMemBytes = objCache_.config_.getFreeMemBytes();
    if (freeMemBytes < objCache_.config_.lowerLimitBytes) {
      // We will shrink the cache even if we had decided to expand the cache
      // based on object size. If we had decided to shrink based on object
      // size, then we shrink to the lesser value.
      newEntriesLimit =
          std::min(newEntriesLimit,
                   (currentNumEntries -
                    ((objCache_.config_.lowerLimitBytes - freeMemBytes) /
                     averageObjSize)));
    } else if (freeMemBytes > objCache_.config_.upperLimitBytes) {
      // We will expand the cache only if we had also decided to expand based
      // on object size. We will shrink if we had decided to shrink based
      // on object size.
      newEntriesLimit =
          std::min(newEntriesLimit,
                   (currentNumEntries +
                    ((objCache_.config_.upperLimitBytes - freeMemBytes) /
                     averageObjSize)));
    }
  } else if (mode_ == ResidentMemory) {
    auto rssBytes = objCache_.config_.getRSSMemBytes();
    if (rssBytes > objCache_.config_.upperLimitBytes) {
      // Same behavior as when shrinking with FreeMemory.
      newEntriesLimit = std::min(
          newEntriesLimit,
          (currentNumEntries -
           ((rssBytes - objCache_.config_.upperLimitBytes) / averageObjSize)));
    } else if (rssBytes < objCache_.config_.lowerLimitBytes) {
      // Same behavior as when expanding with FreeMemory.
      newEntriesLimit = std::min(
          newEntriesLimit,
          (currentNumEntries +
           ((objCache_.config_.lowerLimitBytes - rssBytes) / averageObjSize)));
    }
  }

  // entriesLimit should never exceed the configured entries limit
  newEntriesLimit = std::min(newEntriesLimit, objCache_.config_.l1EntriesLimit);
  if (newEntriesLimit < currentEntriesLimit_ &&
      currentNumEntries >= newEntriesLimit) {
    // shrink the cache
    shrinkCacheByEntriesNum(currentEntriesLimit_ - newEntriesLimit);
  } else if (newEntriesLimit > currentEntriesLimit_ &&
             currentNumEntries == currentEntriesLimit_) {
    // expand cache when getting a higher new limit and current entries num
    // reaches the old limit
    expandCacheByEntriesNum(newEntriesLimit - currentEntriesLimit_);
  }
}

template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::shrinkCacheByEntriesNum(
    size_t entries) {
  util::Throttler t(throttlerConfig_);
  auto before = objCache_.getNumPlaceholders();
  for (size_t i = 0; i < entries; i++) {
    if (!objCache_.allocatePlaceholder()) {
      XLOGF(ERR, "Couldn't allocate placeholder {}",
            objCache_.getNumPlaceholders());
    } else {
      currentEntriesLimit_--;
    }
    // throttle to slow down the allocation speed
    t.throttle();
  }

  XLOGF_EVERY_MS(
      INFO, 60'000,
      "CacheLib size-controller: request to shrink cache by {} entries. "
      "Placeholders num before: {}, after: {}. currentEntriesLimit: {}",
      entries, before, objCache_.getNumPlaceholders(), currentEntriesLimit_);
}

template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::expandCacheByEntriesNum(
    size_t entries) {
  util::Throttler t(throttlerConfig_);
  auto before = objCache_.getNumPlaceholders();
  entries = std::min<size_t>(entries, before);
  for (size_t i = 0; i < entries; i++) {
    objCache_.placeholders_.pop_back();
    currentEntriesLimit_++;
    // throttle to slow down the release speed
    t.throttle();
  }

  XLOGF_EVERY_MS(
      INFO, 60'000,
      "CacheLib size-controller: request to expand cache by {} entries. "
      "Placeholders num before: {}, after: {}. currentEntriesLimit: {}",
      entries, before, objCache_.getNumPlaceholders(), currentEntriesLimit_);
}

template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::getCounters(
    const util::CounterVisitor& visitor) const {
  visitor("objcache.num_placeholders", objCache_.getNumPlaceholders());
  if (folly::usingJEMalloc()) {
    auto [jemallocAllocatedBytes, jemallocActiveBytes] =
        trackJemallocMemStats();
    visitor("objcache.jemalloc_active_bytes", jemallocActiveBytes);
    visitor("objcache.jemalloc_allocated_bytes", jemallocAllocatedBytes);
  }
}

template <typename AllocatorT>
ObjectCacheSizeController<AllocatorT>::ObjectCacheSizeController(
    ObjectCache& objCache, const util::Throttler::Config& throttlerConfig)
    : throttlerConfig_(throttlerConfig),
      objCache_(objCache),
      mode_(objCache_.config_.memoryMode),
      currentEntriesLimit_(objCache_.config_.l1EntriesLimit) {}
} // namespace facebook::cachelib::objcache2
