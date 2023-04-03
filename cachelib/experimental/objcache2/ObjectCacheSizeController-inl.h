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

namespace facebook {
namespace cachelib {
namespace objcache2 {
template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::work() {
  auto currentNumEntries = objCache_.getNumEntries();
  if (currentNumEntries == 0) {
    return;
  }
  auto totalObjSize = objCache_.getTotalObjectSize();
  // Do the calculation only when total object size or total object number
  // achieves the threshold. This is to avoid unreliable calculation of average
  // object size when the cache is new and only has a few objects.
  if (totalObjSize > kSizeControllerThresholdPct *
                         objCache_.config_.cacheSizeLimit / 100 ||
      currentNumEntries > kSizeControllerThresholdPct *
                              objCache_.config_.l1EntriesLimit / 100) {
    auto averageObjSize = totalObjSize / currentNumEntries;
    auto newEntriesLimit = objCache_.config_.cacheSizeLimit / averageObjSize;
    // entriesLimit should never exceed the configured entries limit
    newEntriesLimit =
        std::min(newEntriesLimit, objCache_.config_.l1EntriesLimit);
    if (newEntriesLimit < currentEntriesLimit_ &&
        currentNumEntries >= newEntriesLimit) {
      // shrink cache when getting a lower new limit and current entries num
      // reaches the new limit
      shrinkCacheByEntriesNum(currentEntriesLimit_ - newEntriesLimit);
    } else if (newEntriesLimit > currentEntriesLimit_ &&
               currentNumEntries == currentEntriesLimit_) {
      // expand cache when getting a higher new limit and current entries num
      // reaches the old limit
      expandCacheByEntriesNum(newEntriesLimit - currentEntriesLimit_);
    }

    XLOGF_EVERY_MS(INFO, 60'000,
                   "CacheLib size-controller: total object size = {}, current "
                   "entries = {}, average object size = "
                   "{}, new entries limit = {}, current entries limit = {}",
                   totalObjSize, currentNumEntries, averageObjSize,
                   newEntriesLimit, currentEntriesLimit_);
  }
}

template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::shrinkCacheByEntriesNum(
    int entries) {
  util::Throttler t(throttlerConfig_);
  auto size = objCache_.placeholders_.size();
  for (size_t i = size; i < size + entries; i++) {
    auto key = objCache_.getPlaceHolderKey(i);
    auto success = objCache_.allocatePlaceholder(key);
    if (!success) {
      XLOGF(ERR, "Couldn't allocate {}", key);
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
      entries, size, objCache_.placeholders_.size(), currentEntriesLimit_);
}

template <typename AllocatorT>
void ObjectCacheSizeController<AllocatorT>::expandCacheByEntriesNum(
    int entries) {
  util::Throttler t(throttlerConfig_);
  auto size = objCache_.placeholders_.size();
  for (int i = 0; i < entries && !objCache_.placeholders_.empty(); i++) {
    objCache_.placeholders_.pop_back();
    currentEntriesLimit_++;
    // throttle to slow down the release speed
    t.throttle();
  }

  XLOGF_EVERY_MS(
      INFO, 60'000,
      "CacheLib size-controller: request to expand cache by {} entries. "
      "Placeholders num before: {}, after: {}. currentEntriesLimit: {}",
      entries, size, objCache_.placeholders_.size(), currentEntriesLimit_);
}

template <typename AllocatorT>
ObjectCacheSizeController<AllocatorT>::ObjectCacheSizeController(
    ObjectCache& objCache, const util::Throttler::Config& throttlerConfig)
    : throttlerConfig_(throttlerConfig),
      objCache_(objCache),
      currentEntriesLimit_(objCache_.config_.l1EntriesLimit) {}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
