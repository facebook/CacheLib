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

#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {
template <typename AllocatorT>
class ObjectCache;

// Dynamically adjust the entriesLimit to limit the cache size for object-cache.
template <typename AllocatorT>
class ObjectCacheSizeController : public PeriodicWorker {
 public:
  using ObjectCache = ObjectCache<AllocatorT>;
  explicit ObjectCacheSizeController(
      ObjectCache& objCache, const util::Throttler::Config& throttlerConfig);
  size_t getCurrentEntriesLimit() const {
    return currentEntriesLimit_.load(std::memory_order_relaxed);
  }

 private:
  void work() override final;

  void shrinkCacheByEntriesNum(int entries);
  void expandCacheByEntriesNum(int entries);

  // threshold in percentage to determine whether the size-controller should do
  // the calculation
  const size_t kSizeControllerThresholdPct = 50;

  const util::Throttler::Config throttlerConfig_;

  // reference to the object cache
  ObjectCache& objCache_;

  // will be adjusted to control the cache size limit
  std::atomic<size_t> currentEntriesLimit_;
};

} // namespace objcache2
} // namespace cachelib
} // namespace facebook

#include "cachelib/experimental/objcache2/ObjectCacheSizeController-inl.h"
