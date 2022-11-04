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

#include <folly/stats/QuantileEstimator.h>

#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

#include "cachelib/allocator/nvmcache/BlockCacheReinsertionPolicy.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/Index.h"
#include "folly/Range.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Hits based reinsertion policy.
// By enabling this policy, we will reinsert item that had been accessed more
// than the threshold since the last time it was written into block cache. This
// can better approximate a LRU than the region-based LRU. Typically users
// apply this with a region-granularity FIFO eviction policy, or SFIFO eviction
// policy.
class HitsReinsertionPolicy : public BlockCacheReinsertionPolicy {
 public:
  // @param hitsThreshold how many hits for an item is eligible for reinsertion
  explicit HitsReinsertionPolicy(uint8_t hitsThreshold, const Index& index);

  // Applies hits based policy to determine whether or not we should keep
  // this key around longer in cache.
  bool shouldReinsert(folly::StringPiece key) override;

  // Exports hits based reinsertion policy stats via CounterVisitor.
  void getCounters(const util::CounterVisitor& visitor) const override;

 private:
  const uint8_t hitsThreshold_{};

  const Index& index_;

  mutable util::PercentileStats hitsOnReinsertionEstimator_{
      Index::kQuantileWindowSize};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
