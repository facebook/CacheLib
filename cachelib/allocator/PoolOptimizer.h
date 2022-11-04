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

#include <atomic>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// Periodic worker that optimizes pools by moving a slab from the victim pool to
// the receiver pool picked by the strategy.
class PoolOptimizer : public PeriodicWorker {
 public:
  // @param cache             the cache interface
  // @param strategy          pool optimizing strategy
  // @param intervalRegular   the period in number of intervals for optimizing
  //                          regular pools
  // @param intervalCompact   the period in number of intervals for optimizing
  //                          compact cache pools
  // @param stepSizePercent   the percentage number that controls the size of
  //                          each movement in a compact cache optimization.
  PoolOptimizer(CacheBase& cache,
                std::shared_ptr<PoolOptimizeStrategy> strategy,
                uint32_t intervalRegular,
                uint32_t intervalCompact,
                uint32_t stepSizePercent)
      : cache_(cache),
        strategy_(strategy),
        intervalRegularPools_(intervalRegular),
        intervalCompactCaches_(intervalCompact),
        ccacheStepSizePercent_(stepSizePercent) {}

  ~PoolOptimizer() override;

 private:
  // cache's interface for optimization
  CacheBase& cache_;

  // the strategy for optimization
  std::shared_ptr<PoolOptimizeStrategy> strategy_;

  // for now we need different frequencies for optimizing regular pools and
  // compact caches. These numbers are number of times that interval is to
  // the worker interval.
  uint32_t intervalRegularPools_{0};
  uint32_t intervalCompactCaches_{0};

  // track the progress to know whether regular pools or compact caches
  // should be optimized in the current round
  uint32_t progress_{0};

  // Step size for compact cache optimization (how many percents to move
  // each time). We have this because we want to resize compact caches less
  // frequently and in a larger stepsize than regular pools.
  // If experiments support, we should also switch regular pool stepsize to
  // this parameter.
  uint32_t ccacheStepSizePercent_{1};

  // implements the actual logic of running tryRebalancing and
  // updating the stats
  void work() final;

  void optimizeRegularPoolSizes();
  void optimizeCompactCacheSizes();
};
} // namespace cachelib
} // namespace facebook
