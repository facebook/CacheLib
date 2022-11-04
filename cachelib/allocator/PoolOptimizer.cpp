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

#include "cachelib/allocator/PoolOptimizer.h"

#include <folly/logging/xlog.h>

#include "cachelib/allocator/PoolResizeStrategy.h"

namespace facebook {
namespace cachelib {

PoolOptimizer::~PoolOptimizer() { stop(std::chrono::seconds(0)); }

void PoolOptimizer::optimizeRegularPoolSizes() {
  try {
    auto strategy = cache_.getPoolOptimizeStrategy();
    if (!strategy) {
      strategy = strategy_;
    }
    const auto context = strategy->pickVictimAndReceiverRegularPools(cache_);
    if (context.victimPoolId == Slab::kInvalidPoolId ||
        context.receiverPoolId == Slab::kInvalidPoolId) {
      XLOG(DBG, "Cannot find victim and receiver for pool optimization");
    } else {
      const auto memoryToMove = Slab::kSize;
      cache_.resizePools(context.victimPoolId, context.receiverPoolId,
                         memoryToMove);
      XLOG(DBG, "Moving a slab from Pool {} to Pool {}",
           static_cast<int>(context.victimPoolId),
           static_cast<int>(context.receiverPoolId));
    }
  } catch (const std::exception& ex) {
    XLOGF(CRITICAL, "Optimization interrupted due to exception: {}", ex.what());
    XDCHECK(false);
  }
}

void PoolOptimizer::optimizeCompactCacheSizes() {
  try {
    auto strategy = cache_.getPoolOptimizeStrategy();
    if (!strategy) {
      strategy = strategy_;
    }
    const auto context = strategy->pickVictimAndReceiverCompactCaches(cache_);
    if (context.victimPoolId == Slab::kInvalidPoolId ||
        context.receiverPoolId == Slab::kInvalidPoolId) {
      XLOG(DBG, "Cannot find victim and receiver for pool optimization");
    } else {
      const auto memoryToMove =
          cache_.getCompactCache(context.victimPoolId).getConfiguredSize() *
          ccacheStepSizePercent_ / 100;
      cache_.resizePools(context.victimPoolId, context.receiverPoolId,
                         memoryToMove);
      cache_.resizeCompactCaches();
      XLOG(DBG, "Moving a slab from Pool {} to Pool {}",
           static_cast<int>(context.victimPoolId),
           static_cast<int>(context.receiverPoolId));
    }
  } catch (const std::exception& ex) {
    XLOGF(CRITICAL, "Optimization interrupted due to exception: {}", ex.what());
    XDCHECK(false);
  }
}

void PoolOptimizer::work() {
  progress_++;
  if (intervalRegularPools_ && progress_ % intervalRegularPools_ == 0) {
    optimizeRegularPoolSizes();
  }
  if (intervalCompactCaches_ && progress_ % intervalCompactCaches_ == 0) {
    optimizeCompactCacheSizes();
  }
}

} // namespace cachelib
} // namespace facebook
