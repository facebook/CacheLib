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

#include "cachelib/allocator/MarginalHitsState.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"

namespace facebook {
namespace cachelib {

// Similar to MarginalHitsStrategy but works at a pool level.
// The score of each pool is defined as the maximum tail hits amongst the pool's
// allocation classes. Then rank the pools according to the score to decide
// victim and receiver.
class MarginalHitsOptimizeStrategy : public PoolOptimizeStrategy {
 public:
  struct Config : public BaseConfig {
    // Parameter for moving average, to smooth the ranking.
    // This parameter should be between 0 and 1, where 0 means no smothness
    // applied, and 1 means not incorporating new value. Larger number makes
    // the ranking of a pool through time more steady and smooth.
    // This can be estimated by the condition of reversing rankings:
    //   delta(current round ranking) / delta(smoothed ranking)
    //     > param / (1 - param)
    double movingAverageParam{0.3};

    // Threshold for pool size (# of slabs).
    // Pools with size no more than this many slabs cannot be a victim
    uint32_t poolMinSizeSlabs{1};

    // Threshold for pool free memory (# of slabs).
    // Pools with free memory (free allocs + free slabs) no less than this size
    // cannot be a receiver.
    uint32_t poolMaxFreeSlabs{2};

    Config() noexcept {}
    explicit Config(double param,
                    uint32_t minSizeSlabs,
                    uint32_t maxFreeSlabs) noexcept
        : movingAverageParam(param),
          poolMinSizeSlabs(minSizeSlabs),
          poolMaxFreeSlabs(maxFreeSlabs) {}
  };

  explicit MarginalHitsOptimizeStrategy(Config config = {})
      : PoolOptimizeStrategy(MarginalHits), config_(std::move(config)) {}

  // Update the config. This will not affect the current rebalancing, but
  // will take effect in the next round
  void updateConfig(const BaseConfig& baseConfig) override final {
    std::lock_guard<std::mutex> l(configLock_);
    config_ = static_cast<const Config&>(baseConfig);
  }

 protected:
  // This returns a copy of the current config.
  // This ensures that we're always looking at the same config even though
  // someone else may have updated the config during rebalancing
  Config getConfigCopy() const {
    std::lock_guard<std::mutex> l(configLock_);
    return config_;
  }

  // pick victim and receiver regular pools
  PoolOptimizeContext pickVictimAndReceiverRegularPoolsImpl(
      const CacheBase& cache) override final;

  // pick victim and receiver compact caches
  PoolOptimizeContext pickVictimAndReceiverCompactCachesImpl(
      const CacheBase& cache) override final;

 private:
  // pick victim and receivers from rankings in states
  // @param valildVictim   whether a pool can be a victim in this round
  // @param valildReceiver whether a pool can be a receiver in this round
  PoolOptimizeContext pickVictimAndReceiverFromRankings(
      const MarginalHitsState<PoolId>& state,
      const std::unordered_map<PoolId, bool>& validVictim,
      const std::unordered_map<PoolId, bool>& validReceiver);

  // regular pool optimize states
  MarginalHitsState<PoolId> regularPoolState_;

  // compact cache optimize states
  MarginalHitsState<PoolId> compactCacheState_;

  // stats: accumulative tail hits for each allocation classes in regular pools
  std::unordered_map<PoolId, std::unordered_map<ClassId, uint64_t>>
      accuTailHitsRegularPool;

  // stats: accumulative tail hits for each compact cache
  std::unordered_map<PoolId, uint64_t> accuTailHitsCompactCache;

  // get delta tail hits for all classes from pool stats, and update
  // accumulated number of tail hits for them
  std::unordered_map<ClassId, uint64_t> getTailHitsAndUpdate(
      const PoolStats& poolStats, PoolId pid);

  // get delta tail hits for a compact cache, and update accumulated number
  uint64_t getTailHitsAndUpdate(const CCacheStats& stats, PoolId pid);

  // Config for this strategy, this can be updated anytime.
  // Do not access this directly, always use `getConfig()` to
  // obtain a copy first
  Config config_;
  mutable std::mutex configLock_;
};
} // namespace cachelib
} // namespace facebook
