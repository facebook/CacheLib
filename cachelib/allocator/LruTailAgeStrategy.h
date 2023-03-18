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

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

// If an allocation class has its tail age higher than a threshold,
// we look at how much it is higher than the average tail age. If the
// differnece is greater than the tail age difference ratio specified
// in the config, that allocation class will release a slab.
class LruTailAgeStrategy : public RebalanceStrategy {
 public:
  struct Config : public BaseConfig {
    // any LRU whose tail age surpasses the average tail age by this ratio is to
    // be rebalanced
    double tailAgeDifferenceRatio{0.25};

    // minimum tail age difference between victim and receiver for a slab
    // rebalance to happen
    unsigned int minTailAgeDifference{100};

    // minimum number of slabs to retain in every allocation class.
    unsigned int minSlabs{1};

    // use free memory if it is amounts to more than this many slabs.
    unsigned int numSlabsFreeMem{3};

    // how many slabs worth of items do we project to determine a victim.
    unsigned int slabProjectionLength{1};

    // Weight function can be changed as config parameter. By default,
    // weight function is null, and no weighted tail age is computed.
    // If the weight function is set, tailAgeDifferenceRatio and
    // minTailAgeDifference are ignored
    using WeightFn = std::function<double(
        const PoolId pid, const ClassId classId, const PoolStats& pStats)>;
    WeightFn getWeight = {};

    // This lets us specify which queue's eviction age to use.
    // Note not all eviction policies provide hot, warm, and cold queues.
    // We leave it up to the policy to determine how to define hot, warm, cold
    // eviction ages. For exmaple, in LRU, we use the same eviction-age
    // for all three stats.
    enum class QueueSelector { kHot, kWarm, kCold };
    QueueSelector queueSelector{QueueSelector::kWarm};

    // The free memory threshold to be used to pick victim class.
    size_t getFreeMemThreshold() const noexcept {
      return numSlabsFreeMem * Slab::kSize;
    }

    Config() noexcept {}
    Config(double ratio, unsigned int _minSlabs) noexcept
        : tailAgeDifferenceRatio(ratio), minSlabs{_minSlabs} {}
    Config(double ratio,
           unsigned int _minSlabs,
           const WeightFn& _getWeight) noexcept
        : tailAgeDifferenceRatio(ratio),
          minSlabs{_minSlabs},
          getWeight(_getWeight) {}
  };

  // Update the config. This will not affect the current rebalancing, but
  // will take effect in the next round
  void updateConfig(const BaseConfig& baseConfig) override {
    std::lock_guard<std::mutex> l(configLock_);
    config_ = static_cast<const Config&>(baseConfig);
  }

  explicit LruTailAgeStrategy(Config config = {});

 protected:
  // This returns a copy of the current config.
  // This ensures that we're always looking at the same config even though
  // someone else may have updated the config during rebalancing
  Config getConfigCopy() const {
    std::lock_guard<std::mutex> l(configLock_);
    return config_;
  }

  RebalanceContext pickVictimAndReceiverImpl(
      const CacheBase& cache,
      PoolId pid,
      const PoolStats& poolStats) override final;

  ClassId pickVictimImpl(const CacheBase& cache,
                         PoolId pid,
                         const PoolStats& poolStats) override final;

 private:
  static AllocInfo makeAllocInfo(PoolId pid,
                                 ClassId cid,
                                 const PoolStats& stats) {
    return AllocInfo{pid, cid, stats.allocSizeForClass(cid)};
  }

  ClassId pickVictim(const Config& config,
                     PoolId pid,
                     const PoolStats& stats,
                     const PoolEvictionAgeStats& poolEvictionAgeStats);

  ClassId pickReceiver(const Config& config,
                       PoolId pid,
                       const PoolStats& stats,
                       ClassId victim,
                       const PoolEvictionAgeStats& poolEvictionAgeStats) const;

  uint64_t getOldestElementAge(const PoolEvictionAgeStats& poolEvictionAgeStats,
                               ClassId cid) const;

  uint64_t getProjectedAge(const PoolEvictionAgeStats& poolEvictionAgeStats,
                           ClassId cid) const;

  // Config for this strategy, this can be updated anytime.
  // Do not access this directly, always use `getConfig()` to
  // obtain a copy first
  Config config_;
  mutable std::mutex configLock_;
};
} // namespace cachelib
} // namespace facebook
