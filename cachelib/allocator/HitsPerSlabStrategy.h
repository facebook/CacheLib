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

#include <folly/container/F14Map.h>

#include <memory>
#include <sstream>
#include <string>

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

// Strategy that rebalances the slabs by moving slabs from the allocation class
// with the lowest hits per slab to the highest hits per slab within the pool.
class HitsPerSlabStrategy : public RebalanceStrategy {
 public:
  struct Config : public BaseConfig {
    // Absolute difference to be rebalanced
    unsigned int minDiff{100};

    // Relative difference to be rebalanced
    double diffRatio{0.1};

    // minimum number of slabs to retain in every allocation class.
    unsigned int minSlabs{1};

    // use free memory if it amounts to more than this many slabs.
    unsigned int numSlabsFreeMem{3};

    // minimum tail age for an allocation class to be eligible to be a victim
    unsigned int minLruTailAge{0};

    // max tail age for an allocation class to be excluded from being a receiver
    unsigned int maxLruTailAge{0};

    // Enable victim selection based on free memory
    bool enableVictimByFreeMem{true};

    // optionial weight function based on allocation class size
    using WeightFn = std::function<double(
        const PoolId, const ClassId, const PoolStats& pStats)>;
    WeightFn getWeight = {};

    // A map of classId to target eviction age
    std::shared_ptr<folly::F14FastMap<uint32_t, uint32_t>>
        classIdTargetEvictionAge;

    // free memory threshold used to pick victim.
    size_t getFreeMemThreshold() const noexcept {
      return numSlabsFreeMem * Slab::kSize;
    }

    Config() noexcept {}
    Config(double ratio, unsigned int _minSlabs) noexcept
        : Config(ratio, _minSlabs, 0) {}
    Config(double ratio,
           unsigned int _minSlabs,
           unsigned int _minLruTailAge) noexcept
        : diffRatio(ratio),
          minSlabs(_minSlabs),
          minLruTailAge(_minLruTailAge) {}
  };

  // Update the config. This will not affect the current rebalancing, but
  // will take effect in the next round
  void updateConfig(const BaseConfig& baseConfig) override final {
    std::lock_guard<std::mutex> l(configLock_);
    config_ = static_cast<const Config&>(baseConfig);
  }

  explicit HitsPerSlabStrategy(Config config = {});

  std::string evictionAgeMapToStr(
      const folly::F14FastMap<uint32_t, uint32_t>* map) const {
    if (!map) {
      return "";
    }
    std::ostringstream oss;
    for (const auto& [k, v] : *map) {
      oss << (oss.tellp() ? ", " : "") << k << ":" << v;
    }
    return oss.str();
  }

  std::map<std::string, std::string> exportConfig() const override {
    return {{"rebalancer_type", folly::sformat("{}", getTypeString())},
            {"min_slabs", folly::sformat("{}", config_.minSlabs)},
            {"num_slabs_free_mem", folly::sformat("{}", config_.minSlabs)},
            {"min_lru_tail_age", folly::sformat("{}", config_.minLruTailAge)},
            {"max_lru_tail_age", folly::sformat("{}", config_.maxLruTailAge)},
            {"diff_ratio", folly::sformat("{}", config_.diffRatio)},
            {"min_diff", folly::sformat("{}", config_.minDiff)},
            {"eviction_age_map",
             evictionAgeMapToStr(config_.classIdTargetEvictionAge.get())}};
  }

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
                     const CacheBase& cache,
                     PoolId pid,
                     const PoolStats& stats);

  ClassId pickReceiver(const Config& config,
                       PoolId pid,
                       const PoolStats& stats,
                       ClassId victim) const;

  // Config for this strategy, this can be updated anytime.
  // Do not access this directly, always use `getConfig()` to
  // obtain a copy first
  Config config_;
  mutable std::mutex configLock_;
};
} // namespace cachelib
} // namespace facebook
