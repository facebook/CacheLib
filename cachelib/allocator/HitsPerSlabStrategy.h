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

// Slab rebalancing strategy that moves slabs from allocation classes with
// low hits per slab to classes with high hits per slab, optimizing overall
// cache hit rates. A slab is moved only when the receiver's hits-per-slab
// exceeds the victim's by both an absolute threshold (minDiff) and a relative
// ratio (diffRatio). See Config for tunable parameters and filtering options.
class HitsPerSlabStrategy : public RebalanceStrategy {
 public:
  // Configuration parameters for the HitsPerSlabStrategy.
  struct Config : public BaseConfig {
    // Minimum absolute difference in hits/slab between receiver and victim for
    // rebalancing to occur.
    unsigned int minDiff{100};

    // Minimum relative improvement ratio. Both minDiff and diffRatio must be
    // met for rebalancing to occur.
    double diffRatio{0.1};

    // A class must have more than this many slabs to be considered a victim.
    unsigned int minSlabs{1};

    // Classes with free memory above this threshold (in slabs) are prioritized
    // as victims when enableVictimByFreeMem is true.
    unsigned int numSlabsFreeMem{3};

    // Minimum eviction age (seconds) for victim eligibility. 0 to disable.
    unsigned int minLruTailAge{0};

    // For victims: prioritizes classes with eviction age above this threshold.
    // For receivers: excludes classes with eviction age above this threshold.
    // Falls back to all candidates if none qualify. 0 to disable.
    unsigned int maxLruTailAge{0};

    // Prioritize classes with free memory above numSlabsFreeMem as victims.
    bool enableVictimByFreeMem{true};

    // Update hits on every rebalancing attempt, even when no rebalancing
    // occurs. This ensures consistent time windows for delta hit calculations.
    bool updateHitsOnEveryAttempt{false};

    // Optional weight function to bias hits-per-slab values. Higher weights
    // make a class more likely to receive slabs and less likely to donate.
    using WeightFn = std::function<double(
        const PoolId, const ClassId, const PoolStats& pStats)>;
    WeightFn getWeight = {};

    // Optional per-class target eviction ages. Victims must meet their target;
    // receivers below target are prioritized.
    std::shared_ptr<folly::F14FastMap<uint32_t, uint32_t>>
        classIdTargetEvictionAge;

    // Returns the free memory threshold in bytes.
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

  // Updates configuration. Takes effect on next rebalancing attempt.
  void updateConfig(const BaseConfig& baseConfig) override final {
    std::lock_guard<std::mutex> l(configLock_);
    config_ = static_cast<const Config&>(baseConfig);
  }

  explicit HitsPerSlabStrategy(Config config = {});

  // Converts classIdTargetEvictionAge map to string for debugging.
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

  // Exports configuration as a string map for monitoring.
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

  // Selects victim and receiver, or returns kNoOpContext if no rebalancing.
  RebalanceContext pickVictimAndReceiverImpl(
      const CacheBase& cache,
      PoolId pid,
      const PoolStats& poolStats) override final;

  // Picks only a victim for pool resizing (releasing slabs from pool).
  ClassId pickVictimImpl(const CacheBase& cache,
                         PoolId pid,
                         const PoolStats& poolStats) override final;

 private:
  static AllocInfo makeAllocInfo(PoolId pid,
                                 ClassId cid,
                                 const PoolStats& stats) {
    return AllocInfo{pid, cid, stats.allocSizeForClass(cid)};
  }

  // Selects the class with lowest weighted hits/slab as victim.
  ClassId pickVictim(const Config& config,
                     const CacheBase& cache,
                     PoolId pid,
                     const PoolStats& stats);

  // Selects the class with highest weighted hits/slab as receiver.
  ClassId pickReceiver(const Config& config,
                       PoolId pid,
                       const PoolStats& stats,
                       ClassId victim) const;

  // Config for this strategy, this can be updated anytime.
  // Do not access this directly, always use `getConfigCopy()` to
  // obtain a copy first
  Config config_;
  mutable std::mutex configLock_;
};
} // namespace cachelib
} // namespace facebook
