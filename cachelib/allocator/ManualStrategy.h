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

// This strategy is for testing and does not automatically rebalance. User
// will explicitly instruct this policy to rebalance slabs.
class ManualStrategy : public RebalanceStrategy {
 public:
  struct Config : public BaseConfig {
    std::vector<uint32_t> slabsDistribution;
  };

  // Update the config. This will not affect the current rebalancing, but
  // will take effect in the next round
  void updateConfig(const BaseConfig& baseConfig) override {
    std::lock_guard<std::mutex> l(configLock_);
    config_ = static_cast<const Config&>(baseConfig);
  }

  explicit ManualStrategy(Config config = {})
      : RebalanceStrategy(Manual), config_(config) {}

 protected:
  // This returns a copy of the current config.
  // This ensures that we're always looking at the same config even though
  // someone else may have updated the config during rebalancing
  Config getConfigCopy() const {
    std::lock_guard<std::mutex> l(configLock_);
    return config_;
  }

  RebalanceContext pickVictimAndReceiverImpl(
      const CacheBase& /*cache*/,
      PoolId /*pid*/,
      const PoolStats& poolStats) override final {
    const auto expectedSlabsDistribution = getConfigCopy().slabsDistribution;
    const auto existingSlabsDistribution = poolStats.slabsDistribution();
    if (expectedSlabsDistribution.size() != existingSlabsDistribution.size()) {
      XLOG_EVERY_MS(ERR, 60'000)
          << "Expected slabs distribution size {} does not match existing slab "
             "distribution size {}.",
          expectedSlabsDistribution.size(), existingSlabsDistribution.size();
      return {};
    }

    RebalanceContext ctx;
    for (size_t i = 0; i < expectedSlabsDistribution.size(); ++i) {
      if (expectedSlabsDistribution[i] < existingSlabsDistribution[i]) {
        ctx.victimClassId = static_cast<ClassId>(i);
        XLOGF(INFO, "AC: picked victim: {}", i);
      } else if (expectedSlabsDistribution[i] > existingSlabsDistribution[i]) {
        ctx.receiverClassId = static_cast<ClassId>(i);
        XLOGF(INFO, "AC: picked receiver: {}", i);
      }
      if (ctx.victimClassId != Slab::kInvalidClassId &&
          ctx.receiverClassId != Slab::kInvalidClassId) {
        return ctx;
      }
    }
    return {};
  }

  ClassId pickVictimImpl(const CacheBase& /*cache*/,
                         PoolId /*pid*/,
                         const PoolStats& poolStats) override final {
    const auto expectedSlabsDistribution = getConfigCopy().slabsDistribution;
    const auto existingSlabsDistribution = poolStats.slabsDistribution();
    if (expectedSlabsDistribution.size() != existingSlabsDistribution.size()) {
      XLOG_EVERY_MS(ERR, 60'000)
          << "Expected slabs distribution size {} does not match existing slab "
             "distribution size {}.",
          expectedSlabsDistribution.size(), existingSlabsDistribution.size();
      return Slab::kInvalidClassId;
    }

    for (size_t i = 0; i < expectedSlabsDistribution.size(); ++i) {
      if (expectedSlabsDistribution[i] < existingSlabsDistribution[i]) {
        return static_cast<ClassId>(i);
      }
    }
    return Slab::kInvalidClassId;
  }

 private:
  // Config for this strategy, this can be updated anytime.
  // Do not access this directly, always use `getConfig()` to
  // obtain a copy first
  Config config_;
  mutable std::mutex configLock_;
};
} // namespace cachelib
} // namespace facebook
