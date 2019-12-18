#pragma once

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

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

 protected:
  // This returns a copy of the current config.
  // This ensures that we're always looking at the same config even though
  // someone else may have updated the config during rebalancing
  Config getConfigCopy() const {
    std::lock_guard<std::mutex> l(configLock_);
    return config_;
  }

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& cache,
                                             PoolId pid) override final;

  ClassId pickVictimImpl(const CacheBase& cache, PoolId pid) override final;

 private:
  ClassId pickVictim(const Config& config,
                     const CacheBase& cache,
                     PoolId pid,
                     const PoolStats& stats);

  ClassId pickReceiver(PoolId pid,
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
