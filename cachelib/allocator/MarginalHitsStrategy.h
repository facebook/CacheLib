#pragma once

#include "cachelib/allocator/MarginalHitsState.h"
#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

// This strategy computes number of hits in the tail slab of LRU to estimate
// the potential (given one more slab, how many more hits can this LRU serve).
// And use a smoothed ranking of those potentials to decide victim and receiver.
class MarginalHitsStrategy : public RebalanceStrategy {
 public:
  // Config class for marginal hits strategy
  struct Config : public BaseConfig {
    // parameter for moving average, to smooth the ranking
    double movingAverageParam{0.3};

    // minimum number of slabs to retain in every allocation class.
    unsigned int minSlabs{1};

    // maximum free memory (equivalent to this many slabs) in every allocation
    // class
    unsigned int maxFreeMemSlabs{1};

    Config() noexcept {}
    explicit Config(double param) noexcept : Config(param, 1, 1) {}
    Config(double param, unsigned int minSlab, unsigned int maxFree) noexcept
        : movingAverageParam(param),
          minSlabs(minSlab),
          maxFreeMemSlabs(maxFree) {}
  };

  // Update the config. This will not affect the current rebalancing, but
  // will take effect in the next round
  void updateConfig(const BaseConfig& baseConfig) override final {
    std::lock_guard<std::mutex> l(configLock_);
    config_ = static_cast<const Config&>(baseConfig);
  }

  explicit MarginalHitsStrategy(Config config = {});

 protected:
  // This returns a copy of the current config.
  // This ensures that we're always looking at the same config even though
  // someone else may have updated the config during rebalancing
  Config getConfigCopy() const {
    std::lock_guard<std::mutex> l(configLock_);
    return config_;
  }

  // pick victim and receiver classes from a pool
  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& cache,
                                             PoolId pid) override final;

  // pick victim class from a pool to shrink
  ClassId pickVictimImpl(const CacheBase& cache, PoolId pid) override final;

 private:
  // compute delta of tail hits for every class in this pool
  std::unordered_map<ClassId, double> computeClassMarginalHits(
      PoolId pid, const PoolStats& poolStats);

  // pick victim and receiver according to smoothed rankings
  RebalanceContext pickVictimAndReceiverFromRankings(
      PoolId pid,
      const std::unordered_map<ClassId, bool>& validVictim,
      const std::unordered_map<ClassId, bool>& validReceiver);

  // marginal hits states for classes in each pools
  std::unordered_map<PoolId, MarginalHitsState<ClassId>> classStates_;

  // Config for this strategy, this can be updated anytime.
  // Do not access this directly, always use `getConfig()` to
  // obtain a copy first
  Config config_;
  mutable std::mutex configLock_;
};
} // namespace cachelib
} // namespace facebook
