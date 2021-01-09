#pragma once

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {

struct PoolOptimizeContext {
  PoolId victimPoolId{Slab::kInvalidPoolId};
  PoolId receiverPoolId{Slab::kInvalidPoolId};

  PoolOptimizeContext() = default;
  PoolOptimizeContext(PoolId victim, PoolId receiver)
      : victimPoolId(victim), receiverPoolId(receiver) {}
};

// The idea here is the user should inspect the stats corresponding to the
// pool they want to rebalance. They will then decide if and which allocation
// classes need to release slab and how many slabs should be released back to
// the pool.
//
// The actual release operation is handled by PoolOptimizer
class PoolOptimizeStrategy {
 public:
  struct BaseConfig {};
  virtual void updateConfig(const BaseConfig&) {}

  enum Type { PickNothingOrTest, MarginalHits, NumTypes };
  explicit PoolOptimizeStrategy(Type strategyType = PickNothingOrTest)
      : type_(strategyType) {}
  virtual ~PoolOptimizeStrategy() = default;

  // Pick an victim and receiver pools from regular pools
  //
  // @param allocator   Cache allocator that implements CacheBase
  //
  // @return PoolOptimizeContext   contains victim and receiver
  PoolOptimizeContext pickVictimAndReceiverRegularPools(
      const CacheBase& cache) {
    return pickVictimAndReceiverRegularPoolsImpl(cache);
  }

  // Pick an victim and receiver pools from compact caches
  //
  // @param allocator   Cache allocator that implements CacheBase
  //
  // @return PoolOptimizeContext   contains victim and receiver
  PoolOptimizeContext pickVictimAndReceiverCompactCaches(
      const CacheBase& cache) {
    return pickVictimAndReceiverCompactCachesImpl(cache);
  }

  Type getType() const { return type_; }

 protected:
  virtual PoolOptimizeContext pickVictimAndReceiverRegularPoolsImpl(
      const CacheBase& /* cache */) {
    return {};
  }

  virtual PoolOptimizeContext pickVictimAndReceiverCompactCachesImpl(
      const CacheBase& /* cache */) {
    return {};
  }

  static const PoolOptimizeContext kNoOpContext;

 private:
  Type type_{NumTypes};
};

} // namespace cachelib
} // namespace facebook
