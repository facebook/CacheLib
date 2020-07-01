#pragma once

#include <folly/Random.h>

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

// an implementation of resizing strategy that is less aggressive unless it
// has to drain the pool completely.
//
// TODO (sathya) add some tests for this once we move to Gmock. Adding any
// test case right now is very difficult.
class PoolResizeStrategy : public RebalanceStrategy {
 public:
  PoolResizeStrategy() : RebalanceStrategy(PoolResize) {}
  // @param minSlabs  minimum number of slabs per alloc class before which
  //                  we go aggresive
  explicit PoolResizeStrategy(unsigned int minSlabs)
      : RebalanceStrategy(PoolResize), minSlabsPerAllocClass_(minSlabs) {}

  // implementation that picks a victim
  ClassId pickVictimImpl(const CacheBase& cache, PoolId poolId) final {
    // pick the class with maximum eviction age. also, ensure that the class
    // does not drop below threshold of slabs.
    const auto stats = cache.getPoolStats(poolId);

    auto victims = filterByNumEvictableSlabs(
        stats, stats.getClassIds(), minSlabsPerAllocClass_);

    if (victims.empty()) {
      return Slab::kInvalidClassId;
    }

    // find the class id among victims that has the maximum eviction age.
    // TODO (get a better metric for this by figuring out the impact of
    // removing one slab)
    const auto& cacheStats = stats.cacheStats;
    const auto it =
        std::max_element(victims.begin(),
                         victims.end(),
                         [&cacheStats](const ClassId a, const ClassId b) {
                           return cacheStats.at(a).getEvictionAge() <
                                  cacheStats.at(b).getEvictionAge();
                         });
    return *it;
  }

 private:
  // number of slabs below which we aggressively give away the slabs. If we
  // have more than these number of slabs, we pick the victim by eviction
  // age.
  const unsigned int minSlabsPerAllocClass_{1};
};
} // namespace cachelib
} // namespace facebook
