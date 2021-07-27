#pragma once

#include <gtest/gtest_prod.h>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/SlabReleaseStats.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// Periodic worker that rebalances slabs within each pool so that new
// allocations are less likely to fail. For each pool:
// 1. If there is any allocation class with a high free-alloc-slab (see
// constructors documentation for definition), release a slab from that class.
// Else
// 2. Find a victim and receiver pair according to the strategy and move a slab
// from the victim class to the receiver class.
class PoolRebalancer : public PeriodicWorker {
 public:
  // @param cache               the cache interface
  // @param strategy            rebalancing strategy
  // @param freeAllocThreshold  threshold for free-alloc-slab. free-alloc-slab
  // is calculated by the number of total free allocations divided by the number
  // of allocations in a slab. Only allocation classes with a higher
  // free-alloc-slab could get picked as a victim.
  // @param postWorkHandler     callback to be executed after the worker stops
  PoolRebalancer(CacheBase& cache,
                 std::shared_ptr<RebalanceStrategy> strategy,
                 unsigned int freeAllocThreshold,
                 std::function<void()> postWorkHandler = {});

  ~PoolRebalancer() override;

  // return the slab release event for the specified pool.
  SlabReleaseEvents getSlabReleaseEvents(PoolId pid) const {
    return stats_.getSlabReleaseEvents(pid);
  }

 private:
  // This will attempt to rebalance by
  //  1. reading the stats from the cache allocator
  //  2. analyzing the stats by using the rebalance strategy
  //  3. rebalance
  //
  // @param pid       pool to rebalance
  // @param strategy  rebalancing strategy to use for this pool
  //
  // @return true   A rebalance operation was applied successfully to the
  //                memory pool
  //         false  There was no need for rebalancing
  bool tryRebalancing(PoolId pid, RebalanceStrategy& strategy);

  // Pick only the victim which has number of free allocs per number of
  // allocs per slab above the 'freeAllocThreshold_' ratio. If there are
  // multiple such slab classes, the slab class with highest ratio is picked
  RebalanceContext pickVictimByFreeAlloc(PoolId pid) const;

  void releaseSlab(PoolId pid, ClassId victim, ClassId receiver);
  // cache allocator's interface for rebalancing
  CacheBase& cache_;

  std::shared_ptr<RebalanceStrategy> defaultStrategy_{nullptr};

  // Free alloc threshold to trigger freeing slabs in an allocation
  // class. This threshold for ratio of number
  // of free allocs to number of allocs per slab.
  unsigned int freeAllocThreshold_;

  // slab release stats for this rebalancer.
  ReleaseStats stats_;

  // user defined handler that will be executed when the resizer stops
  std::function<void()> postWorkHandler_;

  // implements the actual logic of running tryRebalancing and
  // updating the stats
  void work() final;

  // executes a user-defined postWork handler
  void postWork() final {
    if (postWorkHandler_) {
      postWorkHandler_();
    }
  }
};
} // namespace cachelib
} // namespace facebook
