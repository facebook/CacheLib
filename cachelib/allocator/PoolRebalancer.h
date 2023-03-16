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
  PoolRebalancer(CacheBase& cache,
                 std::shared_ptr<RebalanceStrategy> strategy,
                 unsigned int freeAllocThreshold);

  ~PoolRebalancer() override;

  // return the slab release event for the specified pool.
  SlabReleaseEvents getSlabReleaseEvents(PoolId pid) const {
    return stats_.getSlabReleaseEvents(pid);
  }

  RebalancerStats getStats() const noexcept;

 private:
  struct LoopStats {
    // record the count and the time taken
    void recordLoopTime(uint64_t msTaken) {
      numLoops_.fetch_add(1, std::memory_order_relaxed);
      lastLoopTimeMs_.store(msTaken, std::memory_order_relaxed);
      totalLoopTimeMs_.fetch_add(msTaken, std::memory_order_relaxed);
    }

    uint64_t getAvgLoopTimeMs() const {
      return numLoops_ ? totalLoopTimeMs_ / numLoops_ : 0;
    }
    uint64_t getLastLoopTimeMs() const { return lastLoopTimeMs_; }
    uint64_t getNumLoops() const { return numLoops_; }

   private:
    // time it took us the last time and the average
    std::atomic<uint64_t> lastLoopTimeMs_{0};
    std::atomic<uint64_t> totalLoopTimeMs_{0};
    std::atomic<uint64_t> numLoops_{0};
  };

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

  // loop timing stats
  LoopStats rebalanceStats_;
  LoopStats releaseStats_;
  LoopStats pickVictimStats_;

  // implements the actual logic of running tryRebalancing and
  // updating the stats
  void work() final;
};
} // namespace cachelib
} // namespace facebook
