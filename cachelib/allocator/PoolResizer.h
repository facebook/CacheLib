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

#include <atomic>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/SlabReleaseStats.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// Periodic worker that resizes pools.
// For each pool that is over the limit, the worker attempts to release up to a
// certain number of slabs.
class PoolResizer : public PeriodicWorker {
 public:
  // @param cache                 the cache interace
  // @param numSlabsPerIteration  maximum number of slabs each pool may remove
  //                              in resizing.
  // @param strategy              the resizing strategy
  PoolResizer(CacheBase& cache,
              unsigned int numSlabsPerIteration,
              std::shared_ptr<RebalanceStrategy> strategy);

  ~PoolResizer() override;

  // number of slabs that have been released to resize pools.
  unsigned int getNumSlabsResized() const noexcept { return slabsReleased_; }

  // returns the slab release events for the specified pool.
  SlabReleaseEvents getSlabReleaseEvents(PoolId pid) const {
    return stats_.getSlabReleaseEvents(pid);
  }

 private:
  // helper function to pick a victim to release a slab based on the provided
  // strategy. If the strategy fails to provide one, we pick one random
  // classId.
  //
  // @param poolId  the pool id
  // @return  a valid class Id if there is one available. returns
  //          Slab::kInvalidClassId if all the allocation classes are exhausted
  ClassId pickVictim(PoolId poolId);

  // cache's interface for rebalancing
  CacheBase& cache_;

  // user-defined rebalance strategy that would be used to pick a victim. If
  // this does not work out, we pick the allocation class with maximum
  // eviction age.
  std::shared_ptr<RebalanceStrategy> strategy_;

  // number of slabs released as a part of resizing pools.
  std::atomic<unsigned int> slabsReleased_{0};

  // number of slabs to be released per iteration per pool that needs resizing
  unsigned int numSlabsPerIteration_{0};

  // slab release stats for resizer.
  ReleaseStats stats_;

  // implements the actual logic of running tryRebalancing and
  // updating the stats
  void work() final;
};
} // namespace cachelib
} // namespace facebook
