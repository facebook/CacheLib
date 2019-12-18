#pragma once

#include <atomic>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/SlabReleaseStats.h"
#include "cachelib/common/PeriodicWorker.h"

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

class PoolResizer : public PeriodicWorker {
 public:
  PoolResizer(CacheBase& cache,
              unsigned int numSlabsPerIteration,
              std::shared_ptr<RebalanceStrategy> strategy,
              std::function<void()> postWorkHandler = {});

  ~PoolResizer() override;

  // number of slabs that have been released to resize pools.
  unsigned int getNumSlabsResized() const noexcept { return slabsReleased_; }

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
