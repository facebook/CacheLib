#pragma once

#include <atomic>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

class PoolOptimizer : public PeriodicWorker {
 public:
  PoolOptimizer(CacheBase& cache,
                std::shared_ptr<PoolOptimizeStrategy> strategy,
                uint32_t intervalRegular,
                uint32_t intervalCompact,
                uint32_t stepSizePercent)
      : cache_(cache),
        strategy_(strategy),
        intervalRegularPools_(intervalRegular),
        intervalCompactCaches_(intervalCompact),
        ccacheStepSizePercent_(stepSizePercent) {}

  ~PoolOptimizer() override;

 private:
  // cache's interface for optimization
  CacheBase& cache_;

  // the strategy for optimization
  std::shared_ptr<PoolOptimizeStrategy> strategy_;

  // for now we need different frequencies for optimizing regular pools and
  // compact caches. These numbers are number of times that interval is to
  // the worker interval.
  uint32_t intervalRegularPools_{0};
  uint32_t intervalCompactCaches_{0};

  // track the progress to know whether regular pools or compact caches
  // should be optimized in the current round
  uint32_t progress_{0};

  // Step size for compact cache optimization (how many percents to move
  // each time). We have this because we want to resize compact caches less
  // frequently and in a larger stepsize than regular pools.
  // If experiments support, we should also switch regular pool stepsize to
  // this parameter.
  uint32_t ccacheStepSizePercent_{1};

  // implements the actual logic of running tryRebalancing and
  // updating the stats
  void work() final;

  void optimizeRegularPoolSizes();
  void optimizeCompactCacheSizes();
};
} // namespace cachelib
} // namespace facebook
