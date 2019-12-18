#include "cachelib/allocator/FreeMemStrategy.h"

#include <algorithm>
#include <functional>

#include <folly/logging/xlog.h>

#include "cachelib/allocator/Util.h"

namespace facebook {
namespace cachelib {

FreeMemStrategy::FreeMemStrategy(Config config)
    : RebalanceStrategy(FreeMem), config_(std::move(config)) {}

// The list of allocation classes to be rebalanced is determined by:
//
// 0. Filter out classes that have below minSlabThreshold_
//
// 1. Filter out classes that have just gained a slab recently
//
// 2. Pick the first class we find with free memory past the threshold
RebalanceContext FreeMemStrategy::pickVictimAndReceiverImpl(
    const CacheBase& cache, PoolId pid) {
  const auto& pool = cache.getPool(pid);
  if (pool.getUnAllocatedSlabMemory() >
      config_.maxUnAllocatedSlabs * Slab::kSize) {
    return kNoOpContext;
  }

  const auto poolStats = cache.getPoolStats(pid);

  // ignore allocation classes that have fewer than the threshold of slabs.
  const auto victims = filterByNumEvictableSlabs(
      poolStats, std::move(poolStats.getClassIds()), config_.minSlabs);

  if (victims.empty()) {
    XLOG(DBG, "Rebalancing: No victims available");
    return kNoOpContext;
  }

  RebalanceContext ctx;
  ctx.victimClassId = pickVictimByFreeMem(
      victims, poolStats, config_.getFreeMemThreshold(), getPoolState(pid));

  if (ctx.victimClassId == Slab::kInvalidClassId) {
    return kNoOpContext;
  }

  XLOGF(DBG, "Rebalancing: victimAC = {}", static_cast<int>(ctx.victimClassId));
  return ctx;
}
} // namespace cachelib
} // namespace facebook
