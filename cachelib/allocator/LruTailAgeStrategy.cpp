#include "cachelib/allocator/LruTailAgeStrategy.h"

#include <algorithm>
#include <functional>

#include <folly/logging/xlog.h>

#include "cachelib/allocator/Util.h"

namespace facebook {
namespace cachelib {

LruTailAgeStrategy::LruTailAgeStrategy(Config config)
    : RebalanceStrategy(LruTailAge), config_(std::move(config)) {}

// The list of allocation classes to be rebalanced is determined by:
//
// 0. Filter out classes that have below minSlabThreshold_
//
// 1. Filter out classes that have just gained a slab recently
//
// 2. Compute weighted tail age from all the remaining ACs
//
// 3. Pick an AC with the oldest tail age higher than the weighted average
ClassId LruTailAgeStrategy::pickVictim(
    const Config& config,
    PoolId pid,
    const PoolStats& poolStats,
    const PoolEvictionAgeStats& poolEvictionAgeStats) {
  auto victims = poolStats.getClassIds();

  // ignore allocation classes that have fewer than the threshold of slabs.
  victims =
      filterByNumEvictableSlabs(poolStats, std::move(victims), config.minSlabs);

  // ignore allocation classes that recently gained a slab. These will be
  // growing in their eviction age and we want to let the evicitons stabilize
  // before we consider  them again.
  victims = filterVictimsByHoldOff(pid, poolStats, std::move(victims));

  if (victims.empty()) {
    XLOG(DBG, "Rebalancing: No victims available");
    return Slab::kInvalidClassId;
  }

  auto victimClassId = pickVictimByFreeMem(
      victims, poolStats, config.getFreeMemThreshold(), getPoolState(pid));

  if (victimClassId != Slab::kInvalidClassId) {
    return victimClassId;
  }

  // the oldest projected age among the victims
  return *std::max_element(
      victims.begin(), victims.end(), [&](ClassId a, ClassId b) {
        return (poolEvictionAgeStats.getProjectedAge(a) *
                    (config.getWeight ? config.getWeight(a) : 1.0) <
                poolEvictionAgeStats.getProjectedAge(b) *
                    (config.getWeight ? config.getWeight(b) : 1.0));
      });
}

ClassId LruTailAgeStrategy::pickReceiver(
    const Config& config,
    PoolId pid,
    const PoolStats& stats,
    ClassId victim,
    const PoolEvictionAgeStats& poolEvictionAgeStats) const {
  auto receivers = stats.getClassIds();
  receivers.erase(victim);

  receivers = filterByNoEvictions(stats, receivers, getPoolState(pid));
  if (receivers.empty()) {
    return Slab::kInvalidClassId;
  }

  // the youngest age among the potenital receivers
  return *std::min_element(
      receivers.begin(), receivers.end(), [&](ClassId a, ClassId b) {
        return (poolEvictionAgeStats.getOldestElementAge(a) *
                    (config.getWeight ? config.getWeight(a) : 1.0) <
                poolEvictionAgeStats.getOldestElementAge(b) *
                    (config.getWeight ? config.getWeight(b) : 1.0));
      });
}

RebalanceContext LruTailAgeStrategy::pickVictimAndReceiverImpl(
    const CacheBase& cache, PoolId pid) {
  if (!cache.getPool(pid).allSlabsAllocated()) {
    XLOGF(DBG,
          "Pool Id: {}"
          " does not have all its slabs allocated"
          " and does not need rebalancing.",
          static_cast<int>(pid));

    return kNoOpContext;
  }

  const auto config = getConfigCopy();

  const auto poolStats = cache.getPoolStats(pid);
  const auto poolEvictionAgeStats =
      cache.getPoolEvictionAgeStats(pid, config.slabProjectionLength);

  RebalanceContext ctx;
  ctx.victimClassId = pickVictim(config, pid, poolStats, poolEvictionAgeStats);
  ctx.receiverClassId = pickReceiver(config, pid, poolStats, ctx.victimClassId,
                                     poolEvictionAgeStats);
  if (ctx.victimClassId == ctx.receiverClassId ||
      ctx.victimClassId == Slab::kInvalidClassId ||
      ctx.receiverClassId == Slab::kInvalidClassId) {
    return kNoOpContext;
  }

  if (!config.getWeight) {
    const auto victimProjectedTailAge =
        poolEvictionAgeStats.getProjectedAge(ctx.victimClassId);
    const auto receiverTailAge =
        poolEvictionAgeStats.getOldestElementAge(ctx.receiverClassId);

    XLOGF(DBG, "Rebalancing: receiver = {}, receiverTailAge = {}, victim = {}",
          static_cast<int>(ctx.receiverClassId), receiverTailAge,
          static_cast<int>(ctx.victimClassId));

    const auto improvement = victimProjectedTailAge - receiverTailAge;
    if (victimProjectedTailAge < receiverTailAge ||
        improvement < config.minTailAgeDifference ||
        improvement < config.tailAgeDifferenceRatio *
                          static_cast<long double>(victimProjectedTailAge)) {
      return kNoOpContext;
    }
  }

  // start a hold off so that the receiver does not become a victim soon
  // enough.
  getPoolState(pid).at(ctx.receiverClassId).startHoldOff();
  return ctx;
}

ClassId LruTailAgeStrategy::pickVictimImpl(const CacheBase& cache, PoolId pid) {
  const auto config = getConfigCopy();
  const auto poolEvictionAgeStats =
      cache.getPoolEvictionAgeStats(pid, config.slabProjectionLength);
  return pickVictim(config, pid, cache.getPoolStats(pid), poolEvictionAgeStats);
}
} // namespace cachelib
} // namespace facebook
