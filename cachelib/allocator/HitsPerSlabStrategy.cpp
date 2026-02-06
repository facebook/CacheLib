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

#include "cachelib/allocator/HitsPerSlabStrategy.h"

#include <folly/logging/xlog.h>

#include <algorithm>
#include <functional>

namespace facebook::cachelib {

HitsPerSlabStrategy::HitsPerSlabStrategy(Config config)
    : RebalanceStrategy(HitsPerSlab), config_(std::move(config)) {}

// Filters candidates by config criteria and returns the class with lowest
// weighted hits/slab. Returns kInvalidClassId if no valid victim found.
// - Filter out classes that have fewer than minSlabs slabs.
// - Filter out classes that recently gained a slab.
// - Filter out classes with tail age < minLruTailAge.
// - Filter out classes with tail age < targetEvictionAge.
// - Prioritize classes with excessive free memory.
// - Prioritize classes with tail age > maxLruTailAge.
ClassId HitsPerSlabStrategy::pickVictim(const Config& config,
                                        const CacheBase& cache,
                                        PoolId pid,
                                        const PoolStats& stats) {
  auto victims = stats.getClassIds();

  // ignore allocation classes that have fewer than the threshold of slabs.
  victims =
      filterByNumEvictableSlabs(stats, std::move(victims), config.minSlabs);

  // ignore allocation classes that recently gained a slab. These will be
  // growing in their eviction age and we want to let the evictions stabilize
  // before we consider them again.
  victims = filterVictimsByHoldOff(pid, stats, std::move(victims));

  // we are only concerned about the eviction age and not the projected age.
  const auto poolEvictionAgeStats =
      cache.getPoolEvictionAgeStats(pid, /* projectionLength */ 0);
  // filter out alloc classes with less than the minimum tail age
  if (config.minLruTailAge != 0) {
    victims =
        filterByMinTailAge(stats, std::move(victims), config.minLruTailAge);
  }

  if (victims.empty()) {
    return Slab::kInvalidClassId;
  }

  // do not pick a victim if it is below the target eviction age
  if (config.classIdTargetEvictionAge != nullptr &&
      !config.classIdTargetEvictionAge->empty()) {
    victims = filterVictimsByTargetEvictionAge(
        stats, std::move(victims), *config.classIdTargetEvictionAge.get());
  }

  if (victims.empty()) {
    return Slab::kInvalidClassId;
  }

  // prioritize victims with excessive free memory
  const auto& poolState = getPoolState(pid);
  if (config.enableVictimByFreeMem) {
    auto victimClassId = pickVictimByFreeMem(
        victims, stats, config.getFreeMemThreshold(), poolState);

    if (victimClassId != Slab::kInvalidClassId) {
      return victimClassId;
    }
  }

  // prioritize victims with tail age > maxLruTailAge
  if (config.maxLruTailAge != 0) {
    auto maxAgeVictims = filter(
        victims,
        [&](ClassId cid) {
          return stats.evictionAgeForClass(cid) < config.maxLruTailAge;
        },
        folly::sformat(" candidates with less than {} seconds for tail age",
                       config.maxLruTailAge));
    if (!maxAgeVictims.empty()) {
      victims = std::move(maxAgeVictims);
    }
  }

  return *std::min_element(
      victims.begin(), victims.end(), [&](ClassId a, ClassId b) {
        double weight_a =
            config.getWeight ? config.getWeight(pid, a, stats) : 1;
        double weight_b =
            config.getWeight ? config.getWeight(pid, b, stats) : 1;
        return poolState.at(a).projectedDeltaHitsPerSlab(stats) * weight_a <
               poolState.at(b).projectedDeltaHitsPerSlab(stats) * weight_b;
      });
}

// Filters candidates by config criteria and returns the class with highest
// weighted hits/slab. Returns kInvalidClassId if no valid receiver found.
// - Filter out classes that are not evicting.
// - Filter out classes with 0 slabs.
// - Filter out classes with tail age > maxLruTailAge.
// - Filter out classes with tail age > targetEvictionAge.
// - Prioritize classes with tail age < minLruTailAge.
ClassId HitsPerSlabStrategy::pickReceiver(const Config& config,
                                          PoolId pid,
                                          const PoolStats& stats,
                                          ClassId victim) const {
  auto receivers = stats.getClassIds();
  receivers.erase(victim);

  const auto& poolState = getPoolState(pid);
  // filter out alloc classes that are not evicting
  receivers = filterByNoEvictions(stats, std::move(receivers), poolState);

  // filter out receivers who currently dont have any slabs. Their delta hits
  // do not make much sense.
  receivers = filterByNumEvictableSlabs(stats, std::move(receivers), 0);

  // filter out alloc classes with more than the maximum tail age
  if (config.maxLruTailAge != 0) {
    auto candidates =
        filterByMaxTailAge(stats, receivers, config.maxLruTailAge);
    // if all the candidates exceed the max eviction age then fallback to the
    // hits-based mechanism
    if (!candidates.empty()) {
      receivers = std::move(candidates);
    }
  }

  if (receivers.empty()) {
    return Slab::kInvalidClassId;
  }

  // 1. Prioritize classes below their target age
  // 2: If all classes tracked in classIdTargetEvictionAge met targets, use
  // untracked classes only
  if (config.classIdTargetEvictionAge != nullptr &&
      !config.classIdTargetEvictionAge->empty()) {
    auto candidates = filterReceiversByTargetEvictionAge(
        stats, receivers, *config.classIdTargetEvictionAge.get());

    if (!candidates.empty()) {
      // Found classes below target age: use ONLY those
      receivers = std::move(candidates);
    } else {
      // All tracked classes met target age: remove them and keep untracked ones
      receivers = filter(
          std::move(receivers),
          [&](ClassId cid) {
            return config.classIdTargetEvictionAge->find(cid) !=
                   config.classIdTargetEvictionAge->end();
          },
          "receivers in target map that met their goals");
    }
  }

  if (receivers.empty()) {
    return Slab::kInvalidClassId;
  }

  // prioritize receivers with eviction age below min LRU tail age
  if (config.minLruTailAge != 0) {
    auto minAgeReceivers = filter(
        receivers,
        [&](ClassId cid) {
          return stats.evictionAgeForClass(cid) >= config.minLruTailAge;
        },
        folly::sformat(" candidates with more than {} seconds for tail age",
                       config.minLruTailAge));
    if (!minAgeReceivers.empty()) {
      receivers = std::move(minAgeReceivers);
    }
  }

  return *std::max_element(
      receivers.begin(), receivers.end(), [&](ClassId a, ClassId b) {
        double weight_a =
            config.getWeight ? config.getWeight(pid, a, stats) : 1;
        double weight_b =
            config.getWeight ? config.getWeight(pid, b, stats) : 1;
        return poolState.at(a).deltaHitsPerSlab(stats) * weight_a <
               poolState.at(b).deltaHitsPerSlab(stats) * weight_b;
      });
}

// Picks victim/receiver and validates the improvement meets thresholds before
// allowing rebalancing.
RebalanceContext HitsPerSlabStrategy::pickVictimAndReceiverImpl(
    const CacheBase& cache, PoolId pid, const PoolStats& poolStats) {
  if (!cache.getPool(pid).allSlabsAllocated()) {
    XLOGF(DBG,
          "Pool Id: {}"
          " does not have all its slabs allocated"
          " and does not need rebalancing.",
          static_cast<int>(pid));
    return kNoOpContext;
  }

  const auto config = getConfigCopy();

  RebalanceContext ctx;
  ctx.victimClassId = pickVictim(config, cache, pid, poolStats);
  ctx.receiverClassId = pickReceiver(config, pid, poolStats, ctx.victimClassId);

  if (ctx.victimClassId == ctx.receiverClassId ||
      ctx.victimClassId == Slab::kInvalidClassId ||
      ctx.receiverClassId == Slab::kInvalidClassId) {
    return kNoOpContext;
  }

  auto& poolState = getPoolState(pid);
  double weightVictim = 1;
  double weightReceiver = 1;
  if (config.getWeight) {
    weightReceiver = config.getWeight(pid, ctx.receiverClassId, poolStats);
    weightVictim = config.getWeight(pid, ctx.victimClassId, poolStats);
  }
  const auto victimProjectedDeltaHitsPerSlab =
      poolState.at(ctx.victimClassId).projectedDeltaHitsPerSlab(poolStats) *
      weightVictim;
  const auto receiverDeltaHitsPerSlab =
      poolState.at(ctx.receiverClassId).deltaHitsPerSlab(poolStats) *
      weightReceiver;

  XLOGF(DBG,
        "Rebalancing: receiver = {}, receiver delta hits per slab = {}, victim "
        "= {}, victim projected delta hits per slab = {}",
        static_cast<int>(ctx.receiverClassId), receiverDeltaHitsPerSlab,
        static_cast<int>(ctx.victimClassId), victimProjectedDeltaHitsPerSlab);

  const auto improvement =
      receiverDeltaHitsPerSlab - victimProjectedDeltaHitsPerSlab;
  if (receiverDeltaHitsPerSlab < victimProjectedDeltaHitsPerSlab ||
      improvement < config.minDiff ||
      improvement < config.diffRatio * static_cast<long double>(
                                           victimProjectedDeltaHitsPerSlab)) {
    XLOG(DBG, " Not enough to trigger slab rebalancing");
    // Update hits on every attempt if enabled, even when no rebalancing occurs.
    // This ensures consistent time windows for delta hit calculations.
    if (config.updateHitsOnEveryAttempt) {
      for (const auto i : poolStats.getClassIds()) {
        poolState[i].updateHits(poolStats);
      }
    }
    return kNoOpContext;
  }

  // start a hold off so that the receiver does not become a victim soon
  // enough.
  poolState.at(ctx.receiverClassId).startHoldOff();

  // update all alloc classes' hits state to current hits so that next time we
  // only look at the delta hits since the last rebalance.
  for (const auto i : poolStats.getClassIds()) {
    poolState[i].updateHits(poolStats);
  }

  return ctx;
}

// Victim-only selection for pool resizing (when slabs leave the pool entirely).
ClassId HitsPerSlabStrategy::pickVictimImpl(const CacheBase& cache,
                                            PoolId pid,
                                            const PoolStats& poolStats) {
  const auto config = getConfigCopy();
  auto victimClassId = pickVictim(config, cache, pid, poolStats);

  auto& poolState = getPoolState(pid);
  // update all alloc classes' hits state to current hits so that next time we
  // only look at the delta hits since the last resize.
  for (const auto i : poolStats.getClassIds()) {
    poolState[i].updateHits(poolStats);
  }

  return victimClassId;
}
} // namespace facebook::cachelib
