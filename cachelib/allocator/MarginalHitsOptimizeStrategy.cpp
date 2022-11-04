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

#include "cachelib/allocator/MarginalHitsOptimizeStrategy.h"

#include <folly/logging/xlog.h>

#include <algorithm>
#include <functional>

#include "cachelib/allocator/Util.h"

namespace facebook {
namespace cachelib {

PoolOptimizeContext
MarginalHitsOptimizeStrategy::pickVictimAndReceiverFromRankings(
    const MarginalHitsState<PoolId>& state,
    const std::unordered_map<PoolId, bool>& validVictim,
    const std::unordered_map<PoolId, bool>& validReceiver) {
  auto victimAndReceiver = state.pickVictimAndReceiverFromRankings(
      validVictim, validReceiver, Slab::kInvalidPoolId);
  PoolOptimizeContext ctx{victimAndReceiver.first, victimAndReceiver.second};
  if (ctx.victimPoolId == Slab::kInvalidPoolId ||
      ctx.receiverPoolId == Slab::kInvalidPoolId ||
      ctx.victimPoolId == ctx.receiverPoolId) {
    return kNoOpContext;
  }

  XLOGF(DBG,
        "Optimizing: receiver = {}, smoothed rank = {}, victim = {}, smoothed "
        "rank = {}",
        static_cast<int>(ctx.receiverPoolId),
        state.smoothedRanks.at(ctx.receiverPoolId),
        static_cast<int>(ctx.victimPoolId),
        state.smoothedRanks.at(ctx.victimPoolId));
  return ctx;
}

std::unordered_map<ClassId, uint64_t>
MarginalHitsOptimizeStrategy::getTailHitsAndUpdate(const PoolStats& poolStats,
                                                   PoolId pid) {
  std::unordered_map<ClassId, uint64_t> tailHits;
  const auto& cacheStats = poolStats.cacheStats;
  for (auto it : accuTailHitsRegularPool[pid]) {
    XDCHECK(cacheStats.find(it.first) != cacheStats.end());
    tailHits[it.first] =
        cacheStats.at(it.first).containerStat.numTailAccesses - it.second;
    it.second = cacheStats.at(it.first).containerStat.numTailAccesses;
  }
  return tailHits;
}

uint64_t MarginalHitsOptimizeStrategy::getTailHitsAndUpdate(
    const CCacheStats& stats, PoolId pid) {
  uint64_t tailHits = stats.tailHits - accuTailHitsCompactCache[pid];
  accuTailHitsCompactCache[pid] = stats.tailHits;
  return tailHits;
}

PoolOptimizeContext
MarginalHitsOptimizeStrategy::pickVictimAndReceiverRegularPoolsImpl(
    const CacheBase& cache) {
  const auto config = getConfigCopy();
  std::unordered_map<PoolId, double> scores;
  std::unordered_map<PoolId, bool> validVictim;
  std::unordered_map<PoolId, bool> validReceiver;

  // If no data is stored yet, initialize stats and return nothing
  if (regularPoolState_.entities.empty()) {
    auto regularPools = cache.getRegularPoolIds();
    for (auto pid : regularPools) {
      if (cache.autoResizeEnabledForPool(pid)) {
        regularPoolState_.entities.push_back(pid);
        regularPoolState_.smoothedRanks[pid] = 0;
        auto poolStats = cache.getPoolStats(pid);
        for (auto cid : poolStats.getClassIds()) {
          accuTailHitsRegularPool[pid][cid] =
              poolStats.cacheStats.at(cid).containerStat.numTailAccesses;
        }
      }
    }
    return kNoOpContext;
  }
  for (auto pid : regularPoolState_.entities) {
    const auto poolStats = cache.getPoolStats(pid);
    auto classScores = getTailHitsAndUpdate(poolStats, pid);
    uint64_t score = 0;
    for (auto it : classScores) {
      score = std::max(score, it.second);
    }
    scores[pid] = score;
    validVictim[pid] =
        poolStats.numEvictions() > 0 &&
        poolStats.poolSize > config.poolMinSizeSlabs * Slab::kSize;
    validReceiver[pid] =
        poolStats.mpStats.freeMemory() < config.poolMaxFreeSlabs * Slab::kSize;
  }

  regularPoolState_.updateRankings(scores, config.movingAverageParam);
  return pickVictimAndReceiverFromRankings(
      regularPoolState_, validVictim, validReceiver);
}

PoolOptimizeContext
MarginalHitsOptimizeStrategy::pickVictimAndReceiverCompactCachesImpl(
    const CacheBase& cache) {
  const auto config = getConfigCopy();
  std::unordered_map<PoolId, double> scores;
  std::unordered_map<PoolId, bool> validVictim;
  std::unordered_map<PoolId, bool> validReceiver;
  // If no data is stored yet, initialize stats and return nothing
  if (compactCacheState_.entities.empty()) {
    for (const auto pid : cache.getCCachePoolIds()) {
      if (cache.autoResizeEnabledForPool(pid)) {
        compactCacheState_.entities.push_back(pid);
        compactCacheState_.smoothedRanks[pid] = 0;
        accuTailHitsCompactCache[pid] =
            cache.getCompactCache(pid).getStats().tailHits;
      }
    }
    return kNoOpContext;
  }

  for (auto pid : compactCacheState_.entities) {
    const auto& ccache = cache.getCompactCache(pid);
    scores[pid] = getTailHitsAndUpdate(ccache.getStats(), pid);
    validVictim[pid] = ccache.getSize() > Slab::kSize * config.poolMinSizeSlabs;
    validReceiver[pid] =
        ccache.getConfiguredSize() > 0 &&
        ccache.getSize() + Slab::kSize * config.poolMaxFreeSlabs >
            ccache.getConfiguredSize();
  }
  compactCacheState_.updateRankings(scores, config.movingAverageParam);
  return pickVictimAndReceiverFromRankings(
      compactCacheState_, validVictim, validReceiver);
}

} // namespace cachelib
} // namespace facebook
