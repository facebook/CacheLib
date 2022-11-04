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

#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {

template <typename CacheT>
void Reaper<CacheT>::work() {
  reapSlabWalkMode();
}

template <typename CacheT>
void Reaper<CacheT>::TraversalStats::recordTraversalTime(uint64_t msTaken) {
  lastTraversalTimeMs_.store(msTaken, std::memory_order_relaxed);
  minTraversalTimeMs_.store(std::min(minTraversalTimeMs_.load(), msTaken),
                            std::memory_order_relaxed);
  maxTraversalTimeMs_.store(std::max(maxTraversalTimeMs_.load(), msTaken),
                            std::memory_order_relaxed);
  totalTraversalTimeMs_.fetch_add(msTaken, std::memory_order_relaxed);
}

template <typename CacheT>
uint64_t Reaper<CacheT>::TraversalStats::getAvgTraversalTimeMs(
    uint64_t numTraversals) const {
  return numTraversals ? totalTraversalTimeMs_ / numTraversals : 0;
}

template <typename CacheT>
void Reaper<CacheT>::reapSlabWalkMode() {
  util::Throttler t(throttlerConfig_);
  const auto begin = util::getCurrentTimeMs();
  auto currentTimeSec = util::getCurrentTimeSec();

  // use a local to accumulate counts since the lambda could be executed
  // millions of times per sec.
  uint64_t visits = 0;
  uint64_t reaps = 0;

  // unlike the iterator mode, in this mode, we traverse all the way
  ReaperAPIWrapper<CacheT>::traverseAndExpireItems(
      cache_, [&](void* ptr, facebook::cachelib::AllocInfo allocInfo) -> bool {
        XDCHECK(ptr);
        // see if we need to stop the traversal and accumulate counts to
        // global
        if (visits++ == kCheckThreshold) {
          numVisitedItems_.fetch_add(visits, std::memory_order_relaxed);
          numReapedItems_.fetch_add(reaps, std::memory_order_relaxed);
          visits = 0;
          reaps = 0;

          // abort the current iteration since we have to stop
          if (shouldStopWork()) {
            return false;
          }

          currentTimeSec = util::getCurrentTimeSec();
        }

        // if we throttle, then we should check for stop condition after
        // the throttler has actually throttled us.
        if (t.throttle() && shouldStopWork()) {
          return false;
        }

        // get an item and check if it is expired and is in the access
        // container before we actually grab the
        // handle to the item and proceed to expire it.
        const auto& item = *reinterpret_cast<const Item*>(ptr);
        if (!item.isExpired(currentTimeSec) || !item.isAccessible()) {
          return true;
        }

        // Item has to be smaller than the alloc size to be a valid item.
        auto key = item.getKey();
        if (Item::getRequiredSize(key, 0 /* value size*/) >
            allocInfo.allocSize) {
          return true;
        }

        try {
          // obtain a valid handle without disturbing the state of the item in
          // cache.
          auto handle = cache_.peek(key);
          auto reaped =
              ReaperAPIWrapper<CacheT>::removeIfExpired(cache_, handle);
          if (reaped) {
            reaps++;
          }
        } catch (const std::exception& e) {
          numErrs_.fetch_add(1, std::memory_order_relaxed);
          XLOGF(DBG, "Error while reaping. Msg = {}", e.what());
        }
        return true;
      });

  // accumulate any left over visits, reaps.
  numVisitedItems_.fetch_add(visits, std::memory_order_relaxed);
  numReapedItems_.fetch_add(reaps, std::memory_order_relaxed);
  auto end = util::getCurrentTimeMs();
  traversalStats_.recordTraversalTime(end > begin ? end - begin : 0);
}

template <typename CacheT>
Reaper<CacheT>::Reaper(Cache& cache, const util::Throttler::Config& config)
    : cache_(cache), throttlerConfig_(config) {}

template <typename CacheT>
Reaper<CacheT>::~Reaper() {
  stop(std::chrono::seconds(0));
}

template <typename CacheT>
ReaperStats Reaper<CacheT>::getStats() const noexcept {
  ReaperStats stats;
  stats.numVisitedItems = numVisitedItems_.load(std::memory_order_relaxed);
  stats.numReapedItems = numReapedItems_.load(std::memory_order_relaxed);
  stats.numVisitErrs = numErrs_.load(std::memory_order_relaxed);
  auto runCount = getRunCount();
  stats.numTraversals = runCount;
  stats.lastTraversalTimeMs = traversalStats_.getLastTraversalTimeMs();
  stats.avgTraversalTimeMs = traversalStats_.getAvgTraversalTimeMs(runCount);
  stats.minTraversalTimeMs = traversalStats_.getMinTraversalTimeMs();
  stats.maxTraversalTimeMs = traversalStats_.getMaxTraversalTimeMs();
  return stats;
}

} // namespace cachelib
} // namespace facebook
