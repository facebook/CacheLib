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

#include <limits>

#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook::cachelib {
// wrapper that exposes the private APIs of CacheType that are specifically
// needed for the Reaper.
template <typename C>
struct ReaperAPIWrapper {
  using Item = typename C::Item;
  using Key = typename Item::Key;
  using WriteHandle = typename C::WriteHandle;
  using ReadHandle = typename C::ReadHandle;

  static std::set<PoolId> getRegularPoolIds(C& cache) {
    return cache.getRegularPoolIds();
  }

  static bool removeIfExpired(C& cache, const ReadHandle& handle) {
    return cache.removeIfExpired(handle);
  }

  template <typename Fn>
  static void traverseAndExpireItems(C& cache, Fn&& f) {
    cache.traverseAndExpireItems(std::forward<Fn>(f));
  }

  static WriteHandle findInternal(C& cache, Key key) {
    return cache.findInternal(key);
  }
};

// Remove the items that are expired in the cache. Creates a new thread
// for background checking with throttler to reap the expired items.
template <typename CacheT>
class Reaper : public PeriodicWorker {
 public:
  using Cache = CacheT;
  // this initialized an itemsReaper to check expired itemsReaper
  // @param cache               instance of the cache
  // @param config              throttler config during iteration
  Reaper(Cache& cache, const util::Throttler::Config& config);

  ~Reaper();

  ReaperStats getStats() const noexcept;

 private:
  struct TraversalStats {
    // record a traversal and its time taken
    void recordTraversalTime(uint64_t msTaken);

    uint64_t getAvgTraversalTimeMs(uint64_t numTraversals) const;
    uint64_t getMinTraversalTimeMs() const { return minTraversalTimeMs_; }
    uint64_t getMaxTraversalTimeMs() const { return maxTraversalTimeMs_; }
    uint64_t getLastTraversalTimeMs() const { return lastTraversalTimeMs_; }
    uint64_t getNumTraversals() const { return numTraversals_; }

   private:
    // time it took us the last time to traverse the cache.
    std::atomic<uint64_t> lastTraversalTimeMs_{0};
    std::atomic<uint64_t> minTraversalTimeMs_{
        std::numeric_limits<uint64_t>::max()};
    std::atomic<uint64_t> maxTraversalTimeMs_{0};
    std::atomic<uint64_t> totalTraversalTimeMs_{0};
    std::atomic<uint64_t> numTraversals_{0};
  };

  using Item = typename Cache::Item;

  // implement logic in the virtual function in PeriodicWorker
  // check whether the items is expired or not
  void work() override final;

  void reapSlabWalkMode();

  // reference to the cache
  Cache& cache_;

  const util::Throttler::Config throttlerConfig_;

  TraversalStats traversalStats_;

  // stats on visited items
  std::atomic<uint64_t> numVisitedItems_{0};
  std::atomic<uint64_t> numReapedItems_{0};
  std::atomic<uint64_t> numErrs_{0};

  // number of items to visit before we check for stopping the worker in super
  // charged mode.
  static constexpr const uint64_t kCheckThreshold = 1ULL << 22;
};

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
          auto handle = ReaperAPIWrapper<CacheT>::findInternal(cache_, key);
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
} // namespace facebook::cachelib
