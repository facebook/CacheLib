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

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook::cachelib {
// wrapper that exposes the private APIs of CacheType that are specifically
// needed for the cache api
template <typename C>
struct BackgroundMoverAPIWrapper {
  // traverse the cache and move items from one tier to another
  // @param cache             the cache interface
  // @param tid               the tier to traverse
  // @param pid               the pool id to traverse
  // @param cid               the class id to traverse
  // @param evictionBatch     number of items to evict in one go
  // @param promotionBatch    number of items to promote in one go
  // @return pair of number of items evicted and promoted
  static std::pair<size_t, size_t> traverseAndMoveItems(C& cache,
                                                        TierId tid,
                                                        PoolId pid,
                                                        ClassId cid,
                                                        size_t evictionBatch,
                                                        size_t promotionBatch) {
    return cache.traverseAndMoveItems(tid, pid, cid, evictionBatch, promotionBatch);
  }
  static std::pair<size_t, double> getApproxUsage(C& cache,
                                                  TierId tid,
                                                  PoolId pid,
                                                  ClassId cid) {
    const auto& pool = cache.getPoolByTid(pid, tid);
    // we wait until all slabs are allocated before we start evicting
    if (!pool.allSlabsAllocated()) {
      return {0, 0.0};
    }
    return pool.getApproxUsage(cid);
  }
  static unsigned int getNumTiers(C& cache) {
    return cache.getNumTiers();
  }
};

// Periodic worker that evicts items from tiers in batches
// The primary aim is to reduce insertion times for new items in the
// cache
template <typename CacheT>
class BackgroundMover : public PeriodicWorker {
 public:
  using ClassBgStatsType =
      std::map<MemoryDescriptorType, std::pair<size_t, size_t>>;
  using Cache = CacheT;
  // @param cache               the cache interface
  // @param evictionBatch       number of items to evict in one go
  // @param promotionBatch      number of items to promote in one go
  // @param targetFree          target free percentage in the class
  BackgroundMover(Cache& cache,
                  size_t evictionBatch,
                  size_t promotionBatch,
                  double targetFree);

  ~BackgroundMover() override;

  BackgroundMoverStats getStats() const noexcept;
  ClassBgStatsType getPerClassStats() const noexcept { return movesPerClass_; }

  void setAssignedMemory(std::vector<MemoryDescriptorType>&& assignedMemory);

  // return id of the worker responsible for promoting/evicting from particlar
  // pool and allocation calss (id is in range [0, numWorkers))
  static size_t workerId(TierId tid, PoolId pid, ClassId cid, size_t numWorkers);

 private:
  struct TraversalStats {
    // record a traversal over all assigned classes
    // and its time taken
    void recordTraversalTime(uint64_t nsTaken);

    uint64_t getAvgTraversalTimeNs(uint64_t numTraversals) const;
    uint64_t getMinTraversalTimeNs() const { return minTraversalTimeNs_; }
    uint64_t getMaxTraversalTimeNs() const { return maxTraversalTimeNs_; }
    uint64_t getLastTraversalTimeNs() const { return lastTraversalTimeNs_; }

   private:
    // time it took us the last time to traverse the cache.
    uint64_t lastTraversalTimeNs_{0};
    uint64_t minTraversalTimeNs_{std::numeric_limits<uint64_t>::max()};
    uint64_t maxTraversalTimeNs_{0};
    uint64_t totalTraversalTimeNs_{0};
  };

  TraversalStats traversalStats_;
  // cache allocator's interface for evicting
  using Item = typename Cache::Item;

  Cache& cache_;
  uint8_t numTiers_{1}; // until we have multi-tier support
  size_t evictionBatch_{0};
  size_t promotionBatch_{0};
  double targetFree_{0.03};

  // implements the actual logic of running the background evictor
  void work() override final;
  void checkAndRun();

  // populates the toFree map for each class with the number of items to free
  std::map<MemoryDescriptorType, size_t> getNumItemsToFree(
      const std::vector<MemoryDescriptorType>& assignedMemory);

  uint64_t numEvictedItems_{0};
  uint64_t numPromotedItems_{0};
  uint64_t numTraversals_{0};

  ClassBgStatsType movesPerClass_;

  std::vector<MemoryDescriptorType> assignedMemory_;
  folly::DistributedMutex mutex_;
};

template <typename CacheT>
BackgroundMover<CacheT>::BackgroundMover(Cache& cache,
                                         size_t evictionBatch,
                                         size_t promotionBatch,
                                         double targetFree)
    : cache_(cache),
      evictionBatch_(evictionBatch),
      promotionBatch_(promotionBatch),
      targetFree_(targetFree) {
        numTiers_ = BackgroundMoverAPIWrapper<CacheT>::getNumTiers(cache_);
      }

template <typename CacheT>
void BackgroundMover<CacheT>::TraversalStats::recordTraversalTime(
    uint64_t nsTaken) {
  lastTraversalTimeNs_ = nsTaken;
  minTraversalTimeNs_ = std::min(minTraversalTimeNs_, nsTaken);
  maxTraversalTimeNs_ = std::max(maxTraversalTimeNs_, nsTaken);
  totalTraversalTimeNs_ += nsTaken;
}

template <typename CacheT>
uint64_t BackgroundMover<CacheT>::TraversalStats::getAvgTraversalTimeNs(
    uint64_t numTraversals) const {
  return numTraversals ? totalTraversalTimeNs_ / numTraversals : 0;
}

template <typename CacheT>
BackgroundMover<CacheT>::~BackgroundMover() {
  stop(std::chrono::seconds(0));
}

template <typename CacheT>
void BackgroundMover<CacheT>::work() {
  try {
    checkAndRun();
  } catch (const std::exception& ex) {
    XLOGF(ERR, "BackgroundMover interrupted due to exception: {}", ex.what());
  }
}

template <typename CacheT>
void BackgroundMover<CacheT>::setAssignedMemory(
    std::vector<MemoryDescriptorType>&& assignedMemory) {
  XLOG(INFO, "Class assigned to background worker:");
  for (auto [tid, pid, cid] : assignedMemory) {
    XLOGF(INFO, "Tid: {}, Pid: {}, Cid: {}", tid, pid, cid);
  }

  mutex_.lock_combine([this, &assignedMemory] {
    this->assignedMemory_ = std::move(assignedMemory);
  });
}

template <typename CacheT>
std::map<MemoryDescriptorType, size_t>
BackgroundMover<CacheT>::getNumItemsToFree(
    const std::vector<MemoryDescriptorType>& assignedMemory) {
  std::map<MemoryDescriptorType, size_t> toFree;
  for (const auto& md : assignedMemory) {
    const auto [tid, pid, cid] = md;
    const auto& pool = cache_.getPool(pid);
    const auto [activeItems, usage] =
        BackgroundMoverAPIWrapper<CacheT>::getApproxUsage(cache_, tid, pid, cid);
    if (usage < 1 - targetFree_) {
      toFree[md] = 0;
    } else {
      size_t maxItems = activeItems / usage;
      size_t targetItems = maxItems * (1 - targetFree_);
      size_t toFreeItems =
          activeItems > targetItems ? activeItems - targetItems : 0;
      toFree[md] = toFreeItems;
    }
  }
  return toFree;
}

template <typename CacheT>
void BackgroundMover<CacheT>::checkAndRun() {
  auto assignedMemory = mutex_.lock_combine([this] { return assignedMemory_; });
  auto toFree = getNumItemsToFree(assignedMemory); // calculate the number of
                                                   // items to free
  while (true) {
    bool allDone = true;
    for (auto md : assignedMemory) {
      const auto [tid, pid, cid] = md;
      size_t evictionBatch = evictionBatch_;
      size_t promotionBatch = 0; // will enable with multi-tier support
      if (toFree[md] == 0) {
        // no eviction work to be done since there is already at least
        // targetFree remaining in the class
        evictionBatch = 0;
      } else {
        allDone = false; // we still have some items to free
      }
      if (promotionBatch + evictionBatch > 0) {
        const auto begin = util::getCurrentTimeNs();
        // try moving BATCH items from the class in order to reach free target
        auto moved = BackgroundMoverAPIWrapper<CacheT>::traverseAndMoveItems(
            cache_, tid, pid, cid, evictionBatch, promotionBatch);
        numEvictedItems_ += moved.first;
        toFree[md] > moved.first ? toFree[md] -= moved.first : toFree[md] = 0;
        numPromotedItems_ += moved.second;
        auto curr = movesPerClass_[md];
        curr.first += moved.first;
        curr.second += moved.second;
        movesPerClass_[md] = curr;
        numTraversals_++;
        auto end = util::getCurrentTimeNs();
        traversalStats_.recordTraversalTime(end > begin ? end - begin : 0);
      }
    }
    if (shouldStopWork() || allDone) {
      break;
    }
  }
}

template <typename CacheT>
BackgroundMoverStats BackgroundMover<CacheT>::getStats() const noexcept {
  BackgroundMoverStats stats;
  stats.numEvictedItems = numEvictedItems_;
  stats.numPromotedItems = numPromotedItems_;
  stats.numTraversals = numTraversals_;
  stats.runCount = getRunCount();
  stats.avgItemsMoved =
      (double)(stats.numEvictedItems + stats.numPromotedItems) /
      (double)numTraversals_;
  stats.lastTraversalTimeNs = traversalStats_.getLastTraversalTimeNs();
  stats.avgTraversalTimeNs =
      traversalStats_.getAvgTraversalTimeNs(numTraversals_);
  stats.minTraversalTimeNs = traversalStats_.getMinTraversalTimeNs();
  stats.maxTraversalTimeNs = traversalStats_.getMaxTraversalTimeNs();

  return stats;
}

template <typename CacheT>
size_t BackgroundMover<CacheT>::workerId(TierId tid,
                                         PoolId pid,
                                         ClassId cid,
                                         size_t numWorkers) {
  XDCHECK(numWorkers);

  // TODO: came up with some better sharding (use hashing?)
  return (tid + pid + cid) % numWorkers;
}
}; // namespace facebook::cachelib
