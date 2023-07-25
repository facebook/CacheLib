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

#include "cachelib/allocator/BackgroundMoverStrategy.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// wrapper that exposes the private APIs of CacheType that are specifically
// needed for the cache api
template <typename C>
struct BackgroundMoverAPIWrapper {
  static size_t traverseAndEvictItems(C& cache,
                                      unsigned int pid,
                                      unsigned int cid,
                                      size_t batch) {
    return cache.traverseAndEvictItems(pid, cid, batch);
  }

  static size_t traverseAndPromoteItems(C& cache,
                                        unsigned int pid,
                                        unsigned int cid,
                                        size_t batch) {
    return cache.traverseAndPromoteItems(pid, cid, batch);
  }
};

enum class MoverDir { Evict = 0, Promote };

// Periodic worker that evicts items from tiers in batches
// The primary aim is to reduce insertion times for new items in the
// cache
template <typename CacheT>
class BackgroundMover : public PeriodicWorker {
 public:
  using Cache = CacheT;
  // @param cache               the cache interface
  // @param strategy            the stragey class that defines how objects are
  // moved (promoted vs. evicted and how much)
  BackgroundMover(Cache& cache,
                  std::shared_ptr<BackgroundMoverStrategy> strategy,
                  MoverDir direction_);

  ~BackgroundMover() override;

  BackgroundMoverStats getStats() const noexcept;
  std::map<PoolId, std::map<ClassId, uint64_t>> getClassStats() const noexcept;

  void setAssignedMemory(std::vector<MemoryDescriptorType>&& assignedMemory);

  // return id of the worker responsible for promoting/evicting from particlar
  // pool and allocation calss (id is in range [0, numWorkers))
  static size_t workerId(PoolId pid, ClassId cid, size_t numWorkers);

 private:
  std::map<PoolId, std::map<ClassId, uint64_t>> movesPerClass_;
  // cache allocator's interface for evicting
  using Item = typename Cache::Item;

  Cache& cache_;
  std::shared_ptr<BackgroundMoverStrategy> strategy_;
  MoverDir direction_;

  std::function<size_t(Cache&, unsigned int, unsigned int, size_t)> moverFunc;

  // implements the actual logic of running the background evictor
  void work() override final;
  void checkAndRun();

  AtomicCounter numMovedItems_{0};
  AtomicCounter numTraversals_{0};
  AtomicCounter totalBytesMoved_{0};

  std::vector<MemoryDescriptorType> assignedMemory_;
  folly::DistributedMutex mutex_;
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/BackgroundMover-inl.h"
