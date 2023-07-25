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

namespace facebook {
namespace cachelib {

template <typename CacheT>
BackgroundMover<CacheT>::BackgroundMover(
    Cache& cache,
    std::shared_ptr<BackgroundMoverStrategy> strategy,
    MoverDir direction)
    : cache_(cache), strategy_(strategy), direction_(direction) {
  if (direction_ == MoverDir::Evict) {
    moverFunc = BackgroundMoverAPIWrapper<CacheT>::traverseAndEvictItems;

  } else if (direction_ == MoverDir::Promote) {
    moverFunc = BackgroundMoverAPIWrapper<CacheT>::traverseAndPromoteItems;
  }
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
  for (auto [pid, cid] : assignedMemory) {
    XLOGF(INFO, "Pid: {}, Cid: {}", pid, cid);
  }

  mutex_.lock_combine([this, &assignedMemory] {
    this->assignedMemory_ = std::move(assignedMemory);
  });
}

// Look for classes that exceed the target memory capacity
// and return those for eviction
template <typename CacheT>
void BackgroundMover<CacheT>::checkAndRun() {
  auto assignedMemory = mutex_.lock_combine([this] { return assignedMemory_; });

  unsigned int moves = 0;
  auto batches = strategy_->calculateBatchSizes(cache_, assignedMemory);

  for (size_t i = 0; i < batches.size(); i++) {
    const auto [pid, cid] = assignedMemory[i];
    const auto batch = batches[i];

    if (batch == 0) {
      continue;
    }

    // try moving BATCH items from the class in order to reach free target
    auto moved = moverFunc(cache_, pid, cid, batch);
    moves += moved;
    movesPerClass_[pid][cid] += moved;
    totalBytesMoved_.add(moved * cache_.getPool(pid).getAllocSizes()[cid]);
  }

  numTraversals_.inc();
  numMovedItems_.add(moves);
}

template <typename CacheT>
BackgroundMoverStats BackgroundMover<CacheT>::getStats() const noexcept {
  BackgroundMoverStats stats;
  stats.numMovedItems = numMovedItems_.get();
  stats.runCount = numTraversals_.get();
  stats.totalBytesMoved = totalBytesMoved_.get();

  return stats;
}

template <typename CacheT>
std::map<PoolId, std::map<ClassId, uint64_t>>
BackgroundMover<CacheT>::getClassStats() const noexcept {
  return movesPerClass_;
}

template <typename CacheT>
size_t BackgroundMover<CacheT>::workerId(PoolId pid,
                                         ClassId cid,
                                         size_t numWorkers) {
  XDCHECK(numWorkers);

  // TODO: came up with some better sharding (use hashing?)
  return (pid + cid) % numWorkers;
}

} // namespace cachelib
} // namespace facebook
