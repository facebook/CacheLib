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

#include "cachelib/allocator/memory/MemoryPoolManager.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

using namespace facebook::cachelib;

constexpr unsigned int MemoryPoolManager::kMaxPools;

MemoryPoolManager::MemoryPoolManager(SlabAllocator& slabAlloc)
    : slabAlloc_(slabAlloc) {}

MemoryPoolManager::MemoryPoolManager(
    const serialization::MemoryPoolManagerObject& object,
    SlabAllocator& slabAlloc)
    : nextPoolId_(*object.nextPoolId()), slabAlloc_(slabAlloc) {
  if (!slabAlloc_.isRestorable()) {
    throw std::logic_error(
        "Memory Pool Manager can not be restored,"
        " slabAlloc not restored");
  }
  // Check if nextPoolid is restored properly or not. If not restored,
  // throw error
  if (!object.nextPoolId().is_set()) {
    throw std::logic_error(
        "Memory Pool Manager can not be restored,"
        " nextPoolId is not set");
  }

  // Number of items in pools must be same as nextPoolId, if not throw error
  if (object.pools()->size() != static_cast<size_t>(nextPoolId_)) {
    throw std::logic_error(
        "Memory Pool Manager can not be restored,"
        "pools size is not equal to nextPoolId");
  }
  size_t slabsAdvised = 0;
  for (size_t i = 0; i < object.pools()->size(); ++i) {
    pools_[i].reset(new MemoryPool(object.pools()[i], slabAlloc_));
    slabsAdvised += pools_[i]->getNumSlabsAdvised();
  }
  for (const auto& kv : *object.poolsByName()) {
    poolsByName_.insert(kv);
  }

  // Number items in the poolsByName map must be same as nextPoolId, if not
  // throw error
  if (object.poolsByName()->size() != static_cast<size_t>(nextPoolId_)) {
    throw std::logic_error(
        "Memory Pool Manager can not be restored,"
        "poolsByName size is not equal to nextPoolId");
  }
  numSlabsToAdvise_ = slabAlloc_.numSlabsReclaimable();
  if (slabsAdvised != numSlabsToAdvise_) {
    throw std::logic_error(folly::sformat(
        "Aggregate of advised slabs in pools {} is not same as SlabAllocator"
        " number of slabs advised {}",
        slabsAdvised, numSlabsToAdvise_.load()));
  }
}

size_t MemoryPoolManager::getRemainingSizeLocked() const noexcept {
  const size_t totalSize =
      slabAlloc_.getNumUsableAndAdvisedSlabs() * Slab::kSize;
  // check if there is enough space left.
  size_t sum = 0;
  for (PoolId id = 0; id < nextPoolId_; id++) {
    sum += pools_[id]->getPoolSize();
  }
  XDCHECK_LE(sum, totalSize);
  return totalSize - sum;
}

PoolId MemoryPoolManager::createNewPool(folly::StringPiece name,
                                        size_t poolSize,
                                        const std::set<uint32_t>& allocSizes) {
  folly::SharedMutex::WriteHolder l(lock_);
  if (poolsByName_.find(name) != poolsByName_.end()) {
    throw std::invalid_argument("Duplicate pool");
  }

  if (nextPoolId_ == kMaxPools) {
    throw std::logic_error("All pools exhausted");
  }

  const size_t remaining = getRemainingSizeLocked();
  if (remaining < poolSize) {
    // not enough memory to create a new pool.
    throw std::invalid_argument(folly::sformat(
        "Not enough memory ({} bytes) to create a new pool of size {} bytes",
        remaining,
        poolSize));
  }

  const PoolId id = nextPoolId_;
  pools_[id].reset(new MemoryPool(id, poolSize, slabAlloc_, allocSizes));
  poolsByName_.insert({name.str(), id});
  nextPoolId_++;
  return id;
}

MemoryPool& MemoryPoolManager::getPoolByName(const std::string& name) const {
  folly::SharedMutex::ReadHolder l(lock_);
  auto it = poolsByName_.find(name);
  if (it == poolsByName_.end()) {
    throw std::invalid_argument(folly::sformat("Invalid pool name {}", name));
  }

  auto poolId = it->second;
  XDCHECK_LT(poolId, nextPoolId_.load());
  XDCHECK_GE(poolId, 0);
  XDCHECK(pools_[poolId] != nullptr);
  return *pools_[poolId];
}

MemoryPool& MemoryPoolManager::getPoolById(PoolId id) const {
  // does not need to grab the lock since nextPoolId_ is atomic and we always
  // bump it up after setting everything up..
  if (id < nextPoolId_ && id >= 0) {
    XDCHECK(pools_[id] != nullptr);
    return *pools_[id];
  }
  throw std::invalid_argument(folly::sformat("Invalid pool id {}", id));
}

const std::string& MemoryPoolManager::getPoolNameById(PoolId id) const {
  folly::SharedMutex::ReadHolder l(lock_);
  for (const auto& pair : poolsByName_) {
    if (pair.second == id) {
      return pair.first;
    }
  }
  throw std::invalid_argument(folly::sformat("Invali pool id {}", id));
}

serialization::MemoryPoolManagerObject MemoryPoolManager::saveState() const {
  if (!slabAlloc_.isRestorable()) {
    throw std::logic_error("Memory Pool Manager can not be restored");
  }

  serialization::MemoryPoolManagerObject object;

  object.pools().emplace();
  for (PoolId i = 0; i < nextPoolId_; ++i) {
    object.pools()->push_back(pools_[i]->saveState());
  }
  object.poolsByName().emplace();
  for (const auto& kv : poolsByName_) {
    object.poolsByName()->insert(kv);
  }
  object.nextPoolId() = nextPoolId_;

  return object;
}

std::set<PoolId> MemoryPoolManager::getPoolIds() const {
  std::set<PoolId> ret;
  for (PoolId id = 0; id < nextPoolId_; ++id) {
    ret.insert(id);
  }
  return ret;
}

bool MemoryPoolManager::resizePools(PoolId src, PoolId dest, size_t bytes) {
  auto& srcPool = getPoolById(src);
  auto& destPool = getPoolById(dest);

  folly::SharedMutex::WriteHolder l(lock_);
  if (srcPool.getPoolSize() < bytes) {
    return false;
  }

  // move the memory.
  srcPool.resize(srcPool.getPoolSize() - bytes);
  destPool.resize(destPool.getPoolSize() + bytes);
  return true;
}

bool MemoryPoolManager::shrinkPool(PoolId pid, size_t bytes) {
  auto& pool = getPoolById(pid);

  folly::SharedMutex::WriteHolder l(lock_);
  if (pool.getPoolSize() < bytes) {
    return false;
  }
  pool.resize(pool.getPoolSize() - bytes);
  return true;
}

bool MemoryPoolManager::growPool(PoolId pid, size_t bytes) {
  auto& pool = getPoolById(pid);

  folly::SharedMutex::WriteHolder l(lock_);
  const auto remaining = getRemainingSizeLocked();
  if (remaining < bytes) {
    return false;
  }

  pool.resize(pool.getPoolSize() + bytes);
  return true;
}

std::set<PoolId> MemoryPoolManager::getPoolsOverLimit() const {
  std::set<PoolId> res;
  folly::SharedMutex::ReadHolder l(lock_);
  for (const auto& kv : poolsByName_) {
    const auto poolId = kv.second;
    const auto& pool = getPoolById(poolId);
    if (pool.overLimit()) {
      res.insert(poolId);
    }
  }
  return res;
}

// Helper routine to determine the target number of slabs to be advised in
// each pool. This first assigns the target based on integer division and
// any remainder is addressed afterwards. The goal is to make the sum of
// target slabs to advise equal to numSlabsToAdvise_
std::unordered_map<PoolId, uint64_t> MemoryPoolManager::getTargetSlabsToAdvise(
    std::set<PoolId> poolIds,
    uint64_t totalSlabsInUse,
    std::unordered_map<PoolId, size_t>& numSlabsInUse) const {
  uint64_t sum = 0;
  std::unordered_map<PoolId, uint64_t> targets;
  for (auto id : poolIds) {
    targets[id] = numSlabsInUse[id] * numSlabsToAdvise_ / totalSlabsInUse;
    sum += targets[id];
  }
  for (auto it = poolIds.begin();
       it != poolIds.end() && sum < numSlabsToAdvise_;
       ++it) {
    auto id = *it;
    if (((targets[id] * totalSlabsInUse) / numSlabsToAdvise_) <
        numSlabsInUse[id]) {
      targets[id]++;
      sum++;
    }
  }
  XDCHECK_EQ(sum, numSlabsToAdvise_);

  return targets;
}

PoolAdviseReclaimData MemoryPoolManager::calcNumSlabsToAdviseReclaim(
    const std::set<PoolId>& poolIds) const {
  folly::SharedMutex::WriteHolder l(lock_);
  uint64_t totalSlabsAdvised = 0;
  uint64_t totalSlabsInUse = 0;
  std::unordered_map<PoolId, size_t> numSlabsInUse;
  for (auto id : poolIds) {
    // Get the individual pool slab usage and cache it so that any changes to
    // it do not reflect in the subsequent calculations.
    numSlabsInUse[id] = pools_[id]->getCurrentUsedSize() / Slab::kSize;
    totalSlabsInUse += numSlabsInUse[id];
    totalSlabsAdvised += pools_[id]->getNumSlabsAdvised();
  }
  PoolAdviseReclaimData results;
  results.advise = false;
  // No slabs in use or no slabs advised and no slabs to advise return empty map
  if (totalSlabsInUse == 0 ||
      (numSlabsToAdvise_ == 0 && totalSlabsAdvised == 0)) {
    return results;
  }
  auto poolAdviseTargets =
      getTargetSlabsToAdvise(poolIds, totalSlabsInUse, numSlabsInUse);
  if (numSlabsToAdvise_ == totalSlabsAdvised) {
    // No need to advise-away or reclaim any new slabs.
    // Just rebalance the advised away slabs in each pool
    for (auto& target : poolAdviseTargets) {
      pools_[target.first]->setNumSlabsAdvised(target.second);
    }
    return results;
  }

  if (numSlabsToAdvise_ > totalSlabsAdvised) {
    results.advise = true;
    uint64_t diff = numSlabsToAdvise_ - totalSlabsAdvised;
    for (auto id : poolIds) {
      if (diff == 0) {
        break;
      }
      uint64_t slabsToAdvise = poolAdviseTargets[id];
      uint64_t currSlabsAdvised = pools_[id]->getNumSlabsAdvised();
      if (slabsToAdvise <= currSlabsAdvised) {
        continue;
      }
      uint64_t poolSlabsToAdvise =
          std::min(slabsToAdvise - currSlabsAdvised, diff);
      results.poolAdviseReclaimMap.emplace(id, poolSlabsToAdvise);
      diff -= poolSlabsToAdvise;
    }
  } else {
    uint64_t diff = totalSlabsAdvised - numSlabsToAdvise_;
    for (auto id : poolIds) {
      if (diff == 0) {
        break;
      }
      auto slabsToAdvise = poolAdviseTargets[id];
      auto currSlabsAdvised = pools_[id]->getNumSlabsAdvised();
      if (slabsToAdvise >= currSlabsAdvised) {
        continue;
      }
      uint64_t poolSlabsToReclaim =
          std::min(currSlabsAdvised - slabsToAdvise, diff);
      results.poolAdviseReclaimMap.emplace(id, poolSlabsToReclaim);
      diff -= poolSlabsToReclaim;
    }
  }
  return results;
}
