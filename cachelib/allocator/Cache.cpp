/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "cachelib/allocator/Cache.h"

#include <mutex>

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

void CacheBase::setRebalanceStrategy(
    PoolId pid, std::shared_ptr<RebalanceStrategy> strategy) {
  std::unique_lock<std::mutex> l(lock_);
  poolRebalanceStrategies_[pid] = std::move(strategy);
}

std::shared_ptr<RebalanceStrategy> CacheBase::getRebalanceStrategy(
    PoolId pid) const {
  std::unique_lock<std::mutex> l(lock_);
  auto it = poolRebalanceStrategies_.find(pid);
  if (it != poolRebalanceStrategies_.end() && it->second) {
    return it->second;
  }
  return nullptr;
}

void CacheBase::setResizeStrategy(PoolId pid,
                                  std::shared_ptr<RebalanceStrategy> strategy) {
  std::unique_lock<std::mutex> l(lock_);
  poolResizeStrategies_[pid] = std::move(strategy);
}

std::shared_ptr<RebalanceStrategy> CacheBase::getResizeStrategy(
    PoolId pid) const {
  std::unique_lock<std::mutex> l(lock_);
  auto it = poolResizeStrategies_.find(pid);
  if (it != poolResizeStrategies_.end() && it->second) {
    return it->second;
  }
  return nullptr;
}

void CacheBase::setPoolOptimizeStrategy(
    std::shared_ptr<PoolOptimizeStrategy> strategy) {
  std::unique_lock<std::mutex> l(lock_);
  poolOptimizeStrategy_ = std::move(strategy);
}

std::shared_ptr<PoolOptimizeStrategy> CacheBase::getPoolOptimizeStrategy()
    const {
  std::unique_lock<std::mutex> l(lock_);
  return poolOptimizeStrategy_;
}
} // namespace cachelib
} // namespace facebook
