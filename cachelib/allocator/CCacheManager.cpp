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

#include "cachelib/allocator/CCacheManager.h"

namespace facebook {
namespace cachelib {

CCacheManager::CCacheManager(const SerializationType& object,
                             MemoryAllocator& memoryAllocator)
    : memoryAllocator_(memoryAllocator) {
  std::lock_guard<std::mutex> guard(lock_);

  for (const auto& allocator : *object.allocators()) {
    auto id = memoryAllocator_.getPoolId(allocator.first);
    allocators_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(allocator.first),
        std::forward_as_tuple(memoryAllocator_, id, allocator.second));
  }
}

CCacheAllocator& CCacheManager::addAllocator(const std::string& name,
                                             PoolId poolId) {
  std::lock_guard<std::mutex> guard(lock_);
  auto result =
      allocators_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(name),
                          std::forward_as_tuple(memoryAllocator_, poolId));
  if (!result.second) {
    throw std::invalid_argument(
        folly::sformat("Duplicate allocator named {}", name));
  }
  return result.first->second;
}

CCacheAllocator& CCacheManager::getAllocator(const std::string& name) {
  std::lock_guard<std::mutex> guard(lock_);
  return allocators_.at(name);
}

void CCacheManager::resizeAll() {
  // put all allocator pointers into a vector Before starting resizing because
  // resizing compact cache involves rehashing which can take a long time, and
  // we don't want to hold the lock for the entire duration
  std::vector<CCacheAllocator*> allAllocators;
  {
    std::lock_guard<std::mutex> guard(lock_);
    for (auto& allocator : allocators_) {
      allAllocators.push_back(&allocator.second);
    }
  }

  for (auto& allocator : allAllocators) {
    // shrink oversized compact caches first
    if (allocator->overSized()) {
      allocator->resizeCompactCache();
    }
  }
  for (auto& allocator : allAllocators) {
    // resize all compact cache
    allocator->resizeCompactCache();
  }
}

CCacheManager::SerializationType CCacheManager::saveState() {
  std::lock_guard<std::mutex> guard(lock_);

  SerializationType object;
  for (auto& allocator : allocators_) {
    object.allocators()->emplace(allocator.first, allocator.second.saveState());
  }
  return object;
}

} // namespace cachelib
} // namespace facebook
