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

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"

namespace facebook {
namespace cachelib {

// manager for compact cache
// It manages the multiple pools of compact caches (CCacheAllocator) and
// interacts with cache allocator.
class CCacheManager {
 public:
  using SerializationType = serialization::CompactCacheAllocatorManagerObject;
  /**
   * Restores the state of all compact cache allocators using information
   * in an object.
   *
   * @param object      state object to be restored
   * @param allocator   the memory allocator to restore pools from
   * @return true on success, false on failure. On failure errno is set to
   *         EINVAL if some allocator can not be found
   *
   * @note This function does not clean up on errors. If an error is returned
   *       the cache is likely left in a partially initialized state. All errors
   *       are assumed to be fatal.
   */
  CCacheManager(const SerializationType& object,
                MemoryAllocator& memoryAllocator);

  explicit CCacheManager(MemoryAllocator& memoryAllocator)
      : memoryAllocator_(memoryAllocator) {}

  // Add a new allocator with given name and poolId
  CCacheAllocator& addAllocator(const std::string& name, PoolId poolId);

  // Get the allocator with given name
  CCacheAllocator& getAllocator(const std::string& name);

  /**
   * Resize all compact caches attached to the allocators
   */
  void resizeAll();

  /**
   * Save the state of all compact cache allocators in an object
   *
   * @return object that contains the state
   */
  SerializationType saveState();

 private:
  std::mutex lock_;
  MemoryAllocator& memoryAllocator_;

  // Mapping from pool names to allocators
  // Compact cache will have direct reference of the allocator, which is fine
  // according to http://en.cppreference.com/w/cpp/container/unordered_map
  // "References and pointers to either key or data stored in the container are
  // only invalidated by erasing that element, even when the corresponding
  // iterator is invalidated."
  std::unordered_map<std::string, CCacheAllocator> allocators_;
};

} // namespace cachelib
} // namespace facebook
