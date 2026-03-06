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

#include "cachelib/allocator/KAllocation.h"

namespace facebook::cachelib::interface {

using Key = KAllocation::Key;

/**
 * A cache item that is managed by a cache component.
 */
class CacheItem {
 public:
  CacheItem() = default;
  virtual ~CacheItem() = default;

  // CacheItem is not copyable or movable
  CacheItem(const CacheItem&) = delete;
  CacheItem& operator=(const CacheItem&) = delete;
  CacheItem(CacheItem&&) = delete;
  CacheItem& operator=(CacheItem&&) = delete;

  // ------------------------------ Interface ------------------------------ //

  /**
   * Get the creation or expiration time, respectively, of the item.
   * @return creation or expiration time
   */
  virtual uint32_t getCreationTime() const noexcept = 0;
  virtual uint32_t getExpiryTime() const noexcept = 0;

  /**
   * Increment the refcount on the item.
   */
  virtual void incrementRefCount() noexcept = 0;

  /**
   * Decrement the refcount on the item. Return whether the item is ready to be
   * released, usually because refcount == 0.
   *
   * @return true if the item is ready to be released or false otherwise
   */
  virtual bool decrementRefCount() noexcept = 0;

  /**
   * Get the cache item key.
   * @return cache item key
   */
  virtual Key getKey() const noexcept = 0;

  /**
   * Get a pointer to the cache item value's memory. Use getMemoryAs() to
   * automatically cast the memory to a concrete type.
   *
   * @return pointer to the value memory
   */
  virtual void* getMemory() const noexcept = 0;

  /**
   * Helper to cast memory to a concrete type.
   * @return pointer to the value
   */
  template <typename T>
  T* getMemoryAs() const noexcept {
    return reinterpret_cast<T*>(getMemory());
  }

  /**
   * Get the size of the key. Default implementation just calls getKey().size(),
   * sub-classes may have a more efficient method to get the key size.
   *
   * @return key size in bytes
   */
  virtual uint32_t getKeySize() const noexcept;

  /**
   * Get the size of the memory allocated for the cache item value.
   * @return allocated memory size in bytes
   */
  virtual uint32_t getMemorySize() const noexcept = 0;

  /**
   * Get the total size of the cache item, including item header/metadata, key &
   * value memory.
   *
   * @return total size of cache item
   */
  virtual uint32_t getTotalSize() const noexcept = 0;
};

} // namespace facebook::cachelib::interface
