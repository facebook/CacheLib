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

#include <folly/CPortability.h>

namespace facebook::cachelib::interface {

class CacheComponent;
class CacheItem;

/**
 * Generic RAII handle referencing a cache item. If you have a handle, it
 * points to a valid cache item (unless it has been moved out of).
 *
 * Automatically increments and decrements the cache item's refcounts during
 * construction/destruction.  If this handle is the last reference to a cache
 * item, then upon this handle's destruction the cache item is released.
 *
 * Designed to work with any cache item, concrete implementations shouldn't need
 * to override handles.
 */
class Handle {
 public:
  /**
   * Whether the handle is valid. All handles are valid unless they have been
   * moved out.
   *
   * @return true if the handle is valid, false if it has been moved out
   */
  FOLLY_ALWAYS_INLINE explicit operator bool() const noexcept {
    return item_ != nullptr;
  }

 protected:
  /**
   * Construct a handle. Only called from sub-classes.
   * @param cache the cache component that owns the cache item
   * @param item the cache item
   * @param inserted whether the cache item has been inserted into cache
   */
  Handle(CacheComponent& cache, CacheItem& item, bool inserted) noexcept;
  ~Handle() noexcept;

  // Handle is *not* copyable
  Handle(const Handle& other) noexcept = delete;
  Handle& operator=(const Handle& other) noexcept = delete;

  // Handle *is* move-constructible but *not* move-assignable. Handle `other` is
  // no longer usable after the move.
  Handle(Handle&& other) noexcept;
  Handle& operator=(Handle&& other) noexcept = delete;

  CacheComponent* cache_;
  CacheItem* item_;

 private:
  // Whether the CacheItem has been inserted
  bool inserted_;

  FOLLY_ALWAYS_INLINE void release() noexcept { item_ = nullptr; }

  friend class CacheComponent;
};

/**
 * A writable handle for an item that has been allocated AND inserted.
 *
 * The user *must* mark the write handle as dirty if they write to it, otherwise
 * we'll skip flushing the write to the cache component.
 */
class WriteHandle : public Handle {
 public:
  WriteHandle(CacheComponent& cache, CacheItem& item) noexcept;
  ~WriteHandle() noexcept;

  // WriteHandle *is* move-constructible but *not* move-assignable. WriteHandle
  // `other` is no longer usable after the move.
  WriteHandle(WriteHandle&& other) noexcept;
  WriteHandle& operator=(WriteHandle&& other) noexcept = delete;

  FOLLY_ALWAYS_INLINE CacheItem* operator->() const noexcept { return item_; }
  FOLLY_ALWAYS_INLINE CacheItem& operator*() const noexcept { return *item_; }
  FOLLY_ALWAYS_INLINE CacheItem* get() const noexcept { return item_; }

  /**
   * Mark as dirty; on destruction we'll call into the cache to do the write.
   */
  FOLLY_ALWAYS_INLINE void markDirty(bool dirty = true) noexcept {
    dirty_ = dirty;
  }

 protected:
  // Whether the CacheItem needs to be written back
  bool dirty_{false};

  // Only used by AllocatedHandle
  WriteHandle(CacheComponent& cache, CacheItem& item, bool inserted) noexcept;
};

/**
 * A handle for an item that has been allocated but not yet inserted. Provides
 * the same APIs as WriteHandle.
 */
class AllocatedHandle : public WriteHandle {
 public:
  AllocatedHandle(CacheComponent& cache, CacheItem& item) noexcept;
};

/**
 * A read-only handle for an item that has been allocated AND inserted.
 */
class ReadHandle : public Handle {
 public:
  ReadHandle(CacheComponent& cache, CacheItem& item) noexcept;

  FOLLY_ALWAYS_INLINE const CacheItem* operator->() const noexcept {
    return item_;
  }
  FOLLY_ALWAYS_INLINE const CacheItem& operator*() const noexcept {
    return *item_;
  }
  FOLLY_ALWAYS_INLINE const CacheItem* get() const noexcept { return item_; }
};

} // namespace facebook::cachelib::interface
