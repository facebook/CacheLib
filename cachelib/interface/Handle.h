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

#include <concepts>
#include <cstdint>

#include "cachelib/interface/CacheItem.h"
#include "cachelib/interface/Result.h"

namespace facebook::cachelib::interface {

class CacheComponent;

// Tag type for constructing handles that store cache items inline
struct InlineItemTag {};
constexpr InlineItemTag InlineItem;

/**
 * Generic RAII handle referencing a cache item. If you have a handle, it
 * points to a valid cache item (unless it has been moved out of).
 *
 * Use tryCreateHandle() to create handles -- it increments the item's refcount
 * before constructing the handle. On destruction, the handle decrements the
 * refcount and releases the item if it reaches zero.
 *
 * Designed to work with any cache item, concrete implementations shouldn't need
 * to override handles.
 */
class Handle {
 public:
  // Size of the inline buffer for storing cache items without heap allocation
  static constexpr unsigned kInlineBufSize = 80;

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
   *
   * @param cache the cache component that owns the cache item
   * @param item the cache item
   * @param inserted whether the cache item has been inserted into cache
   */
  Handle(CacheComponent& cache, CacheItem& item, bool inserted) noexcept;

  /**
   * Construct a handle for an inline cache item. Sets item_ to point at buf_
   * so the caller only needs to placement-new the item into
   * CacheComponent::getInlineBuf().
   *
   * Note: the caller is responsible for incrementing the item's refcount after
   * allocating it in the inline buffer
   *
   * @param cache the cache component that owns the cache item
   * @param inserted whether the cache item has been inserted into cache
   */
  Handle(CacheComponent& cache, bool inserted, InlineItemTag) noexcept;

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
  // Note: not explicitly initializing so implementations that don't use it
  // don't pay the cost to zero it out
  alignas(8) uint8_t buf_[kInlineBufSize]; // NOLINT

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
  WriteHandle(CacheComponent& cache, InlineItemTag) noexcept;
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
  WriteHandle(CacheComponent& cache, bool inserted, InlineItemTag) noexcept;

 private:
  WriteHandle(CacheComponent& cache, CacheItem& item) noexcept;

  template <typename HandleT>
    requires std::derived_from<HandleT, Handle>
  friend Result<HandleT> tryCreateHandle(CacheComponent&, CacheItem&);
  template <typename HandleT>
    requires std::derived_from<HandleT, Handle>
  friend HandleT adoptHandle(CacheComponent&, CacheItem&);
};

/**
 * A handle for an item that has been allocated but not yet inserted. Provides
 * the same APIs as WriteHandle.
 */
class AllocatedHandle : public WriteHandle {
 public:
  AllocatedHandle(CacheComponent& cache, InlineItemTag) noexcept;

 private:
  AllocatedHandle(CacheComponent& cache, CacheItem& item) noexcept;

  template <typename HandleT>
    requires std::derived_from<HandleT, Handle>
  friend Result<HandleT> tryCreateHandle(CacheComponent&, CacheItem&);
  template <typename HandleT>
    requires std::derived_from<HandleT, Handle>
  friend HandleT adoptHandle(CacheComponent&, CacheItem&);
};

/**
 * A read-only handle for an item that has been allocated AND inserted.
 */
class ReadHandle : public Handle {
 public:
  ReadHandle(CacheComponent& cache, InlineItemTag) noexcept;

  FOLLY_ALWAYS_INLINE const CacheItem* operator->() const noexcept {
    return item_;
  }
  FOLLY_ALWAYS_INLINE const CacheItem& operator*() const noexcept {
    return *item_;
  }
  FOLLY_ALWAYS_INLINE const CacheItem* get() const noexcept { return item_; }

 private:
  ReadHandle(CacheComponent& cache, CacheItem& item) noexcept;

  template <typename HandleT>
    requires std::derived_from<HandleT, Handle>
  friend Result<HandleT> tryCreateHandle(CacheComponent&, CacheItem&);
  template <typename HandleT>
    requires std::derived_from<HandleT, Handle>
  friend HandleT adoptHandle(CacheComponent&, CacheItem&);
};

/**
 * Create a handle by atomically incrementing the item's refcount. Returns an
 * error if the refcount could not be incremented (e.g., item is being evicted).
 */
template <typename HandleT>
  requires std::derived_from<HandleT, Handle>
Result<HandleT> tryCreateHandle(CacheComponent& cache, CacheItem& item) {
  auto result = item.incrementRefCount(cache);
  if (result.hasError()) {
    return folly::makeUnexpected(std::move(result).error());
  }
  return HandleT(cache, item);
}

/**
 * Create a handle that adopts an existing refcount -- does NOT increment the
 * item's refcount. The caller must have already incremented it (e.g., via an
 * implementation-specific handle) and is transferring ownership to the returned
 * handle.
 */
template <typename HandleT>
  requires std::derived_from<HandleT, Handle>
HandleT adoptHandle(CacheComponent& cache, CacheItem& item) {
  return HandleT(cache, item);
}

} // namespace facebook::cachelib::interface
