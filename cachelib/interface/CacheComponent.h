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

#include <folly/coro/Task.h>

#include "cachelib/interface/CacheItem.h"
#include "cachelib/interface/Handle.h"
#include "cachelib/interface/Result.h"

namespace facebook::cachelib::interface {

class CacheComponent {
 public:
  virtual ~CacheComponent() = default;

  // ------------------------------ Interface ------------------------------ //

  /**
   * Get the name of the component. Should not change throughout the lifetime of
   * the cache component since it will be used to identify items during dynamic
   * reconfiguration.
   *
   * @return the name of the component
   */
  virtual const std::string& getName() const noexcept = 0;

  /**
   * Allocate space for a new item. Returns an AllocatedHandle that can be used
   * to access the allocated memory.
   *
   * NOTE: the item *must* be inserted before it is visible for lookups.
   * Allocated items that are not inserted into cache will be freed when the
   * returned AllocatedHandle goes out of scope.  In other words if you've got
   * an AllocatedHandle, it's not yet in cache!
   *
   * @param key cache item key
   * @param size size of the value of the item
   * @param creationTime when the item was created
   * @param ttlSecs time-to-live of the item in seconds. After the item has been
   * in cache for this long, it will no longer be visible.
   * @return an AllocatedHandle suitable for writing to cache memory if space
   * was allocated or an error result otherwise
   */
  virtual folly::coro::Task<Result<AllocatedHandle>> allocate(
      Key key, uint32_t size, uint32_t creationTime, uint32_t ttlSecs) = 0;

  /**
   * Insert an item into cache using an AllocatedHandle returned by allocate().
   * If inserted, the AllocatedHandle is no longer usable (moved out).  If not
   * inserted, the handle *may or may not* be usable; the behavior is
   * implementation-defined.  You can check by using `if (handle)`.
   *
   * @param handle AllocatedHandle returned by allocate()
   * @return folly::unit or an error result otherwise
   */
  virtual folly::coro::Task<UnitResult> insert(AllocatedHandle&& handle) = 0;

  /**
   * Same as above, but replaces the item if it was already inserted into cache.
   * Returns an AllocatedHandle to the old item if it was replaced. The input
   * AllocatedHandle is no longer usable (moved out).
   *
   * NOTE: it may be too expensive for some implementations to return the old
   * item; they'll just return std::nullopt.
   *
   * @param handle AllocatedHandle returned by allocate()
   * @return an empty optional if the item was inserted, an AllocatedHandle to
   * the replaced item if it was replaced (no longer findable replaced) or an
   * error result otherwise
   */
  virtual folly::coro::Task<Result<std::optional<AllocatedHandle>>>
  insertOrReplace(AllocatedHandle&& handle) = 0;

  /**
   * Find an item in cache. Returns a handle if found, std::nullopt otherwise.
   * find() is for read-only access, findToWrite() is for write access.
   *
   * NOTE: if you write to the handle returned by findToWrite(), you must *must*
   * mark the handle as dirty in order for the cache component to flush the
   * write to the underlying storage.
   *
   * @param key cache item key
   * @return a handle if found, std::nullopt if not found or an error result
   * otherwise
   */
  virtual folly::coro::Task<Result<std::optional<ReadHandle>>> find(
      Key key) = 0;
  virtual folly::coro::Task<Result<std::optional<WriteHandle>>> findToWrite(
      Key key) = 0;

  /**
   * Remove an item from cache if it is present.
   * @param key cache item key
   * @return a bool indicating whether the item was removed or an error result
   * otherwise
   */
  virtual folly::coro::Task<Result<bool>> remove(Key key) = 0;

  /**
   * Remove an item from cache. The ReadHandle is no longer usable (moved out).
   * @param handle cache item handle
   * @return folly::unit if the item was removed or an error result otherwise
   */
  virtual folly::coro::Task<UnitResult> remove(ReadHandle&& handle) = 0;

 protected:
  /**
   * Mark an item as inserted into cache.
   * @param handle handle to the cache item
   * @param inserted whether the item was inserted into cache
   */
  FOLLY_ALWAYS_INLINE void setInserted(Handle& handle, bool inserted) {
    handle.inserted_ = inserted;
  }

  /**
   * Release a handle (make unusable) without adjusting refcounts. Useful when
   * CacheComponent takes an rvalue ref to a handle that needs to be destroyed.
   *
   * @param handle handle to release
   */
  FOLLY_ALWAYS_INLINE void releaseHandle(Handle&& handle) { handle.release(); }

 private:
  // ------------------------------ Interface ------------------------------ //

  /**
   * Write a dirty cache item back to the cache.
   *
   * Called by WriteHandle destructor when the handle is marked dirty.
   * Implementations should flush any buffered writes to the underlying storage.
   *
   * @param item cache item to write back
   * @return folly::unit on success or an error result otherwise
   */
  virtual UnitResult writeBack(CacheItem& item) = 0;

  /**
   * Release the item from the cache. Frees the associated allocation and
   * executes any necessary callbacks. Only meant to be called by the component
   * or cache item handles.
   *
   * @param item the item to release
   * @param inserted whether the item was previously inserted into the cache
   * (callbacks are typically only called when it was inserted)
   */
  virtual folly::coro::Task<void> release(CacheItem& item, bool inserted) = 0;

  friend class Handle;
  friend class WriteHandle;
};

} // namespace facebook::cachelib::interface
