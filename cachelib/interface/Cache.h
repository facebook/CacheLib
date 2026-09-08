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

#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Task.h>

#include <cstdint>
#include <memory>
#include <optional>

#include "cachelib/interface/CacheComponent.h"

namespace facebook::cachelib::interface {

/**
 * User-facing entry point to a composable cache.
 *
 * Cache owns the root of a graph built from cache components, selectors, and
 * connectors and exposes one handle-based API over that graph. Application
 * code interacts with Cache rather than the graph's individual pieces.
 *
 * Construct one long-lived Cache, typically a process singleton, and reuse it
 * throughout the process lifetime. Cache must outlive all outstanding
 * operations, generators, and handles.
 *
 * Key arguments are non-owning views. Their backing storage must remain alive
 * until the returned Task completes. For example:
 *
 * auto task = cache.find(std::string{"key"}); // temporary is destroyed here
 * auto result = co_await std::move(task); // unsafe: Key now dangles
 *
 * std::string key{"key"};
 * auto result = co_await cache.find(key); // safe: key remains alive
 */
class Cache {
 public:
  explicit Cache(std::unique_ptr<CacheComponent> component) noexcept;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;
  // Not movable: allocate(), exists(), find(), findToWrite(), and iterator()
  // are coroutines that capture `this` and don't read component_ until the
  // first co_await, so moving between creating one and awaiting it would leave
  // it dereferencing the moved-from Cache's empty component_.
  Cache(Cache&&) = delete;
  Cache& operator=(Cache&&) = delete;
  ~Cache() = default;

  /**
   * Allocate space for a new item.
   *
   * The item is not visible to lookups until it is inserted. Dropping the
   * returned handle before insertion discards the allocation.
   *
   * @param key cache item key
   * @param valueSize size of the item's value
   * @param creationTime when the item was created
   * @param ttlSecs time-to-live in seconds, after which the item is no longer
   * visible
   * @return an AllocatedHandle for writing the value, or an error
   */
  folly::coro::Task<Result<AllocatedHandle>> allocate(Key key,
                                                      uint32_t valueSize,
                                                      uint32_t creationTime,
                                                      uint32_t ttlSecs);

  /**
   * Insert an item returned by allocate().
   *
   * On success, the input handle is no longer usable. On failure, it may or
   * may not remain usable; test it with `if (handle)`.
   *
   * @param handle allocated item to insert
   * @return folly::unit on success, or an error
   */
  folly::coro::Task<UnitResult> insert(AllocatedHandle&& handle);

  /**
   * Insert an item, replacing any existing item with the same key.
   *
   * The input handle is no longer usable. Implementations that can return the
   * displaced item do so; others return std::nullopt.
   *
   * @param handle allocated item to insert
   * @return the displaced item when available, std::nullopt otherwise, or an
   * error
   */
  folly::coro::Task<Result<std::optional<AllocatedHandle>>> insertOrReplace(
      AllocatedHandle&& handle);

  /**
   * Check whether an item exists.
   *
   * This performs a full lookup and may update the cache's access policy. It
   * is not a cheap probe: flash-backed caches read and materialize the value.
   *
   * @param key cache item key
   * @return whether the item exists, or an error
   */
  folly::coro::Task<Result<bool>> exists(Key key);

  /**
   * Find an item for read-only access.
   *
   * @param key cache item key
   * @return a ReadHandle if found, std::nullopt if absent, or an error
   */
  folly::coro::Task<Result<std::optional<ReadHandle>>> find(Key key);

  /**
   * Find an item for mutable access.
   *
   * Call markDirty() on the returned handle after modifying its value;
   * otherwise the changes are not flushed.
   *
   * @param key cache item key
   * @return a WriteHandle if found, std::nullopt if absent, or an error
   */
  folly::coro::Task<Result<std::optional<WriteHandle>>> findToWrite(Key key);

  /**
   * Iterate over non-expired items in the cache.
   *
   * There are no consistency guarantees with concurrent inserts, updates, or
   * removals. Consume the returned generator as follows:
   *
   * auto items = cache.iterator();
   * while (auto item = co_await items.next()) {
   *   auto handle = std::move(*item);
   *   // use handle
   * }
   *
   * @return an async generator that yields ReadHandles
   */
  folly::coro::AsyncGenerator<ReadHandle> iterator();

  /**
   * Remove an item by key if it is present.
   *
   * @param key cache item key
   * @return whether an item was removed, or an error
   */
  folly::coro::Task<Result<bool>> remove(Key key);

  /**
   * Remove an item using a ReadHandle returned by this Cache.
   *
   * The input handle is no longer usable.
   *
   * @param handle item to remove
   * @return folly::unit on success, or an error
   */
  folly::coro::Task<UnitResult> remove(ReadHandle&& handle);

  /**
   * Persist configured state and stop all background workers.
   *
   * All outstanding operations, generators, and handles must be destroyed
   * before shutdown(). After shutdown() returns, Cache must not be used and may
   * be destroyed.
   *
   * @return folly::unit on success, or an error
   */
  UnitResult shutdown();

 private:
  std::unique_ptr<CacheComponent> component_;
};

} // namespace facebook::cachelib::interface
