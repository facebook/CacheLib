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

#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/SharedLock.h>
#include <folly/coro/SharedMutex.h>
#include <folly/coro/Task.h>
#include <folly/coro/Traits.h>

#include <mutex>
#include <vector>

#include "cachelib/common/Hash.h"
#include "cachelib/interface/CacheItem.h"

namespace facebook::cachelib::interface::utils {

/**
 * Utility to serialize operations on keys.
 *
 * In order to guarantee linearizability, the cache needs to order reads and
 * writes to cache items.  This can be done by serializing operations on an
 * item.  Rather than creating an explicit queue of operations to execute on a
 * cache item, just queue up a list of coroutines. ShardedSerializer
 * accomplishes this via folly::coro::SharedMutex, which provides reader/writer
 * locks with with FIFO ordering.  This allows reads to execute concurrently,
 * but serializes reads and writes.  We explicitly avoid using SharedMutex's
 * upgrade operations as they don't respect FIFO ordering.
 *
 * NOTE: do not use upgrade APIs as this will violate strong consistency!
 *
 * Ordering operations per cache item key requires too many locks, so shard the
 * cache items and enforce an ordering per shard.  Shard granularity is
 * controlled by the shardsPower knob -- ShardedSerializer will create (1 <<
 * shardsPower) shards.
 *
 * It is up to the user to figure out when you need a read vs. write lock.
 */
class ShardedSerializer {
 private:
  /**
   * detect coroutines while filtering out folly::Expected, which satisfies
   * folly::coro::is_semi_awaitable (but is not co_awaitable).
   */
  template <typename Type>
  struct IsExpected : std::false_type {};
  template <typename Value, typename Error>
  struct IsExpected<folly::Expected<Value, Error>> : std::true_type {};

  template <typename Type>
  static constexpr bool IsCoro =
      folly::coro::is_semi_awaitable_v<Type> && !IsExpected<Type>::value;

  /* wrap type in a Task (if not already a Task) */
  template <typename Type>
  using TaskWrapper =
      std::conditional_t<IsCoro<Type>, Type, folly::coro::Task<Type>>;

 public:
  using Mutex = folly::coro::SharedMutex;
  using ReadLock = folly::coro::SharedLock<Mutex>;
  using WriteLock = std::unique_lock<Mutex>;

  // Allows up to 1M shards
  static constexpr uint8_t kMaxShardsPower = 20;

  /**
   * Default constructor.
   * @param hasher      hasher to use for sharding
   * @param shardsPower number of shards to use (2^shardsPower), must be <= 20
   */
  ShardedSerializer(std::unique_ptr<Hash> hasher, uint8_t shardsPower)
      : hasher_(std::move(hasher)), mutexes_(1ULL << shardsPower) {
    if (shardsPower == 0) {
      throw std::invalid_argument("shardsPower must be > 0");
    } else if (shardsPower > kMaxShardsPower) {
      throw std::invalid_argument(
          fmt::format("shardsPower must be <= {}", kMaxShardsPower));
    } else if (hasher_ == nullptr) {
      throw std::invalid_argument("hasher cannot be null");
    }
  }

  /**
   * Acquire a read (shared) lock on the mutex associated with a key. The
   * returned ReadLock automatically releases the lock when destroyed.
   *
   * @param key key whose lock to acquire
   * @return    an RAII ReadLock holding a shared lock
   */
  folly::coro::Task<ReadLock> rlock(Key key) const {
    co_return co_await folly::coro::detachOnCancel(
        getMutex(key).co_scoped_lock_shared());
  }

  /**
   * Acquire a write (exclusive) lock on the mutex associated with a key. The
   * returned WriteLock automatically releases the lock when destroyed.
   *
   * @param key key whose lock to acquire
   * @return    an RAII WriteLock holding an exclusive lock
   */
  folly::coro::Task<WriteLock> wlock(Key key) {
    co_return co_await folly::coro::detachOnCancel(
        getMutex(key).co_scoped_lock());
  }

  /**
   * Run a function/coroutine while holding a read (shared) lock on the mutex
   * associated with a key.
   *
   * @param key  key whose lock to acquire
   * @param func function/coroutine to run while holding the lock
   * @return     result of the function/coroutine
   */
  template <typename FuncT, typename ReturnT = std::invoke_result_t<FuncT>>
  TaskWrapper<ReturnT> withRlock(Key key, FuncT&& func) const {
    auto lock = co_await rlock(key);
    if constexpr (IsCoro<ReturnT>) {
      co_return co_await func();
    } else {
      co_return func();
    }
  }

  /**
   * Run a function/coroutine while holding a write (exclusive) lock on the
   * mutex associated with a key.
   *
   * @param key  key whose lock to acquire
   * @param func function/coroutine to run while holding the lock
   * @return     result of the function/coroutine
   */
  template <typename FuncT, typename ReturnT = std::invoke_result_t<FuncT>>
  TaskWrapper<ReturnT> withWlock(Key key, FuncT&& func) {
    auto lock = co_await wlock(key);
    if constexpr (IsCoro<ReturnT>) {
      co_return co_await func();
    } else {
      co_return func();
    }
  }

 private:
  std::unique_ptr<Hash> hasher_;
  mutable std::vector<Mutex> mutexes_;

  Mutex& getMutex(Key key) const {
    return mutexes_[(*hasher_)(key.data(), key.size()) % mutexes_.size()];
  }
};

} // namespace facebook::cachelib::interface::utils
