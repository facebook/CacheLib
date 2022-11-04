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

#include <folly/Range.h>

namespace facebook {
namespace cachelib {

// The Cache API wrapper for NvmCache to access private methods in
// 'CacheAllocator.h'.
template <typename C>
class CacheAPIWrapperForNvm {
  using Item = typename C::Item;
  using ChainedItemIter = typename C::ChainedItemIter;
  using Key = typename Item::Key;
  using WriteHandle = typename C::WriteHandle;
  using ReadHandle = typename C::ReadHandle;
  using EventTracker = typename C::EventTracker;

 public:
  // Get chained allocation on the item.
  // The order of iteration will be LIFO of the addChainedItem calls.
  //
  // @param cache   the cache instance using nvmcache
  // @param parent  the item to get chained allocations
  // @return iterator to the item's chained allocations
  static folly::Range<ChainedItemIter> viewAsChainedAllocsRange(
      C& cache, const Item& parent) {
    return cache.viewAsChainedAllocsRange(parent);
  }

  // Grab a refcounted handle to the item.
  //
  // @param  cache the cache instance using nvmcache
  // @param  key   the key to look up in the access container
  // @return handle if item is found, nullptr otherwise
  // @throw  std::overflow_error is the maximum item refcount is execeeded by
  //         creating this item handle.
  static WriteHandle findInternal(C& cache, Key key) {
    return cache.findInternal(key);
  }

  // Create a new cache allocation.
  //
  // @param cache           the cache instance using nvmcache
  // @param id              the pool id for the allocation that was previously
  //                        created through addPool
  // @param key             the key for the allocation. This will be made a
  //                        part of the Item and be available through getKey().
  // @param size            the size of the allocation, exclusive of the key
  //                        size.
  // @param creationTime    Timestamp when this item was created
  // @param expiryTime      set an expiry timestamp for the item
  //                        (0 means no expiration time).
  // @return      the handle for the item or an invalid handle(nullptr) if the
  //              allocation failed. Allocation can fail if one such
  //              allocation already exists or if we are out of memory and
  //              can not find an eviction. Handle must be destroyed *before*
  //              the instance of the CacheAllocator gets destroyed
  // @throw   std::invalid_argument if the poolId is invalid or the size
  //          requested is invalid or if the key is invalid(key.size() == 0 or
  //          key.size() > 255)
  static WriteHandle allocateInternal(C& cache,
                                      PoolId id,
                                      Key key,
                                      uint32_t size,
                                      uint32_t creationTime,
                                      uint32_t expiryTime) {
    return cache.allocateInternal(id, key, size, creationTime, expiryTime);
  }

  // Insert the allocated handle into the AccessContainer from nvmcache, making
  // it accessible for everyone. This needs to be the handle that the caller
  // allocated through _allocate_. If this call fails, the allocation will be
  // freed back when the handle gets out of scope in the caller.
  //
  // @param cache  the cache instance using nvmcache
  // @param handle the handle for the allocation.
  // @return true if the handle was successfully inserted into the hashtable
  //         and is now accessible to everyone. False if there was an error.
  // @throw  std::invalid_argument if the handle is already accessible or
  //         invalid
  static bool insertFromNvm(C& cache, const WriteHandle& handle) {
    return cache.insertImpl(handle, AllocatorApiEvent::INSERT_FROM_NVM);
  }

  // Acquire the wait context for the handle. This is used by nvmcache to
  // maintain a list of waiters.
  //
  // @param cache  the cache instance using nvmcache
  // @param handle the handle to acquire the wait context
  // @return the wait context for the handle
  static std::shared_ptr<WaitContext<ReadHandle>> getWaitContext(
      C& cache, ReadHandle& handle) {
    return cache.getWaitContext(handle);
  }

  // Create an item handle with wait context.
  //
  // @param cache the cache instance using nvmcache
  // @return the created item handle
  static WriteHandle createNvmCacheFillHandle(C& cache) {
    return cache.createNvmCacheFillHandle();
  }

  // Get the thread local version of the Stats.
  //
  // @param cache the cache instance using nvmcache
  // @return Stats of the nvmcache
  static detail::Stats& getStats(C& cache) { return cache.stats(); }

  static EventTracker* getEventTracker(C& cache) {
    return cache.getEventTracker();
  }
};

} // namespace cachelib
} // namespace facebook
