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
  using ItemHandle = typename C::ItemHandle;

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
  static ItemHandle findInternal(C& cache, Key key) {
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
  // @param unevictable     optional argument to make an item unevictable
  //                        unevictable item may prevent the slab it belongs to
  //                        from being released if it cannot be moved
  //                        (0 means no expiration time).
  // @return      the handle for the item or an invalid handle(nullptr) if the
  //              allocation failed. Allocation can fail if one such
  //              allocation already exists or if we are out of memory and
  //              can not find an eviction. Handle must be destroyed *before*
  //              the instance of the CacheAllocator gets destroyed
  // @throw   std::invalid_argument if the poolId is invalid or the size
  //          requested is invalid or if the key is invalid(key.size() == 0 or
  //          key.size() > 255)
  static ItemHandle allocateInternal(C& cache,
                                     PoolId id,
                                     Key key,
                                     uint32_t size,
                                     uint32_t creationTime,
                                     uint32_t expiryTime,
                                     bool unevictable) {
    return cache.allocateInternal(
        id, key, size, creationTime, expiryTime, unevictable);
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
  static bool insertFromNvm(C& cache, const ItemHandle& handle) {
    return cache.insertImpl(handle, AllocatorApiEvent::INSERT_FROM_NVM);
  }

  // Acquire the wait context for the handle. This is used by nvmcache to
  // maintain a list of waiters.
  //
  // @param cache  the cache instance using nvmcache
  // @param handle the handle to acquire the wait context
  // @return the wait context for the handle
  static std::shared_ptr<WaitContext<ItemHandle>> getWaitContext(
      C& cache, ItemHandle& handle) {
    return cache.getWaitContext(handle);
  }

  // Create an item handle with wait context.
  //
  // @param cache the cache instance using nvmcache
  // @return the created item handle
  static ItemHandle createNvmCacheFillHandle(C& cache) {
    return cache.createNvmCacheFillHandle();
  }

  // Get the thread local version of the Stats.
  //
  // @param cache the cache instance using nvmcache
  // @return Stats of the nvmcache
  static detail::Stats& getStats(C& cache) { return cache.stats(); }
};

} // namespace cachelib
} // namespace facebook
