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
#include <folly/Likely.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>
#include <folly/synchronization/SanitizeThread.h>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#include <folly/Range.h>
#pragma GCC diagnostic pop

#include "cachelib/allocator/CCacheManager.h"
#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/CacheChainedItemIterator.h"
#include "cachelib/allocator/CacheItem.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/CacheStatsInternal.h"
#include "cachelib/allocator/CacheTraits.h"
#include "cachelib/allocator/CacheVersion.h"
#include "cachelib/allocator/ChainedAllocs.h"
#include "cachelib/allocator/ICompactCache.h"
#include "cachelib/allocator/KAllocation.h"
#include "cachelib/allocator/MemoryMonitor.h"
#include "cachelib/allocator/NvmAdmissionPolicy.h"
#include "cachelib/allocator/NvmCacheState.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "cachelib/allocator/PoolOptimizer.h"
#include "cachelib/allocator/PoolRebalancer.h"
#include "cachelib/allocator/PoolResizer.h"
#include "cachelib/allocator/ReadOnlySharedCacheView.h"
#include "cachelib/allocator/Reaper.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/Refcount.h"
#include "cachelib/allocator/TempShmMapping.h"
#include "cachelib/allocator/TlsActiveItemRing.h"
#include "cachelib/allocator/TypedHandle.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/allocator/memory/MemoryAllocatorStats.h"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#include "cachelib/allocator/nvmcache/NvmCache.h"
#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Mutex.h"
#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/common/Throttler.h"
#include "cachelib/common/Time.h"
#include "cachelib/common/Utils.h"
#include "cachelib/shm/ShmManager.h"

namespace facebook {
namespace cachelib {

template <typename AllocatorT>
class FbInternalRuntimeUpdateWrapper;

template <typename K, typename V, typename C>
class ReadOnlyMap;

namespace objcache2 {
template <typename AllocatorT>
class ObjectCache;

template <typename AllocatorT>
class ObjectCacheBase;
} // namespace objcache2

namespace cachebench {
template <typename Allocator>
class Cache;
namespace tests {
class CacheTest;
}
} // namespace cachebench

namespace tests {
template <typename AllocatorT>
class BaseAllocatorTest;

template <typename AllocatorT>
class AllocatorHitStatsTest;

template <typename AllocatorT>
class AllocatorResizeTest;

template <typename AllocatorT>
class FixedSizeArrayTest;

template <typename AllocatorT>
class MapTest;

class NvmCacheTest;

template <typename AllocatorT>
class PoolOptimizeStrategyTest;

class NvmAdmissionPolicyTest;

class CacheAllocatorTestWrapper;
class PersistenceCache;
} // namespace tests

namespace objcache {
template <typename CacheDescriptor, typename AllocatorRes>
class ObjectCache;
namespace test {
#define GET_CLASS_NAME(test_case_name, test_name) \
  test_case_name##_##test_name##_Test

#define GET_DECORATED_CLASS_NAME(namespace, test_case_name, test_name) \
  namespace ::GET_CLASS_NAME(test_case_name, test_name)

class GET_CLASS_NAME(ObjectCache, ObjectHandleInvalid);
} // namespace test
} // namespace objcache

// CacheAllocator can provide an interface to make Keyed Allocations(Item) and
// takes two templated types that control how the allocation is
// maintained(MMType aka MemoryManagementType) and accessed(AccessType). The
// cache allocator internally has an allocator that it interacts with to make
// allocations. All active allocations are put into the AccessContainer and
// the MMContainer for maintenance. When the cache is full, allocations are
// garbage collected by the implementation of MMType.
//
// The MMType is used for keeping track of allocations that are currently
// under the control of cache allocator. The MMType is required to provide a
// data structure MMContainer with a well defined interface. For example,
// check MMLru.h's Container (TODO use boost concepts to enforce the
// interface if possible and have it defined in a .h file). The MMType must
// provide a Hook type that will be used to instrument the resultant Item to
// be compatible for use with the MMContainer similar to a boost intrusive
// member hook. MMType::Hook must be sufficient for MMType::Container<T> to
// operate. The MMContainer is expected to implement interfaces to
// add/remove/evict/recordAccess a T& object into the container. This allows
// us to change/abstract away the memory management implementation of the
// cache from the other parts of the cache.
//
// Similar to the MMType, the AccessType is an intrusive data type that
// provides a container to access the keyed allocations. AccessType must
// provide an AccessType::Hook and AccessType::Container with
// find/insert/remove interface similar to a hash table.
//
template <typename CacheTrait>
class CacheAllocator : public CacheBase {
 public:
  using CacheT = CacheAllocator<CacheTrait>;
  using MMType = typename CacheTrait::MMType;
  using AccessType = typename CacheTrait::AccessType;
  using Config = CacheAllocatorConfig<CacheT>;

  // configs for the MMtype and AccessType.
  using MMConfig = typename MMType::Config;
  using AccessConfig = typename AccessType::Config;

  using Item = CacheItem<CacheTrait>;
  using ChainedItem = typename Item::ChainedItem;

  // the holder for the item when we hand it to the caller. This ensures
  // that the reference count is maintained when the caller is done with the
  // item. The ReadHandle/WriteHandle provides a getMemory() and getKey()
  // interface. The caller is free to use the result of these two as long as the
  // handle is active/alive. Using the result of the above interfaces after
  // destroying the ReadHandle/WriteHandle is UB. The ReadHandle/WriteHandle
  // safely wraps a pointer to the "const Item"/"Item".
  using ReadHandle = typename Item::ReadHandle;
  using WriteHandle = typename Item::WriteHandle;
  // Following is deprecated as of allocator version 17 and this line will be
  // removed at a future date
  // using ItemHandle = WriteHandle;
  template <typename UserType,
            typename Converter =
                detail::DefaultUserTypeConverter<Item, UserType>>
  using TypedHandle = TypedHandleImpl<Item, UserType, Converter>;

  // TODO (sathya) some types take CacheT and some take CacheTrait. need to
  // clean this up and come up with a consistent policy that is intuitive.
  using ChainedItemIter = CacheChainedItemIterator<CacheT, const Item>;
  using WritableChainedItemIter = CacheChainedItemIterator<CacheT, Item>;
  using ChainedAllocs = CacheChainedAllocs<CacheT, ReadHandle, ChainedItemIter>;
  using WritableChainedAllocs =
      CacheChainedAllocs<CacheT, WriteHandle, WritableChainedItemIter>;

  using Key = typename Item::Key;
  using PoolIds = std::set<PoolId>;

  using EventTracker = EventInterface<Key>;

  // SampleItem is a wrapper for the CacheItem which is provided as the sample
  // for uploading to Scuba (see ItemStatsExporter). It is guaranteed that the
  // CacheItem is accessible as long as the SampleItem is around since the
  // internal resource (e.g., ref counts, buffer) will be managed by the iobuf
  class SampleItem {
   public:
    explicit SampleItem(bool fromNvm) : fromNvm_{fromNvm} {}

    SampleItem(folly::IOBuf&& iobuf, const AllocInfo& allocInfo, bool fromNvm)
        : iobuf_{std::move(iobuf)}, allocInfo_{allocInfo}, fromNvm_{fromNvm} {}

    SampleItem(folly::IOBuf&& iobuf,
               PoolId poolId,
               ClassId classId,
               size_t allocSize,
               bool fromNvm)
        : SampleItem(std::move(iobuf),
                     AllocInfo{poolId, classId, allocSize},
                     fromNvm) {}

    const Item* operator->() const noexcept { return get(); }

    const Item& operator*() const noexcept { return *get(); }

    [[nodiscard]] const Item* get() const noexcept {
      return reinterpret_cast<const Item*>(iobuf_.data());
    }

    [[nodiscard]] bool isValid() const { return !iobuf_.empty(); }

    [[nodiscard]] bool isNvmItem() const { return fromNvm_; }

    [[nodiscard]] const AllocInfo& getAllocInfo() const { return allocInfo_; }

   private:
    folly::IOBuf iobuf_;
    AllocInfo allocInfo_{};
    bool fromNvm_ = false;
  };

  // holds information about removal, used in RemoveCb
  struct RemoveCbData {
    // remove or eviction
    RemoveContext context;

    // item about to be freed back to allocator
    Item& item;

    // Iterator range pointing to chained allocs associated with @item
    folly::Range<ChainedItemIter> chainedAllocs;
  };

  struct DestructorData {
    DestructorData(DestructorContext ctx,
                   Item& it,
                   folly::Range<ChainedItemIter> iter,
                   PoolId id)
        : context(ctx), item(it), chainedAllocs(iter), pool(id) {}

    // helps to convert RemoveContext to DestructorContext,
    // the context for RemoveCB is re-used to create DestructorData,
    // this can be removed if RemoveCB is dropped.
    DestructorData(RemoveContext ctx,
                   Item& it,
                   folly::Range<ChainedItemIter> iter,
                   PoolId id)
        : item(it), chainedAllocs(iter), pool(id) {
      if (ctx == RemoveContext::kEviction) {
        context = DestructorContext::kEvictedFromRAM;
      } else {
        context = DestructorContext::kRemovedFromRAM;
      }
    }

    // remove or eviction
    DestructorContext context;

    // item about to be freed back to allocator
    // when the item is evicted/removed from NVM, the item is created on the
    // heap, functions (e.g. CacheAllocator::getAllocInfo) that assumes item is
    // located in cache slab doesn't work in such case.
    // chained items must be iterated though @chainedAllocs.
    // Other APIs used to access chained items are not compatible and should not
    // be used.
    Item& item;

    // Iterator range pointing to chained allocs associated with @item
    // when chained items are evicted/removed from NVM, items are created on the
    // heap, functions (e.g. CacheAllocator::getAllocInfo) that assumes items
    // are located in cache slab doesn't work in such case.
    folly::Range<ChainedItemIter> chainedAllocs;

    // the pool that this item is/was
    PoolId pool;
  };

  // call back to execute when moving an item, this could be a simple memcpy
  // or something more complex.
  // An optional parentItem pointer is provided if the item being moved is a
  // chained item.
  using MoveCb =
      std::function<void(Item& oldItem, Item& newItem, Item* parentItem)>;

  // call back type that is executed when the cache item is removed
  // (evicted / freed) from RAM, only items inserted into cache (not nascent)
  // successfully are tracked
  using RemoveCb = std::function<void(const RemoveCbData& data)>;

  // the destructor being executed when the item is removed from cache (both RAM
  // and NVM), only items inserted into cache (not nascent) successfully are
  // tracked.
  using ItemDestructor = std::function<void(const DestructorData& data)>;

  using NvmCacheT = NvmCache<CacheT>;
  using NvmCacheConfig = typename NvmCacheT::Config;
  using DeleteTombStoneGuard = typename NvmCacheT::DeleteTombStoneGuard;

  // Interface for the sync object provided by the user if movingSync is turned
  // on.
  // SyncObj is for CacheLib to obtain exclusive access to an item when
  // it is moving it during slab release. Once held, the user should guarantee
  // the item will not be accessed from another thread.
  struct SyncObj {
    virtual ~SyncObj() = default;

    // Override this function to indicate success/failure of the sync obj,
    // if user-supplied SyncObj can fail. e.g. if a lock can timeout.
    virtual bool isValid() const { return true; }
  };
  using ChainedItemMovingSync = std::function<std::unique_ptr<SyncObj>(Key)>;

  using AccessContainer = typename Item::AccessContainer;
  using MMContainer = typename Item::MMContainer;

  // serialization types
  using MMSerializationType = typename MMType::SerializationType;
  using MMSerializationConfigType = typename MMType::SerializationConfigType;
  using MMSerializationTypeContainer =
      typename MMType::SerializationTypeContainer;
  using AccessSerializationType = typename AccessType::SerializationType;

  using ShmManager = facebook::cachelib::ShmManager;

  // The shared memory segments that can be persisted and re-attached to
  enum SharedMemNewT { SharedMemNew };
  // Attach to a persisted shared memory segment
  enum SharedMemAttachT { SharedMemAttach };

  // instantiates a cache allocator on heap memory
  //
  // @param config        the configuration for the whole cache allocator
  explicit CacheAllocator(Config config);

  // instantiates a cache allocator on shared memory
  //
  // @param config        the configuration for the whole cache allocator
  CacheAllocator(SharedMemNewT, Config config);

  // restore a cache allocator from shared memory
  //
  // @param config        the configuration for the whole cache allocator
  //
  // @throw std::invalid_argument if cannot restore successful
  CacheAllocator(SharedMemAttachT, Config config);

  // Shared segments will be detached upon destruction
  ~CacheAllocator() override;

  // create a new cache allocation. The allocation can be initialized
  // appropriately and made accessible through insert or insertOrReplace.
  // If the handle returned from this api is not passed on to
  // insert/insertOrReplace, the allocation gets destroyed when the handle
  // goes out of scope.
  //
  // @param id              the pool id for the allocation that was previously
  //                        created through addPool
  // @param key             the key for the allocation. This will be made a
  //                        part of the Item and be available through getKey().
  // @param size            the size of the allocation, exclusive of the key
  //                        size.
  // @param ttlSecs         Time To Live(second) for the item,
  //                        default with 0 means no expiration time.
  //
  // @return      the handle for the item or an invalid handle(nullptr) if the
  //              allocation failed. Allocation can fail if we are out of memory
  //              and can not find an eviction.
  // @throw   std::invalid_argument if the poolId is invalid or the size
  //          requested is invalid or if the key is invalid(key.size() == 0 or
  //          key.size() > 255)
  WriteHandle allocate(PoolId id,
                       Key key,
                       uint32_t size,
                       uint32_t ttlSecs = 0,
                       uint32_t creationTime = 0);

  // Allocate a chained item
  //
  // The resulting chained item does not have a parent item and
  // will be freed once the handle is dropped
  //
  // The parent handle parameter here is mainly used to find the
  // correct pool to allocate memory for this chained item
  //
  // @param parent    handle to the cache item
  // @param size      the size for the chained allocation
  //
  // @return    handle to the chained allocation
  // @throw     std::invalid_argument if the size requested is invalid or
  //            if the item is invalid
  WriteHandle allocateChainedItem(const ReadHandle& parent, uint32_t size);

  // Link a chained item to a parent item and mark this parent handle as having
  // chained allocations.
  // The parent handle is not reset (to become a null handle) so that the caller
  // can continue using it as before calling this api.
  //
  // @param parent  handle to the parent item
  // @param child   chained item that will be linked to the parent
  //
  // @throw std::invalid_argument if parent is nullptr
  void addChainedItem(WriteHandle& parent, WriteHandle child);

  // Pop the first chained item assocaited with this parent and unmark this
  // parent handle as having chained allocations.
  // The parent handle is not reset (to become a null handle) so that the caller
  // can continue using it as before calling this api.
  //
  // @param parent  handle to the parent item
  //
  // @return ChainedItem  head if there exists one
  //         nullptr      otherwise
  WriteHandle popChainedItem(WriteHandle& parent);

  // Return the key to the parent item.
  //
  // This API is racy with transferChainedAndReplace and also with moving during
  // a slab release. To use this safely. user needs to synchronize calls to this
  // API using their user level lock (in exclusive mode). The same user level
  // lock should've been provided via movingSync to CacheLib if moving is
  // enabled for slab rebalancing.
  //
  // @throw std::invalid_argument if chainedItem is not actually a chained item.
  Key getParentKey(const Item& chainedItem);

  // replace a chained item in the existing chain. old and new item must be
  // chained items that have been allocated with the same parent that is
  // passed in. oldItem must be in the chain and newItem must not be.
  //
  // Upon success a handle to the oldItem is returned for the caller
  //
  // @param oldItem  the item we are replacing in the chain
  // @param newItem  the item we are replacing it with
  // @param parent   the parent for the chain
  //
  // @return  handle to the oldItem on return.
  //
  // @throw std::invalid_argument if any of the pre-conditions fails
  WriteHandle replaceChainedItem(Item& oldItem,
                                 WriteHandle newItem,
                                 Item& parent);

  // Transfers the ownership of the chain from the current parent to the new
  // parent and inserts the new parent into the cache. Parent will be unmarked
  // as having chained allocations and its nvmCache will be invalidated. Parent
  // will not be null after calling this API.
  //
  // Caller must synchronize with any modifications to the parent's chain and
  // any calls to find() for the same key to ensure there are no more concurrent
  // parent handle while doing this. While calling this method, the cache does
  // not guarantee a consistent view for the key and the caller must not rely on
  // this. The new parent and old parent must be allocations for the same key.
  // New parent must also be an allocation that is not added to the cache.
  //
  //
  // @param parent    the current parent of the chain we want to transfer
  // @param newParent the new parent for the chain
  //
  // @throw   std::invalid_argument if the parent does not have chained item or
  //          incorrect state of chained item or if any of the pre-conditions
  //          are not met
  void transferChainAndReplace(WriteHandle& parent, WriteHandle& newParent);

  // Inserts the allocated handle into the AccessContainer, making it
  // accessible for everyone. This needs to be the handle that the caller
  // allocated through _allocate_. If this call fails, the allocation will be
  // freed back when the handle gets out of scope in the caller.
  //
  // @param  handle  the handle for the allocation.
  //
  // @return true if the handle was successfully inserted into the hashtable
  //         and is now accessible to everyone. False if there was an error.
  //
  // @throw std::invalid_argument if the handle is already accessible.
  bool insert(const WriteHandle& handle);

  // Replaces the allocated handle into the AccessContainer, making it
  // accessible for everyone. If an existing handle is already in the
  // container, remove that handle. This needs to be the handle that the caller
  // allocated through _allocate_. If this call fails, the allocation will be
  // freed back when the handle gets out of scope in the caller.
  //
  // @param  handle  the handle for the allocation.
  //
  // @throw std::invalid_argument if the handle is already accessible.
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @return handle to the old item that had been replaced
  WriteHandle insertOrReplace(const WriteHandle& handle);

  // look up an item by its key across the nvm cache as well if enabled.
  //
  // @param key       the key for lookup
  //
  // @return          the read handle for the item or a handle to nullptr if the
  //                  key does not exist.
  ReadHandle find(Key key);

  // Warning: this API is synchronous today with HybridCache. This means as
  //          opposed to find(), we will block on an item being read from
  //          flash until it is loaded into DRAM-cache. In find(), if an item
  //          is missing in dram, we will return a "not-ready" handle and
  //          user can choose to block or convert to folly::SemiFuture and
  //          process the item only when it becomes ready (loaded into DRAM).
  //          If blocking behavior is NOT what you want, a workaround is:
  //            auto readHandle = cache->find("my key");
  //            if (!readHandle.isReady()) {
  //              auto sf = std::move(readHandle)
  //                .toSemiFuture()
  //                .defer([] (auto readHandle)) {
  //                  return std::move(readHandle).toWriteHandle();
  //                }
  //            }
  //
  // look up an item by its key across the nvm cache as well if enabled. Users
  // should call this API only when they are going to mutate the item data.
  //
  // @param key               the key for lookup
  // @param isNvmInvalidate   whether to do nvm invalidation;
  //                          defaults to be true
  //
  // @return      the write handle for the item or a handle to nullptr if the
  //              key does not exist.
  WriteHandle findToWrite(Key key, bool doNvmInvalidation = true);

  // look up an item by its key. This ignores the nvm cache and only does RAM
  // lookup.
  //
  // @param key         the key for lookup
  //
  // @return      the read handle for the item or a handle to nullptr if the key
  //              does not exist.
  FOLLY_ALWAYS_INLINE ReadHandle findFast(Key key);

  // look up an item by its key. This ignores the nvm cache and only does RAM
  // lookup. Users should call this API only when they are going to mutate the
  // item data.
  //
  // @param key         the key for lookup
  // @param isNvmInvalidate   whether to do nvm invalidation;
  //                          defaults to be true
  //
  // @return      the write handle for the item or a handle to nullptr if the
  //              key does not exist.
  FOLLY_ALWAYS_INLINE WriteHandle
  findFastToWrite(Key key, bool doNvmInvalidation = true);

  // look up an item by its key. This ignores the nvm cache and only does RAM
  // lookup. This API does not update the stats related to cache gets and misses
  // nor mark the item as useful (see markUseful below).
  //
  // @param key   the key for lookup
  // @return      the handle for the item or a handle to nullptr if the key does
  //              not exist.
  FOLLY_ALWAYS_INLINE ReadHandle peek(Key key);

  // Returns true if a key is potentially in cache. There is a non-zero chance
  // the key does not exist in cache (e.g. hash collision in NvmCache). This
  // check is meant to be synchronous and fast as we only check DRAM cache and
  // in-memory index for NvmCache. Similar to peek, this does not indicate to
  // cachelib you have looked up an item (i.e. no stats bump, no eviction queue
  // promotion, etc.)
  //
  // @param key   the key for lookup
  // @return      true if the key could exist, false otherwise
  bool couldExistFast(Key key);

  // Mark an item that was fetched through peek as useful. This is useful when
  // users want to look into the cache and only mark items as useful when they
  // inspect the contents of it.
  //
  // @param handle        the item handle
  // @param mode          the mode of access for the lookup. defaults to
  //                      AccessMode::kRead
  void markUseful(const ReadHandle& handle, AccessMode mode);

  using AccessIterator = typename AccessContainer::Iterator;
  // Iterator interface for the cache. It guarantees that all keys that were
  // present when the iteration started will be accessible unless they are
  // removed. Keys that are removed/inserted during the lifetime of an
  // iterator are not guaranteed to be either visited or not-visited.
  // Adding/Removing from the hash table while the iterator is alive will not
  // inivalidate any iterator or the element that the iterator points at
  // currently. The iterator internally holds a Handle to the item and hence
  // the keys that the iterator holds reference to, will not be evictable
  // until the iterator is destroyed.
  AccessIterator begin() { return accessContainer_->begin(); }

  // return an iterator with a throttler for throttled iteration
  AccessIterator begin(util::Throttler::Config config) {
    return accessContainer_->begin(config);
  }

  AccessIterator end() { return accessContainer_->end(); }

  enum class RemoveRes : uint8_t {
    kSuccess,
    kNotFoundInRam,
  };
  // removes the allocation corresponding to the key, if present in the hash
  // table. The key will not be accessible through find() after this returns
  // success. The allocation for the key will be recycled once all active
  // Item handles are released.
  //
  // @param key   the key for the allocation.
  // @return      kSuccess if the key exists and was successfully removed.
  //              kNotFoundInRam if the key was not present in memory (doesn't
  //              check nvm)
  RemoveRes remove(Key key);

  // remove the key that the iterator is pointing to. The element will
  // not be accessible upon success. However, the elemenet will not actually be
  // recycled until the iterator destroys the internal handle.
  //
  // @param  it   the iterator to the key to be destroyed.
  // @return      kSuccess if the element was still in the hashtable and it was
  //              successfully removed.
  //              kNotFoundInRam if the element the iterator was pointing to was
  //              deleted already.
  RemoveRes remove(AccessIterator& it);

  // removes the allocation corresponding to the handle. The allocation will
  // be freed when all the existing handles are released.
  //
  // @param  it   item read handle
  //
  // @return      kSuccess if the item exists and was successfully removed.
  //              kNotFoundInRam otherwise
  //
  // @throw std::invalid_argument if item handle is null
  RemoveRes remove(const ReadHandle& it);

  // view a read-only parent item as a chain of allocations if it has chained
  // alloc. The returned chained-alloc is good to iterate upon, but will block
  // any concurrent addChainedItem or popChainedItem for the same key until the
  // ChainedAllocs object is released. This is ideal for use cases which do
  // very brief operations on the chain of allocations.
  //
  // The ordering of the iteration for the chain is LIFO. Check
  // CacheChainedAllocs.h for the API and usage.
  //
  // @param parent  the parent allocation of the chain from a ReadHandle.
  // @return        read-only chained alloc view of the parent
  //
  // @throw std::invalid_argument if the parent does not have chained allocs
  ChainedAllocs viewAsChainedAllocs(const ReadHandle& parent) {
    return viewAsChainedAllocsT<ReadHandle, ChainedItemIter>(parent);
  }

  // view a writable parent item as a chain of allocations if it has chained
  // alloc. The returned chained-alloc is good to iterate upon, but will block
  // any concurrent addChainedItem or popChainedItem for the same key until the
  // ChainedAllocs object is released. This is ideal for use cases which do
  // very brief operations on the chain of allocations.
  //
  // The ordering of the iteration for the chain is LIFO. Check
  // CacheChainedAllocs.h for the API and usage.
  //
  // @param parent  the parent allocation of the chain from a WriteHandle.
  // @return        writable chained alloc view of the parent
  //
  // @throw std::invalid_argument if the parent does not have chained allocs
  WritableChainedAllocs viewAsWritableChainedAllocs(const WriteHandle& parent) {
    return viewAsChainedAllocsT<WriteHandle, WritableChainedItemIter>(parent);
  }

  // Returns the full usable size for this item
  // This can be bigger than item.getSize()
  //
  // @param item    reference to an item
  //
  // @return    the full usable size for this item
  uint32_t getUsableSize(const Item& item) const;

  // Get a random item from memory
  // This is useful for profiling and sampling cachelib managed memory
  //
  // @return Valid SampleItem if an valid item is found
  //         Invalid SampleItem if the randomly chosen memory does not
  //                 belong to an valid item
  //         Should be checked with SampleItem.isValid() before use
  SampleItem getSampleItem();

  // Convert a Read Handle to an IOBuf. The returned IOBuf gives a
  // read-only view to the user. The item's ownership is retained by
  // the IOBuf until its destruction.
  //
  // When the read handle has one or more chained items attached to it,
  // user will also get a series of IOBufs (first of which is the Parent).
  //
  // **WARNING**: folly::IOBuf allows mutation to a cachelib item even when the
  // item is read-only. User is responsible to ensure no mutation occurs (i.e.
  // only const functions are called). If mutation is required, please use
  // `convertToIOBufForWrite`.
  //
  // @param handle    read handle that will transfer its ownership to an IOBuf
  //
  // @return   an IOBuf that contains the value of the item.
  //           This IOBuf acts as a Read Handle, on destruction, it will
  //           properly decrement the refcount (to release the item).
  // @throw   std::invalid_argument if ReadHandle is nullptr
  folly::IOBuf convertToIOBuf(ReadHandle handle) {
    return convertToIOBufT<ReadHandle>(handle);
  }

  // Convert a Write Handle to an IOBuf. The returned IOBuf gives a
  // writable view to the user. The item's ownership is retained by
  // the IOBuf until its destruction.
  //
  // When the write handle has one or more chained items attached to it,
  // user will also get a series of IOBufs (first of which is the Parent).
  //
  // @param handle    write handle that will transfer its ownership to an IOBuf
  //
  // @return   an IOBuf that contains the value of the item.
  //           This IOBuf acts as a Write Handle, on destruction, it will
  //           properly decrement the refcount (to release the item).
  // @throw   std::invalid_argument if WriteHandle is nullptr
  folly::IOBuf convertToIOBufForWrite(WriteHandle handle) {
    return convertToIOBufT<WriteHandle>(handle);
  }

  // TODO: When Read/Write Handles are ready, change this to allow
  //       const-only access to data manged by iobuf and offer a
  //       wrapAsWritableIOBuf() API.
  //
  // wrap an IOBuf over the data for an item. This IOBuf does not own the item
  // and the caller is responsible for ensuring that the IOBuf is valid with
  // the item lifetime. If the item has chained allocations, the chains are
  // also wrapped into the iobuf as chained iobufs
  //
  // @param item  the item to wrap around
  //
  // @return   an IOBuf that contains the value of the item.
  folly::IOBuf wrapAsIOBuf(const Item& item);

  // creates a pool for the cache allocator with the corresponding name.
  //
  // @param name                  name of the pool
  // @param size                  size of the pool
  // @param allocSizes            allocation class sizes, if empty, a default
  //                              one from the memory allocator will be used
  // @param config                MMConfig for the MMContainer,
  //                              default constructed if user doesn't supply one
  // @param rebalanceStrategy     rebalance strategy for the pool. If not set,
  //                              the default one will be used.
  // @param resizeStrategy        resize strategy for the pool. If not set,
  //                              the default one will be used.
  // @param ensureProvisionable   ensures that the size of the pool is enough
  //                              to give one slab to each allocation class,
  //                              false by default.
  //
  // @return a valid PoolId that the caller can use.
  // @throw   std::invalid_argument if the size is invalid or there is not
  //          enough space for creating the pool.
  //          std::logic_error if we have run out of pools.
  PoolId addPool(folly::StringPiece name,
                 size_t size,
                 const std::set<uint32_t>& allocSizes = {},
                 MMConfig config = {},
                 std::shared_ptr<RebalanceStrategy> rebalanceStrategy = nullptr,
                 std::shared_ptr<RebalanceStrategy> resizeStrategy = nullptr,
                 bool ensureProvisionable = false);

  // update an existing pool's config
  //
  // @param pid       pool id for the pool to be updated
  // @param config    new config for the pool
  //
  // @throw std::invalid_argument if the poolId is invalid
  void overridePoolConfig(PoolId pid, const MMConfig& config);

  // update an existing pool's rebalance strategy
  //
  // @param pid                 pool id for the pool to be updated
  // @param rebalanceStrategy   new rebalance strategy for the pool
  //
  // @throw std::invalid_argument if the poolId is invalid
  void overridePoolRebalanceStrategy(
      PoolId pid, std::shared_ptr<RebalanceStrategy> rebalanceStrategy);

  // update an existing pool's resize strategy
  //
  // @param pid                 pool id for the pool to be updated
  // @param resizeStrategy      new resize strategy for the pool
  //
  // @throw std::invalid_argument if the poolId is invalid
  void overridePoolResizeStrategy(
      PoolId pid, std::shared_ptr<RebalanceStrategy> resizeStrategy);

  // update pool size optimization strategy for this cache
  // @param optimizeStrategy    new resize strategy
  void overridePoolOptimizeStrategy(
      std::shared_ptr<PoolOptimizeStrategy> optimizeStrategy);

  /**
   * PoolResizing can be done online while the cache allocator is being used
   * to do allocations. Pools can be grown or shrunk using the following api.
   * The actual resizing happens asynchronously and is controlled by the
   * config parameters poolResizeIntervalSecs and poolResizeSlabsPerIter. The
   * pool resizer releases slabs from pools that are over limit when the
   * memory allocator is out of memory. If there is enough free memory
   * available, the pool resizer does not do any resizing until the memory is
   * exhausted and there is some pool that is over the limit
   */

  // shrink the existing pool by _bytes_ .
  // @param bytes  the number of bytes to be taken away from the pool
  // @return  true if the operation succeeded. false if the size of the pool is
  //          smaller than _bytes_
  // @throw   std::invalid_argument if the poolId is invalid.
  bool shrinkPool(PoolId pid, size_t bytes) {
    return allocator_->shrinkPool(pid, bytes);
  }

  // grow an existing pool by _bytes_. This will fail if there is no
  // available memory across all the pools to provide for this pool
  // @param bytes  the number of bytes to be added to the pool.
  // @return    true if the pool was grown. false if the necessary number of
  //            bytes were not available.
  // @throw     std::invalid_argument if the poolId is invalid.
  bool growPool(PoolId pid, size_t bytes) {
    return allocator_->growPool(pid, bytes);
  }

  // move bytes from one pool to another. The source pool should be at least
  // _bytes_ in size.
  //
  // @param src     the pool to be sized down and giving the memory.
  // @param dest    the pool receiving the memory.
  // @param bytes   the number of bytes to move from src to dest.
  // @param   true if the resize succeeded. false if src does does not have
  //          correct size to do the transfer.
  // @throw   std::invalid_argument if src or dest is invalid pool
  bool resizePools(PoolId src, PoolId dest, size_t bytes) override {
    return allocator_->resizePools(src, dest, bytes);
  }

  // Add a new compact cache with given name and size
  //
  // @param name  name of the compact cache pool
  // @param size  size of the compact cache pool
  // @param args  All of the arguments in CompactCache afer allocator
  //              So the signature of addCompactCache is:
  //              addCompactCache(folly::StringPiece name,
  //                              size_t size,
  //                              RemoveCb removeCb,
  //                              ReplaceCb replaceCb,
  //                              ValidCb validCb,
  //                              bool allowPromotions = true);
  //              addCompactCache(folly::StringPiece name,
  //                              size_t size,
  //                              bool allowPromotions = true);
  //
  // @return pointer to CompactCache instance of the template type
  //
  // @throw std::logic_error        if compact cache is not enabled
  // @throw std::invalid_argument   There is a memory pool that has the same
  //                                name as the compact cache we are adding or
  //                                if there is no sufficient space to create
  //                                a compact cache.
  template <typename CCacheT, typename... Args>
  CCacheT* addCompactCache(folly::StringPiece name,
                           size_t size,
                           Args&&... args);

  // Attach a compact cache to the given pool after warm roll
  //
  // @param name  name of the compact cache pool
  // @param args  All of the arguments in CompactCache afer allocator
  //              So the signature of attachCompactCache is:
  //              attachCompactCache(folly::StringPiece name,
  //                                 RemoveCb removeCb,
  //                                 ReplaceCb replaceCb,
  //                                 ValidCb validCb,
  //                                 bool allowPromotions = true);
  //              attachCompactCache(folly::StringPiece name,
  //                                 bool allowPromotions = true);
  //
  // @return  pointer to CompactCache instance of the template type.
  //
  // @throw std::out_of_range       if the pool does not exist
  // @throw std::invalid_argument   if the compact key/value size does not match
  //                                from warm roll
  template <typename CCacheT, typename... Args>
  CCacheT* attachCompactCache(folly::StringPiece name, Args&&... args);

  // Return the base iterface of an attached compact cache to pull out its
  // stats. For non-active compact cache, this would throw
  // std::invalid_argument.
  const ICompactCache& getCompactCache(PoolId pid) const override;

  // The enum value that indicates the CacheAllocator's shutdown status.
  enum class ShutDownStatus {
    kSuccess = 0, // Successfully persisted the DRAM cache, and the NvmCache if
                  // enabled.
    kSavedOnlyDRAM, // Successfully persisted the DRAM cache only; NvmCache is
                    // enabled but failed to persist it.
    kSavedOnlyNvmCache, // Successfully persisted the enabled NvM cache only;
                        // Failed to persist DRAM cache.
    kFailed // Failed to persist both the DRAM cache and the enabled NvmCache.
  };

  // Persists the state of the cache allocator. On a successful shutdown,
  // this cache allocator can be restored on restart.
  //
  // precondition:  serialization must happen without any reader or writer
  // present. Any modification of this object afterwards will result in an
  // invalid, inconsistent state for the serialized data. There must not be
  // any outstanding active handles
  //
  // @throw   std::invalid_argument if the cache allocator isn't using shared
  //          memory
  // @throw   std::logic_error if any component is not restorable.
  // @return  A ShutDownStatus value indicating the result of the shutDown
  //          operation.
  //          kSuccess - successfully shut down and can be re-attached
  //          kFailed - failure due to outstanding active handle or error with
  //                    cache dir
  //          kSavedOnlyDRAM and kSavedOnlyNvmCache - partial content saved
  ShutDownStatus shutDown();

  // No-op for workers that are already running. Typically user uses this in
  // conjunction with `config.delayWorkerStart()` to avoid initialization
  // ordering issues with user callback for cachelib's workers.
  void startCacheWorkers();

  // Functions that stop existing ones (if any) and create new workers

  // start pool rebalancer
  // @param interval            the period this worker fires.
  // @param strategy            rebalancing strategy
  // @param freeAllocThreshold  threshold for free-alloc-slab for picking victim
  // allocation class. free-alloc-slab is calculated by the number of free
  // allocation divided by the number of allocations in one slab. Only
  // allocation classes with a higher free-alloc-slab than the threshold would
  // be picked as a victim.
  //
  //
  bool startNewPoolRebalancer(std::chrono::milliseconds interval,
                              std::shared_ptr<RebalanceStrategy> strategy,
                              unsigned int freeAllocThreshold);

  // start pool resizer
  // @param interval                the period this worker fires.
  // @param poolResizeSlabsPerIter  maximum number of slabs each pool may remove
  //                                in resizing.
  // @param strategy                resizing strategy
  bool startNewPoolResizer(std::chrono::milliseconds interval,
                           unsigned int poolResizeSlabsPerIter,
                           std::shared_ptr<RebalanceStrategy> strategy);

  // start pool optimizer
  // @param regularInterval         the period for optimizing regular cache
  // @param ccacheInterval          the period for optimizing compact cache
  // @param strategy                pool optimizing strategy
  // @param ccacheStepSizePercent   the percentage number that controls the size
  //                                of each movement in a compact cache
  //                                optimization.
  bool startNewPoolOptimizer(std::chrono::seconds regularInterval,
                             std::chrono::seconds ccacheInterval,
                             std::shared_ptr<PoolOptimizeStrategy> strategy,
                             unsigned int ccacheStepSizePercent);
  // start memory monitor
  // @param memMonitorMode                  memory monitor mode
  // @param interval                        the period this worker fires
  // @param memAdvisePercentPerIter         Percentage of
  //                                        upperLimitGB-lowerLimitGB to be
  //                                        advised every poll period. This
  //                                        governs the rate of advise
  // @param memReclaimPercentPerIter        Percentage of
  //                                        upperLimitGB-lowerLimitGB to be
  //                                        reclaimed every poll period. This
  //                                        governs the rate of reclaim
  // @param memLowerLimit                   The lower limit of resident memory
  //                                        in GBytes
  //                                        that triggers reclaiming of
  //                                        previously advised away of memory
  //                                        from cache
  // @param memUpperLimit                   The upper limit of resident memory
  //                                         in GBytes
  //                                        that triggers advising of memory
  //                                        from cache
  // @param memMaxAdvisePercent             Maximum percentage of item cache
  //                                        limit that
  //                                        can be advised away before advising
  //                                        is disabled leading to a probable
  //                                        OOM.
  // @param strategy                        strategy to find an allocation class
  //                                        to release slab from
  // @param reclaimRateLimitWindowSecs      specifies window in seconds over
  //                                        which free/resident memory values
  //                                        are tracked to determine rate of
  //                                        change to rate limit reclaim
  bool startNewMemMonitor(MemoryMonitor::Mode memMonitorMode,
                          std::chrono::milliseconds interval,
                          unsigned int memAdvisePercentPerIter,
                          unsigned int memReclaimPercentPerIter,
                          unsigned int memLowerLimitGB,
                          unsigned int memUpperLimitGB,
                          unsigned int memMaxAdvisePercent,
                          std::shared_ptr<RebalanceStrategy> strategy,
                          std::chrono::seconds reclaimRateLimitWindowSecs);
  // start memory monitor
  // @param interval                        the period this worker fires
  // @param config                          memory monitoring config
  // @param strategy                        strategy to find an allocation class
  //                                        to release slab from
  bool startNewMemMonitor(std::chrono::milliseconds interval,
                          MemoryMonitor::Config config,
                          std::shared_ptr<RebalanceStrategy> strategy);

  // start reaper
  // @param interval                the period this worker fires
  // @param reaperThrottleConfig    throttling config
  bool startNewReaper(std::chrono::milliseconds interval,
                      util::Throttler::Config reaperThrottleConfig);

  // Stop existing workers with a timeout
  bool stopPoolRebalancer(std::chrono::seconds timeout = std::chrono::seconds{
                              0});
  bool stopPoolResizer(std::chrono::seconds timeout = std::chrono::seconds{0});
  bool stopPoolOptimizer(std::chrono::seconds timeout = std::chrono::seconds{
                             0});
  bool stopMemMonitor(std::chrono::seconds timeout = std::chrono::seconds{0});
  bool stopReaper(std::chrono::seconds timeout = std::chrono::seconds{0});

  // Set pool optimization to either true or false
  //
  // @param poolId The ID of the pool to optimize
  void setPoolOptimizerFor(PoolId poolId, bool enableAutoResizing);

  // stop the background workers
  // returns true if all workers have been successfully stopped
  bool stopWorkers(std::chrono::seconds timeout = std::chrono::seconds{0});

  // get the allocation information for the specified memory address.
  // @throw std::invalid_argument if the memory does not belong to this
  //        cache allocator
  AllocInfo getAllocInfo(const void* memory) const {
    return allocator_->getAllocInfo(memory);
  }

  // return the ids for the set of existing pools in this cache.
  std::set<PoolId> getPoolIds() const override final {
    return allocator_->getPoolIds();
  }

  // return a list of pool ids that are backing compact caches. This includes
  // both attached and un-attached compact caches.
  std::set<PoolId> getCCachePoolIds() const override final;

  // return a list of pool ids for regular pools.
  std::set<PoolId> getRegularPoolIds() const override final;

  // return the pool with speicified id.
  const MemoryPool& getPool(PoolId pid) const override final {
    return allocator_->getPool(pid);
  }

  // calculate the number of slabs to be advised/reclaimed in each pool
  PoolAdviseReclaimData calcNumSlabsToAdviseReclaim() override final {
    auto regularPoolIds = getRegularPoolIds();
    return allocator_->calcNumSlabsToAdviseReclaim(regularPoolIds);
  }

  // update number of slabs to advise in the cache
  void updateNumSlabsToAdvise(int32_t numSlabsToAdvise) override final {
    allocator_->updateNumSlabsToAdvise(numSlabsToAdvise);
  }

  // returns a valid PoolId corresponding to the name or kInvalidPoolId if the
  // name is not a recognized pool
  PoolId getPoolId(folly::StringPiece name) const noexcept;

  // returns the pool's name by its poolId.
  std::string getPoolName(PoolId poolId) const override {
    return allocator_->getPoolName(poolId);
  }

  // get stats related to all kinds of slab release events.
  SlabReleaseStats getSlabReleaseStats() const noexcept override final;

  // return the distribution of the keys in the cache. This is expensive to
  // compute at times even with caching. So use with caution.
  // TODO think of a way to abstract this since it only makes sense for
  // bucketed hashtables with chaining.
  using DistributionStats = typename AccessContainer::DistributionStats;
  DistributionStats getAccessContainerDistributionStats() const {
    return accessContainer_->getDistributionStats();
  }

  // Provide stats on the number of keys cached and the access container for
  // this cache.
  using AccessStats = typename AccessContainer::Stats;
  AccessStats getAccessContainerStats() const {
    return accessContainer_->getStats();
  }

  // Get the total number of keys inserted into the access container
  uint64_t getAccessContainerNumKeys() const {
    return accessContainer_->getNumKeys();
  }

  // returns the reaper stats
  ReaperStats getReaperStats() const {
    auto stats = reaper_ ? reaper_->getStats() : ReaperStats{};
    return stats;
  }

  // returns the pool rebalancer stats
  RebalancerStats getRebalancerStats() const {
    auto stats =
        poolRebalancer_ ? poolRebalancer_->getStats() : RebalancerStats{};
    return stats;
  }

  // return the LruType of an item
  typename MMType::LruType getItemLruType(const Item& item) const;

  // return the recent slab release events for a pool for rebalancer, Resizer
  // and Memory monitor workers.
  AllSlabReleaseEvents getAllSlabReleaseEvents(PoolId pid) const override final;

  // get cache name
  const std::string getCacheName() const override final;

  // whether it is object-cache
  bool isObjectCache() const override final { return false; }

  // pool stats by pool id
  PoolStats getPoolStats(PoolId pid) const override final;

  // This can be expensive so it is not part of PoolStats
  PoolEvictionAgeStats getPoolEvictionAgeStats(
      PoolId pid, unsigned int slabProjectionLength) const override final;

  // return the cache's metadata
  CacheMetadata getCacheMetadata() const noexcept override final;

  // return the overall cache stats
  GlobalCacheStats getGlobalCacheStats() const override final;

  // return cache's memory usage stats
  CacheMemoryStats getCacheMemoryStats() const override final;

  // return the nvm cache stats map
  util::StatsMap getNvmCacheStatsMap() const override final;

  // return the event tracker stats map
  std::unordered_map<std::string, uint64_t> getEventTrackerStatsMap()
      const override {
    std::unordered_map<std::string, uint64_t> eventTrackerStats;
    if (auto eventTracker = getEventTracker()) {
      eventTracker->getStats(eventTrackerStats);
    }
    return eventTrackerStats;
  }

  // Whether this cache allocator was created on shared memory.
  bool isOnShm() const noexcept { return isOnShm_; }

  // Whether NvmCache is currently enabled
  bool isNvmCacheEnabled() const noexcept {
    return nvmCache_ && nvmCache_->isEnabled();
  }

  // unix timestamp when the cache was created. If 0, the cache creation
  // time was not recorded from the older version of the binary.
  //
  // @return  time when the cache was created.
  time_t getCacheCreationTime() const noexcept { return cacheCreationTime_; }
  time_t getNVMCacheCreationTime() const {
    return nvmCacheState_.getCreationTime();
  }

  // Inspects the cache without changing its state.
  //
  // @param key     for the cache item
  // @return  std::pair<ReadHandle, ReadHandle> the first represents the state
  //          in the RAM and the second is a copy of the state in NVM
  std::pair<ReadHandle, ReadHandle> inspectCache(Key key);

  // blocks until the inflight operations are flushed to nvmcache. Used for
  // benchmarking when we want to load up the cache first with some data and
  // run the benchmarks after flushing.
  void flushNvmCache();

  // Dump the last N items for an evictable MM Container
  // @return  vector of the string of each item. Empty if nothing in LRU
  // @throw  std::invalid_argument if <pid, cid> does not exist
  std::vector<std::string> dumpEvictionIterator(PoolId pid,
                                                ClassId cid,
                                                size_t numItems = 10);

  // returns the current count of the active handles that are handed out
  // through the API. This also includes the handles that are maintained by
  // the iterator and internal rebalancing threads.
  int64_t getNumActiveHandles() const;

  // returns the current count of handle at an given time for this thread. If
  // the threads do not transfer handles between them, the caller can rely on
  // this being the current active outstanding handles. If handles can be
  // transferred, then the caller needs to take snapshot of the count before
  // and after and make sure that the count reflects any transfer.
  //
  // TODO (sathya) wrap this into a guard object
  //
  // @return  the current handle count. If handles are never transferred
  //          between threads, this will always be >=0. If handles are
  //          transferred, this can be negative  on threads that give away
  //          handles and always non zero on threads that acquire a handle from
  //          another thread.
  int64_t getHandleCountForThread() const;

  // (Deprecated) reset and adjust handle count for the current thread.
  // Please do not use this API as it will be moved to a private function.
  void resetHandleCountForThread_private();
  void adjustHandleCountForThread_private(int64_t delta);

  // madvise(MADV_DODUMP) all recently accessed items.
  // this function is intended for signal handler which can mprotect other
  // code. Hence, it is  inlined to make sure that the code lives in the
  // caller's text segment
  void FOLLY_ALWAYS_INLINE madviseRecentlyAccessedItems() const {
    for (const auto& tlring : ring_.accessAllThreads()) {
      tlring.madviseItems();
    }
  }

  // Mark the item as dirty and enqueue for deletion from nvmcache
  // @param item         item to invalidate.
  void invalidateNvm(Item& item);

  // Attempts to clean up left-over shared memory from preivous instance of
  // cachelib cache for the cache directory. If there are other processes
  // using the same directory, we don't touch it. If the directory is not
  // present, we do our best to clean up based on what is possible.
  // It is hard to determine if we actually cleaned up something.
  //
  // returns true if there was no error in trying to cleanup the segment
  // because another process was attached. False if the user tried to clean up
  // and the cache was actually attached.
  static bool cleanupStrayShmSegments(const std::string& cacheDir, bool posix);

  // gives a relative offset to a pointer within the cache.
  uint64_t getItemPtrAsOffset(const void* ptr);

  // this ensures that we dont introduce any more hidden fields like vtable by
  // inheriting from the Hooks and their bool interface.
  static_assert((sizeof(typename MMType::template Hook<Item>) +
                 sizeof(typename AccessType::template Hook<Item>) +
                 sizeof(typename RefcountWithFlags::Value) + sizeof(uint32_t) +
                 sizeof(uint32_t) + sizeof(KAllocation)) == sizeof(Item),
                "vtable overhead");
  static_assert(32 == sizeof(Item), "item overhead is 32 bytes");

  // make sure there is no overhead in ChainedItem on top of a regular Item
  static_assert(sizeof(Item) == sizeof(ChainedItem),
                "Item and ChainedItem must be the same size");

  static_assert(std::is_standard_layout<KAllocation>::value,
                "KAllocation not standard layout");
  static_assert(std::is_standard_layout<ChainedItemPayload<CacheTrait>>::value,
                "ChainedItemPayload not standard layout");

// ensure that Item::alloc_ is the last member of Item. If not,
// Item::alloc_::data[0] will not work as a variable sized struct.
// gcc is strict about using offsetof in Item when Item has a default
// constructor, hence becoming a non-POD. Suppress -Winvalid-offsetof for
// this sake.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
  static_assert(sizeof(Item) - offsetof(Item, alloc_) == sizeof(Item::alloc_),
                "alloc_ incorrectly arranged");
#pragma GCC diagnostic pop

 private:
  // wrapper around Item's refcount and active handle tracking
  FOLLY_ALWAYS_INLINE bool incRef(Item& it);
  FOLLY_ALWAYS_INLINE RefcountWithFlags::Value decRef(Item& it);

  // drops the refcount and if needed, frees the allocation back to the memory
  // allocator and executes the necessary callbacks. no-op if it is nullptr.
  FOLLY_ALWAYS_INLINE void release(Item* it, bool isNascent);

  // Differtiate different memory setting for the initialization
  enum class InitMemType { kNone, kMemNew, kMemAttach };
  // instantiates a cache allocator for common initialization
  //
  // @param types         the type of the memory used
  // @param config        the configuration for the whole cache allocator
  CacheAllocator(InitMemType types, Config config);

  // This is the last step in item release. We also use this for the eviction
  // scenario where we have to do everything, but not release the allocation
  // to the allocator and instead recycle it for another new allocation. If
  // toRecycle is present, the item to be released and the item to be recycled
  // will be the same if we want to recycle a parent item. If we want to
  // recycle a child item, the parent would be the item to release and the
  // child will be the item to recycle. If it is a chained item, we simply
  // free it back.
  //
  // 1. It calls the remove callback
  // 2. It releases the item and chained allocation memory back to allocator
  //
  // An item should have its refcount dropped to zero, and unlinked from
  // access and mm container before being passed to this function.
  //
  // @param it    item to be released.
  // @param ctx   removal context
  // @param toRecycle  An item that will be recycled, this item is to be
  //                   ignored if it's found in the process of freeing
  //                   a chained allocation
  //
  // @return One of ReleaseRes. In all cases, _it_ is always released back to
  // the allocator unless an exception is thrown
  //
  // @throw   runtime_error if _it_ has pending refs or is not a regular item.
  //          runtime_error if parent->chain is broken
  enum class ReleaseRes {
    kRecycled,    // _it_ was released and _toRecycle_ was recycled
    kNotRecycled, // _it_ was released and _toRecycle_ was not recycled
    kReleased,    // toRecycle == nullptr and it was released
  };
  ReleaseRes releaseBackToAllocator(Item& it,
                                    RemoveContext ctx,
                                    bool nascent = false,
                                    const Item* toRecycle = nullptr);

  // acquires an handle on the item. returns an empty handle if it is null.
  // @param it    pointer to an item
  // @return WriteHandle   return a handle to this item
  // @throw std::overflow_error is the maximum item refcount is execeeded by
  //        creating this item handle.
  WriteHandle acquire(Item* it);

  // creates an item handle with wait context.
  WriteHandle createNvmCacheFillHandle() { return WriteHandle{*this}; }

  // acquires the wait context for the handle. This is used by NvmCache to
  // maintain a list of waiters
  std::shared_ptr<WaitContext<ReadHandle>> getWaitContext(
      ReadHandle& hdl) const {
    return hdl.getItemWaitContext();
  }

  using MMContainerPtr = std::unique_ptr<MMContainer>;
  using MMContainers =
      std::array<std::array<MMContainerPtr, MemoryAllocator::kMaxClasses>,
                 MemoryPoolManager::kMaxPools>;

  void createMMContainers(const PoolId pid, MMConfig config);

  // acquire the MMContainer corresponding to the the Item's class and pool.
  //
  // @return pointer to the MMContainer.
  // @throw  std::invalid_argument if the Item does not point to a valid
  // allocation from the memory allocator.
  MMContainer& getMMContainer(const Item& item) const noexcept;

  MMContainer& getMMContainer(PoolId pid, ClassId cid) const noexcept;

  // create a new cache allocation. The allocation can be initialized
  // appropriately and made accessible through insert or insertOrReplace.
  // If the handle returned from this api is not passed on to
  // insert/insertOrReplace, the allocation gets destroyed when the handle
  // goes out of scope.
  //
  // @param id              the pool id for the allocation that was previously
  //                        created through addPool
  // @param key             the key for the allocation. This will be made a
  //                        part of the Item and be available through getKey().
  // @param size            the size of the allocation, exclusive of the key
  //                        size.
  // @param creationTime    Timestamp when this item was created
  // @param expiryTime      set an expiry timestamp for the item (0 means no
  //                        expiration time).
  //
  // @return      the handle for the item or an invalid handle(nullptr) if the
  //              allocation failed. Allocation can fail if one such
  //              allocation already exists or if we are out of memory and
  //              can not find an eviction. Handle must be destroyed *before*
  //              the instance of the CacheAllocator gets destroyed
  // @throw   std::invalid_argument if the poolId is invalid or the size
  //          requested is invalid or if the key is invalid(key.size() == 0 or
  //          key.size() > 255)
  WriteHandle allocateInternal(PoolId id,
                               Key key,
                               uint32_t size,
                               uint32_t creationTime,
                               uint32_t expiryTime);

  // Allocate a chained item
  //
  // The resulting chained item does not have a parent item and
  // will be freed once the handle is dropped
  //
  // The parent handle parameter here is mainly used to find the
  // correct pool to allocate memory for this chained item
  //
  // @param parent    handle to the cache item
  // @param size      the size for the chained allocation
  //
  // @return    handle to the chained allocation
  // @throw     std::invalid_argument if the size requested is invalid or
  //            if the item is invalid
  WriteHandle allocateChainedItemInternal(const ReadHandle& parent,
                                          uint32_t size);

  // Given an item and its parentKey, validate that the parentKey
  // corresponds to an item that's the parent of the supplied item.
  //
  // @param item       item that we want to get the parent handle for
  // @param parentKey  key of the item's parent
  //
  // @return  handle to the parent item if the validations pass
  //          otherwise, an empty Handle is returned.
  //
  ReadHandle validateAndGetParentHandleForChainedMoveLocked(
      const ChainedItem& item, const Key& parentKey);

  // Given an existing item, allocate a new one for the
  // existing one to later be moved into.
  //
  // @param oldItem    the item we want to allocate a new item for
  //
  // @return  handle to the newly allocated item
  //
  WriteHandle allocateNewItemForOldItem(const Item& oldItem);

  // internal helper that grabs a refcounted handle to the item. This does
  // not record the access to reflect in the mmContainer.
  //
  // @param key   key to look up in the access container
  //
  // @return handle if item is found, nullptr otherwise
  //
  // @throw std::overflow_error is the maximum item refcount is execeeded by
  //        creating this item handle.
  WriteHandle findInternal(Key key) {
    // Note: this method can not be const because we need  a non-const
    // reference to create the ItemReleaser.
    return accessContainer_->find(key);
  }

  // look up an item by its key. This ignores the nvm cache and only does RAM
  // lookup.
  //
  // @param key         the key for lookup
  // @param mode        the mode of access for the lookup.
  //                    AccessMode::kRead or AccessMode::kWrite
  //
  // @return      the handle for the item or a handle to nullptr if the key does
  //              not exist.
  FOLLY_ALWAYS_INLINE WriteHandle findFastInternal(Key key, AccessMode mode);

  // look up an item by its key across the nvm cache as well if enabled.
  //
  // @param key         the key for lookup
  // @param mode        the mode of access for the lookup.
  //                    AccessMode::kRead or AccessMode::kWrite
  //
  // @return      the handle for the item or a handle to nullptr if the key does
  //              not exist.
  FOLLY_ALWAYS_INLINE WriteHandle findImpl(Key key, AccessMode mode);

  // look up an item by its key. This ignores the nvm cache and only does RAM
  // lookup.
  //
  // @param key         the key for lookup
  // @param mode        the mode of access for the lookup.
  //                    AccessMode::kRead or AccessMode::kWrite
  //
  // @return      the handle for the item or a handle to nullptr if the key does
  //              not exist.
  FOLLY_ALWAYS_INLINE WriteHandle findFastImpl(Key key, AccessMode mode);

  // Moves a regular item to a different slab. This should only be used during
  // slab release after the item's exclusive bit has been set. The user supplied
  // callback is responsible for copying the contents and fixing the semantics
  // of chained item.
  //
  // @param oldItem     Reference to the item being moved
  // @param newItemHdl  Reference to the handle of the new item being moved into
  //
  // @return true  If the move was completed, and the containers were updated
  //               successfully.
  bool moveRegularItem(Item& oldItem, WriteHandle& newItemHdl);

  // template class for viewAsChainedAllocs that takes either ReadHandle or
  // WriteHandle
  template <typename Handle, typename Iter>
  CacheChainedAllocs<CacheT, Handle, Iter> viewAsChainedAllocsT(
      const Handle& parent);

  // template class for convertToIOBuf that takes either ReadHandle or
  // WriteHandle
  template <typename Handle>
  folly::IOBuf convertToIOBufT(Handle& handle);

  // Moves a chained item to a different slab. This should only be used during
  // slab release after the item's exclusive bit has been set. The user supplied
  // callback is responsible for copying the contents and fixing the semantics
  // of chained item.
  //
  // Note: If we have successfully moved the old item into the new, the
  //       newItemHdl is reset and no longer usable by the caller.
  //
  // @param oldItem       Reference to the item being moved
  // @param newItemHdl    Reference to the handle of the new item being
  //                      moved into
  //
  // @return true  If the move was completed, and the containers were updated
  //               successfully.
  bool moveChainedItem(ChainedItem& oldItem, WriteHandle& newItemHdl);

  // Transfers the chain ownership from parent to newParent. Parent
  // will be unmarked as having chained allocations. Parent will not be null
  // after calling this API.
  //
  // Parent and NewParent must be valid handles to items with same key and
  // parent must have chained items and parent handle must be the only
  // outstanding handle for parent. New parent must be without any chained item
  // handles.
  //
  // Chained item lock for the parent's key needs to be held in exclusive mode.
  //
  // @param parent    the current parent of the chain we want to transfer
  // @param newParent the new parent for the chain
  //
  // @throw if any of the conditions for parent or newParent are not met.
  void transferChainLocked(WriteHandle& parent, WriteHandle& newParent);

  // replace a chained item in the existing chain. This needs to be called
  // with the chained item lock held exclusive
  //
  // @param oldItem  the item we are replacing in the chain
  // @param newItem  the item we are replacing it with
  // @param parent   the parent for the chain
  //
  // @return handle to the oldItem
  WriteHandle replaceChainedItemLocked(Item& oldItem,
                                       WriteHandle newItemHdl,
                                       const Item& parent);

  // Insert an item into MM container. The caller must hold a valid handle for
  // the item.
  //
  // @param  item  Item that we want to insert.
  //
  // @throw std::runtime_error if the handle is already in the mm container
  void insertInMMContainer(Item& item);

  // Removes an item from the corresponding MMContainer if it is in the
  // container. The caller must hold a valid handle for the item.
  //
  // @param  item  Item that we want to remove.
  //
  // @return true   if the item is successfully removed
  //         false  if the item is not in MMContainer
  bool removeFromMMContainer(Item& item);

  // Replaces an item in the MMContainer with another item, at the same
  // position.
  //
  // @param oldItem   item being replaced
  // @param newItem   item to replace oldItem with
  //
  // @return true  If the replace was successful. Returns false if the
  //               destination item did not exist in the container, or if the
  //               source item already existed.
  bool replaceInMMContainer(Item& oldItem, Item& newItem);

  // Replaces an item in the MMContainer with another item, at the same
  // position. Or, if the two chained items belong to two different MM
  // containers, remove the old one from its MM container and add the new
  // one to its MM container.
  //
  // @param oldItem   item being replaced
  // @param newItem   item to replace oldItem with
  //
  // @return true  If the replace was successful. Returns false if the
  //               destination item did not exist in the container, or if the
  //               source item already existed.
  bool replaceChainedItemInMMContainer(Item& oldItem, Item& newItem);

  // Only replaces an item if it is accessible
  bool replaceIfAccessible(Item& oldItem, Item& newItem);

  // Inserts the allocated handle into the AccessContainer, making it
  // accessible for everyone. This needs to be the handle that the caller
  // allocated through _allocate_. If this call fails, the allocation will be
  // freed back when the handle gets out of scope in the caller.
  //
  // @param handle the handle for the allocation.
  // @param event AllocatorApiEvent that corresponds to the current operation.
  //              supported events are INSERT, corresponding to the client
  //              insert call, and INSERT_FROM_NVM, cooresponding to the insert
  //              call that happens when an item is promoted from NVM storage
  //              to memory.
  //
  // @return true if the handle was successfully inserted into the hashtable
  //         and is now accessible to everyone. False if there was an error.
  //
  // @throw std::invalid_argument if the handle is already accessible or invalid
  bool insertImpl(const WriteHandle& handle, AllocatorApiEvent event);

  // Removes an item from the access container and MM container.
  //
  // @param hk               the hashed key for the item
  // @param it               Item to remove
  // @param tombstone        A tombstone for nvm::remove job created by
  //                         nvm::createDeleteTombStone, can be empty if nvm is
  //                         not enable, or removeFromNvm is false
  // @param removeFromNvm    if true clear key from nvm
  // @param recordApiEvent   should we record API event for this operation.
  RemoveRes removeImpl(HashedKey hk,
                       Item& it,
                       DeleteTombStoneGuard tombstone,
                       bool removeFromNvm = true,
                       bool recordApiEvent = true);

  // Implementation to find a suitable eviction from the container. The
  // two parameters together identify a single container.
  //
  // @param  pid  the id of the pool to look for evictions inside
  // @param  cid  the id of the class to look for evictions inside
  // @return An evicted item or nullptr  if there is no suitable candidate.
  Item* findEviction(PoolId pid, ClassId cid);

  using EvictionIterator = typename MMContainer::LockedIterator;

  // Advance the current iterator and try to evict a regular item
  //
  // @param  mmContainer  the container to look for evictions.
  // @param  itr          iterator holding the item
  //
  // @return  valid handle to regular item on success. This will be the last
  //          handle to the item. On failure an empty handle.
  WriteHandle advanceIteratorAndTryEvictRegularItem(MMContainer& mmContainer,
                                                    EvictionIterator& itr);

  // Advance the current iterator and try to evict a chained item
  // Iterator may also be reset during the course of this function
  //
  // @param  itr          iterator holding the item
  //
  // @return  valid handle to the parent item on success. This will be the last
  //          handle to the item
  WriteHandle advanceIteratorAndTryEvictChainedItem(EvictionIterator& itr);

  // Deserializer CacheAllocatorMetadata and verify the version
  //
  // @param  deserializer   Deserializer object
  // @throws                runtime_error
  serialization::CacheAllocatorMetadata deserializeCacheAllocatorMetadata(
      Deserializer& deserializer);

  MMContainers deserializeMMContainers(
      Deserializer& deserializer,
      const typename Item::PtrCompressor& compressor);

  unsigned int reclaimSlabs(PoolId id, size_t numSlabs) final {
    return allocator_->reclaimSlabsAndGrow(id, numSlabs);
  }

  FOLLY_ALWAYS_INLINE EventTracker* getEventTracker() const {
    return config_.eventTracker.get();
  }

  // Releases a slab from a pool into its corresponding memory pool
  // or back to the slab allocator, depending on SlabReleaseMode.
  //  SlabReleaseMode::kRebalance -> back to the pool
  //  SlabReleaseMode::kResize -> back to the slab allocator
  //
  // @param pid        Pool that will make make one slab available
  // @param cid        Class that will release a slab back to its pool
  //                   or slab allocator
  // @param mode       the mode for slab release (rebalance/resize)
  // @param hint       hint referring to the slab. this can be an allocation
  //                   that the user knows to exist in the slab. If this is
  //                   nullptr, a random slab is selected from the pool and
  //                   allocation class.
  //
  // @throw std::invalid_argument if the hint is invalid or if the pid or cid
  //        is invalid.
  // @throw std::runtime_error if fail to release a slab due to internal error
  void releaseSlab(PoolId pid,
                   ClassId cid,
                   SlabReleaseMode mode,
                   const void* hint = nullptr) final;

  // Releasing a slab from this allocation class id and pool id. The release
  // could be for a pool resizing or allocation class rebalancing.
  //
  // All active allocations will be evicted in the process.
  //
  // This function will be blocked until a slab is freed
  //
  // @param pid        Pool that will make one slab available
  // @param victim     Class that will release a slab back to its pool
  //                   or slab allocator or a receiver if defined
  // @param receiver   Class that will receive when the mode is set to rebalance
  // @param mode  the mode for slab release (rebalance/resize)
  //              the mode must be rebalance if an valid receiver is specified
  // @param hint  hint referring to the slab. this can be an allocation
  //              that the user knows to exist in the slab. If this is
  //              nullptr, a random slab is selected from the pool and
  //              allocation class.
  //
  // @throw std::invalid_argument if the hint is invalid or if the pid or cid
  //        is invalid. Or if the mode is set to kResize but the receiver is
  //        also specified. Receiver class id can only be specified if the mode
  //        is set to kRebalance.
  // @throw std::runtime_error if fail to release a slab due to internal error
  void releaseSlab(PoolId pid,
                   ClassId victim,
                   ClassId receiver,
                   SlabReleaseMode mode,
                   const void* hint = nullptr) final;

  // @param releaseContext  slab release context
  void releaseSlabImpl(const SlabReleaseContext& releaseContext);

  // @return  true when successfully marked as moving,
  //          fasle when this item has already been freed
  bool markMovingForSlabRelease(const SlabReleaseContext& ctx,
                                void* alloc,
                                util::Throttler& throttler);

  // "Move" (by copying) the content in this item to another memory
  // location by invoking the move callback.
  //
  //
  // @param ctx         slab release context
  // @param item        old item to be moved elsewhere
  // @param throttler   slow this function down as not to take too much cpu
  //
  // @return    true  if the item has been moved
  //            false if we have exhausted moving attempts
  bool moveForSlabRelease(const SlabReleaseContext& ctx,
                          Item& item,
                          util::Throttler& throttler);

  // "Move" (by copying) the content in this item to another memory
  // location by invoking the move callback.
  //
  // @param item         old item to be moved elsewhere
  // @param newItemHdl   handle of new item to be moved into
  //
  // @return    true  if the item has been moved
  //            false if we have exhausted moving attempts
  bool tryMovingForSlabRelease(Item& item, WriteHandle& newItemHdl);

  // Evict an item from access and mm containers and
  // ensure it is safe for freeing.
  //
  // @param ctx         slab release context
  // @param item        old item to be moved elsewhere
  // @param throttler   slow this function down as not to take too much cpu
  void evictForSlabRelease(const SlabReleaseContext& ctx,
                           Item& item,
                           util::Throttler& throttler);

  // Helper function to evict a normal item for slab release
  //
  // @return last handle for corresponding to item on success. empty handle on
  // failure. caller can retry if needed.
  WriteHandle evictNormalItemForSlabRelease(Item& item);

  // Helper function to evict a child item for slab release
  // As a side effect, the parent item is also evicted
  //
  // @return  last handle to the parent item of the child on success. empty
  // handle on failure. caller can retry.
  WriteHandle evictChainedItemForSlabRelease(ChainedItem& item);

  // Helper function to remove a item if expired.
  //
  // @return true if it item expire and removed successfully.
  bool removeIfExpired(const ReadHandle& handle);

  // exposed for the Reaper to iterate through the memory and find items to
  // reap under the super charged mode. This is faster if there are lots of
  // items in cache and only a small fraction of them are expired at any given
  // time.
  template <typename Fn>
  void traverseAndExpireItems(Fn&& f) {
    // The intent here is to scan the memory to identify candidates for reaping
    // without holding any locks. Candidates that are identified as potential
    // ones are further processed by holding the right synchronization
    // primitives. So we consciously exempt ourselves here from TSAN data race
    // detection.
    folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
    auto slabsSkipped = allocator_->forEachAllocation(std::forward<Fn>(f));
    stats().numReaperSkippedSlabs.add(slabsSkipped);
  }

  // returns true if nvmcache is enabled and we should write this item to
  // nvmcache.
  bool shouldWriteToNvmCache(const Item& item);

  // (this should be only called when we're the only thread accessing item)
  // returns true if nvmcache is enabled and we should write this item.
  bool shouldWriteToNvmCacheExclusive(const Item& item);

  // Serialize the metadata for the cache into an IOBUf. The caller can now
  // use this to serialize into a serializer by estimating the size and
  // calling writeToBuffer.
  folly::IOBufQueue saveStateToIOBuf();

  static typename MemoryAllocator::Config getAllocatorConfig(
      const Config& config) {
    return MemoryAllocator::Config{
        config.defaultAllocSizes.empty()
            ? util::generateAllocSizes(
                  config.allocationClassSizeFactor,
                  config.maxAllocationClassSize,
                  config.minAllocationClassSize,
                  config.reduceFragmentationInAllocationClass)
            : config.defaultAllocSizes,
        config.enableZeroedSlabAllocs, config.disableFullCoredump,
        config.lockMemory};
  }

  // starts one of the cache workers passing the current instance and the args
  template <typename T, typename... Args>
  bool startNewWorker(folly::StringPiece name,
                      std::unique_ptr<T>& worker,
                      std::chrono::milliseconds interval,
                      Args&&... args);

  // stops one of the workers belonging to this instance.
  template <typename T>
  bool stopWorker(folly::StringPiece name,
                  std::unique_ptr<T>& worker,
                  std::chrono::seconds timeout = std::chrono::seconds{0});

  ShmSegmentOpts createShmCacheOpts();
  std::unique_ptr<MemoryAllocator> createNewMemoryAllocator();
  std::unique_ptr<MemoryAllocator> restoreMemoryAllocator();
  std::unique_ptr<CCacheManager> restoreCCacheManager();

  PoolIds filterCompactCachePools(const PoolIds& poolIds) const;

  // returns a list of pools excluding compact cache pools that are over the
  // limit
  PoolIds getRegularPoolIdsForResize() const override final;

  // return whether a pool participates in auto-resizing
  bool autoResizeEnabledForPool(PoolId) const override final;

  // resize all compact caches to their configured size
  void resizeCompactCaches() override;

  std::map<std::string, std::string> serializeConfigParams()
      const override final {
    return config_.serialize();
  }

  typename Item::PtrCompressor createPtrCompressor() const {
    return allocator_->createPtrCompressor<Item>();
  }

  // helper utility to throttle and optionally log.
  static void throttleWith(util::Throttler& t, std::function<void()> fn);

  // Write the item to nvm cache if enabled. This is called on eviction.
  void pushToNvmCache(const Item& item);

  // Test utility function to move a key from ram into nvmcache.
  bool pushToNvmCacheFromRamForTesting(Key key);
  // Test utility functions to remove things from individual caches.
  bool removeFromRamForTesting(Key key);
  void removeFromNvmForTesting(Key key);

  // @param dramCacheAttached   boolean indicating if the dram cache was
  //                            restored from previous state
  void initCommon(bool dramCacheAttached);
  void initNvmCache(bool dramCacheAttached);
  void initWorkers();

  // @param type        the type of initialization
  // @return nullptr if the type is invalid
  // @return pointer to memory allocator
  // @throw std::runtime_error if type is invalid
  std::unique_ptr<MemoryAllocator> initAllocator(InitMemType type);
  // @param type        the type of initialization
  // @return nullptr if the type is invalid
  // @return pointer to access container
  // @throw std::runtime_error if type is invalid
  std::unique_ptr<AccessContainer> initAccessContainer(InitMemType type,
                                                       const std::string name,
                                                       AccessConfig config);

  std::optional<bool> saveNvmCache();
  void saveRamCache();

  static bool itemExclusivePredicate(const Item& item) {
    return item.getRefCount() == 0;
  }

  static bool itemExpiryPredicate(const Item& item) {
    return item.getRefCount() == 1 && item.isExpired();
  }

  static bool parentEvictForSlabReleasePredicate(const Item& item) {
    return item.getRefCount() == 1 && !item.isMoving();
  }

  std::unique_ptr<Deserializer> createDeserializer();

  // Execute func on each item. `func` can throw exception but must ensure
  // the item remains in a valid state
  //
  // @param item      Parent item
  // @param func      Function that gets executed on each chained item
  void forEachChainedItem(const Item& item,
                          std::function<void(ChainedItem&)> func);

  // @param item    Record the item has been accessed in its mmContainer
  // @param mode    the mode of access
  // @param stats   stats object to avoid a thread local lookup.
  // @return true   if successfully recorded in MMContainer
  bool recordAccessInMMContainer(Item& item, AccessMode mode);

  WriteHandle findChainedItem(const Item& parent) const;

  // Get the thread local version of the Stats
  detail::Stats& stats() const noexcept { return stats_; }

  void initStats();

  // return a read-only iterator to the item's chained allocations. The order of
  // iteration on the item will be LIFO of the addChainedItem calls.
  folly::Range<ChainedItemIter> viewAsChainedAllocsRange(
      const Item& parent) const;

  // updates the maxWriteRate for DynamicRandomAdmissionPolicy
  // returns true if update successfully
  //         false if no AdmissionPolicy is set or it is not DynamicRandom
  bool updateMaxRateForDynamicRandomAP(uint64_t maxRate) {
    return nvmCache_ ? nvmCache_->updateMaxRateForDynamicRandomAP(maxRate)
                     : false;
  }

  // BEGIN private members

  // Whether the memory allocator for this cache allocator was created on shared
  // memory. The hash table, chained item hash table etc is also created on
  // shared memory except for temporary shared memory mode when they're created
  // on heap.
  const bool isOnShm_{false};

  Config config_{};

  // Manages the temporary shared memory segment for memory allocator that
  // is not persisted when cache process exits.
  std::unique_ptr<TempShmMapping> tempShm_;

  std::unique_ptr<ShmManager> shmManager_;

  // Deserialize data to restore cache allocator. Used only while attaching to
  // existing shared memory.
  std::unique_ptr<Deserializer> deserializer_;

  // used only while attaching to existing shared memory.
  serialization::CacheAllocatorMetadata metadata_{};

  // configs for the access container and the mm container.
  const MMConfig mmConfig_{};

  // the memory allocator for allocating out of the available memory.
  std::unique_ptr<MemoryAllocator> allocator_;

  // compact cache allocator manager
  std::unique_ptr<CCacheManager> compactCacheManager_;

  // compact cache instances reside here when user "add" or "attach" compact
  // caches. The lifetime is tied to CacheAllocator.
  // The bool is an indicator for whether the compact cache participates in
  // size optimization in PoolOptimizer
  std::unordered_map<PoolId, std::unique_ptr<ICompactCache>> compactCaches_;

  // check whether a pool is enabled for optimizing
  std::array<std::atomic<bool>, MemoryPoolManager::kMaxPools>
      optimizerEnabled_{};

  // ptr compressor
  typename Item::PtrCompressor compressor_;

  // Lock to synchronize addition of a new pool and its resizing/rebalancing
  folly::SharedMutex poolsResizeAndRebalanceLock_;

  // container for the allocations which are currently being memory managed by
  // the cache allocator.
  // we need mmcontainer per allocator pool/allocation class.
  MMContainers mmContainers_;

  // container that is used for accessing the allocations by their key.
  std::unique_ptr<AccessContainer> accessContainer_;

  // container that is used for accessing the chained allocation
  std::unique_ptr<AccessContainer> chainedItemAccessContainer_{nullptr};

  friend ChainedAllocs;
  friend WritableChainedAllocs;
  // ensure any modification to a chain of chained items are synchronized
  using ChainedItemLock = facebook::cachelib::SharedMutexBuckets;
  ChainedItemLock chainedItemLocks_;

  // nvmCache
  std::unique_ptr<NvmCacheT> nvmCache_;

  // rebalancer for the pools
  std::unique_ptr<PoolRebalancer> poolRebalancer_;

  // resizer for the pools.
  std::unique_ptr<PoolResizer> poolResizer_;

  // automatic arena resizing i.e. pool optimization
  std::unique_ptr<PoolOptimizer> poolOptimizer_;

  // free memory monitor
  std::unique_ptr<MemoryMonitor> memMonitor_;

  // check whether a pool is a slabs pool
  std::array<bool, MemoryPoolManager::kMaxPools> isCompactCachePool_{};

  // lock to serilize access of isCompactCachePool_ array, including creation of
  // compact cache pools
  folly::SharedMutex compactCachePoolsLock_;

  // mutex protecting the creation and destruction of workers poolRebalancer_,
  // poolResizer_, poolOptimizer_, memMonitor_, reaper_
  mutable std::mutex workersMutex_;

  // time when the ram cache was first created
  const uint32_t cacheCreationTime_{0};

  // time when CacheAllocator structure is created. Whenever a process restarts
  // and even if cache content is persisted, this will be reset. It's similar
  // to process uptime. (But alternatively if user explicitly shuts down and
  // re-attach cache, this will be reset as well)
  const uint32_t cacheInstanceCreationTime_{0};

  // thread local accumulation of handle counts
  mutable util::FastStats<int64_t> handleCount_{};

  mutable detail::Stats stats_{};
  // allocator's items reaper to evict expired items in bg checking
  std::unique_ptr<Reaper<CacheT>> reaper_;

  class DummyTlsActiveItemRingTag {};
  folly::ThreadLocal<TlsActiveItemRing, DummyTlsActiveItemRingTag> ring_;

  // state for the nvmcache
  NvmCacheState nvmCacheState_;

  // admission policy for nvmcache
  std::shared_ptr<NvmAdmissionPolicy<CacheT>> nvmAdmissionPolicy_;

  // indicates if the shutdown of cache is in progress or not
  std::atomic<bool> shutDownInProgress_{false};

  // END private members

  // Make this friend to give access to acquire and release
  friend ReadHandle;
  friend ReaperAPIWrapper<CacheT>;
  friend class CacheAPIWrapperForNvm<CacheT>;
  friend class FbInternalRuntimeUpdateWrapper<CacheT>;
  friend class objcache2::ObjectCache<CacheT>;
  friend class objcache2::ObjectCacheBase<CacheT>;
  template <typename K, typename V, typename C>
  friend class ReadOnlyMap;

  // tests
  friend class facebook::cachelib::tests::NvmCacheTest;
  FRIEND_TEST(CachelibAdminTest, WorkingSetAnalysisLoggingTest);
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::BaseAllocatorTest;
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::AllocatorHitStatsTest;
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::AllocatorResizeTest;
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::PoolOptimizeStrategyTest;
  friend class facebook::cachelib::tests::NvmAdmissionPolicyTest;
  friend class facebook::cachelib::tests::CacheAllocatorTestWrapper;
  friend class facebook::cachelib::tests::PersistenceCache;
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::FixedSizeArrayTest;
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::MapTest;

  // benchmarks
  template <typename Allocator>
  friend class facebook::cachelib::cachebench::Cache;
  friend class facebook::cachelib::cachebench::tests::CacheTest;
  friend void lookupCachelibBufferManagerOnly();
  friend void lookupCachelibMap();
  friend void benchCachelibMap();
  friend void benchCachelibRangeMap();

  // objectCache
  template <typename CacheDescriptor, typename AllocatorRes>
  friend class facebook::cachelib::objcache::ObjectCache;
  friend class GET_DECORATED_CLASS_NAME(objcache::test,
                                        ObjectCache,
                                        ObjectHandleInvalid);
};
} // namespace cachelib
} // namespace facebook
#include "cachelib/allocator/CacheAllocator-inl.h"

namespace facebook {
namespace cachelib {

// Declare templates ahead of use to reduce compilation time
extern template class CacheAllocator<LruCacheTrait>;
extern template class CacheAllocator<LruCacheWithSpinBucketsTrait>;
extern template class CacheAllocator<Lru2QCacheTrait>;
extern template class CacheAllocator<TinyLFUCacheTrait>;

// CacheAllocator with an LRU eviction policy
// LRU policy can be configured to act as a segmented LRU as well
using LruAllocator = CacheAllocator<LruCacheTrait>;
using LruAllocatorSpinBuckets = CacheAllocator<LruCacheWithSpinBucketsTrait>;

// CacheAllocator with 2Q eviction policy
// Hot, Warm, Cold queues are maintained
// Item Life Time:
//  0. On access, each item is promoted to the head of its current
//  queue
//  1. first enter Hot queue
//  2. if accessed while in Hot, item will qualify entry to Warm queue
//     otherwise, item will enter cold queue
//  3. items in cold queue are evicted to make room for new items
using Lru2QAllocator = CacheAllocator<Lru2QCacheTrait>;

// CacheAllocator with Tiny LFU eviction policy
// It has a window initially to gauage the frequency of accesses of newly
// inserted items. And eventually it will onl admit items that are accessed
// beyond a threshold into the warm cache.
using TinyLFUAllocator = CacheAllocator<TinyLFUCacheTrait>;
} // namespace cachelib
} // namespace facebook
