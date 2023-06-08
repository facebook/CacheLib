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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/dynamic.h>
#include <folly/hash/Hash.h>
#include <folly/json.h>
#include <folly/synchronization/Baton.h>

#include <array>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "cachelib/allocator/nvmcache/CacheApiWrapper.h"
#include "cachelib/allocator/nvmcache/InFlightPuts.h"
#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/allocator/nvmcache/NavySetup.h"
#include "cachelib/allocator/nvmcache/NvmItem.h"
#include "cachelib/allocator/nvmcache/ReqContexts.h"
#include "cachelib/allocator/nvmcache/TombStones.h"
#include "cachelib/allocator/nvmcache/WaitContext.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/EventInterface.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Utils.h"
#include "cachelib/navy/common/Device.h"
#include "folly/Range.h"

namespace facebook {
namespace cachelib {
namespace tests {
class NvmCacheTest;
}

// NvmCache is a key-value cache on flash. It is intended to be used
// along with a CacheAllocator to provide a uniform API to the application
// by abstracting away the details of ram and flash management.
template <typename C>
class NvmCache {
 public:
  using Item = typename C::Item;
  using ChainedItem = typename Item::ChainedItem;
  using ChainedItemIter = typename C::ChainedItemIter;
  using ItemDestructor = typename C::ItemDestructor;
  using DestructorData = typename C::DestructorData;
  using SampleItem = typename C::SampleItem;

  // Context passed in encodeCb or decodeCb. If the item has children,
  // they are passed in the form of a folly::Range.
  // Chained items must be iterated though @chainedItemRange.
  // Other APIs used to access chained items are not compatible and should not
  // be used.
  struct EncodeDecodeContext {
    Item& item;
    folly::Range<ChainedItemIter> chainedItemRange;
  };

  // call back to encode an item to prepare for nvmcache if needed
  // @param it      item we are writing to nvmcache
  // @param allocs  pointer to chained allocs if the item has chained allocs
  // @return true if the item can be written. false if we need to fail
  using EncodeCB = std::function<bool(EncodeDecodeContext)>;

  // same as the above, but reverses the item back to its expected state if
  // needed.
  using DecodeCB = std::function<void(EncodeDecodeContext)>;

  // encrypt everything written to the device and decrypt on every read
  using DeviceEncryptor = navy::DeviceEncryptor;

  using WriteHandle = typename C::WriteHandle;
  using ReadHandle = typename C::ReadHandle;
  using DeleteTombStoneGuard = typename TombStones::Guard;
  using PutToken = typename InFlightPuts::PutToken;

  struct Config {
    navy::NavyConfig navyConfig{};

    // (Optional) enables the user to change some bits to prepare the item for
    // nvmcache serialization like fixing up pointers etc. Both must be encode
    // and decode must be specified or NOT specified. Encode and decode happen
    // before encryption and after decryption respectively if enabled.
    EncodeCB encodeCb{};
    DecodeCB decodeCb{};

    // (Optional) This enables encryption on a device level. Everything we write
    // into the nvm device will be encrypted.
    std::shared_ptr<navy::DeviceEncryptor> deviceEncryptor{};

    // Whether or not store full alloc-class sizes into NVM device.
    // If true, only store the orignal size the user requested.
    bool truncateItemToOriginalAllocSizeInNvm{false};

    // when enabled, nvmcache will attempt to resolve misses without incurring
    // thread hops by using synchronous methods.
    bool enableFastNegativeLookups{true};

    // serialize the config for debugging purposes
    std::map<std::string, std::string> serialize() const;

    // validate the config, set any defaults that were not set and  return a
    // copy.
    //
    // @throw std::invalid_argument if encoding/decoding/encryption is
    // incompatible.
    Config validateAndSetDefaults();
  };

  // @param c         the cache instance using nvmcache
  // @param config    the config for nvmcache
  // @param truncate  if we should truncate the nvmcache store
  NvmCache(C& c,
           Config config,
           bool truncate,
           const ItemDestructor& itemDestructor);

  // Look up item by key
  // @param key         key to lookup
  // @return            WriteHandle
  WriteHandle find(HashedKey key);

  // Returns true if a key is potentially in cache. There is a non-zero chance
  // the key does not exist in cache (e.g. hash collision in NvmCache). This
  // check is meant to be synchronous and fast as we only check DRAM cache and
  // in-memory index for NvmCache.
  //
  // @param key   the key for lookup
  // @return      true if the key could exist, false otherwise
  bool couldExistFast(HashedKey key);

  // Try to mark the key as in process of being evicted from RAM to NVM.
  // This is used to maintain the consistency between the RAM cache and
  // NvmCache when an item is transitioning from RAM to NvmCache in the
  // presence of concurrent gets and deletes from other threads.
  //
  // @param key         the key which is being evicted
  // @return            return true if successfully marked and the eviction
  //                    can proceed to queue a put in the future.  return
  //                    false otherwise
  PutToken createPutToken(folly::StringPiece key);

  // store the given item in navy
  // @param hdl         handle to cache item. should not be null
  // @param token       the put token for the item. this must have been
  //                    obtained before enqueueing the put to maintain
  //                    consistency
  void put(WriteHandle& hdl, PutToken token);

  // returns the current state of whether nvmcache is enabled or not. nvmcache
  // can be disabled if the backend implementation ends up in a corrupt state
  bool isEnabled() const noexcept { return navyEnabled_; }

  // creates a delete tombstone for the key. This will ensure that all
  // concurrent gets and puts to nvmcache can synchronize with an upcoming
  // delete to make the cache consistent.
  DeleteTombStoneGuard createDeleteTombStone(HashedKey hk);

  // remove an item by key
  // @param hk          key to remove with hash
  // @param tombstone   the tombstone guard associated for this key. See
  //                    CacheAllocator::remove on how tombstone maintain
  //                    consistency with presence of concurrent get/puts to the
  //                    same key from nvmcache.Tombstone is kept alive and
  //                    destroyed until the remove operation is finished
  //                    processing asynchronously.
  //                    The tombstone should be created by
  //                    'createDeleteTombStone', it can be created right before
  //                    the remove called if caller is only removing item in
  //                    nvm; if the caller is also removing the item from ram,
  //                    tombstone should be created before removing item in ram.
  void remove(HashedKey hk, DeleteTombStoneGuard tombstone);

  // peek the nvmcache without bringing the item into the cache. creates a
  // temporary item handle with the content of the nvmcache. this is intended
  // for debugging purposes
  //
  // @param key   the key for the cache item
  // @return    handle to the item in nvmcache if present. if not, nullptr is
  //            returned. if a handle is returned, it is not inserted into
  //            cache and is temporary.
  WriteHandle peek(folly::StringPiece key);

  SampleItem getSampleItem();

  // safely shut down the cache. must be called after stopping all concurrent
  // access to cache. using nvmcache after this will result in no-op.
  // Returns true if shutdown was performed properly, false otherwise.
  bool shutDown();

  // blocks until all in-flight ops are flushed to the device. To be used when
  // there are no more operations being enqueued.
  void flushPendingOps();

  // Obtain stats in a <string -> double> representation.
  util::StatsMap getStatsMap() const;

  // returns the size of the NVM device
  size_t getSize() const noexcept { return navyCache_->getSize(); }

  // returns the size of the NVM space used for caching
  size_t getUsableSize() const noexcept { return navyCache_->getUsableSize(); }

  bool updateMaxRateForDynamicRandomAP(uint64_t maxRate) {
    return navyCache_->updateMaxRateForDynamicRandomAP(maxRate);
  }

  // This lock is to protect concurrent NvmCache evictCB and CacheAllocator
  // remove/insertOrReplace/invalidateNvm.
  // This lock scope within the above functions is
  // 1. check DRAM visibility in NvmCache evictCB
  // 2. modify DRAM visibility in CacheAllocator remove/insertOrReplace
  // 3. check/modify NvmClean, NvmEvicted flag
  // 4. check/modify itemRemoved_ set
  // The lock ensures that the items in itemRemoved_ must exist in nvm, and nvm
  // eviction must erase item from itemRemoved_, so there won't memory leak or
  // influence to future item with same key.
  std::unique_lock<std::mutex> getItemDestructorLock(HashedKey hk) const {
    using LockType = std::unique_lock<std::mutex>;
    return itemDestructor_ ? LockType{itemDestructorMutex_[getShardForKey(hk)]}
                           : LockType{};
  }

  // For items with this key that are present in NVM, mark the DRAM to be the
  // authoritative copy for destructor events. This is usually done when items
  // are in-place mutated/removed/replaced and the nvm copy is being
  // invalidated by calling NvmCache::remove subsequently.
  //
  // caller must make sure itemDestructorLock is locked,
  // and the item is present in NVM (NvmClean set and NvmEvicted flag unset).
  void markNvmItemRemovedLocked(HashedKey hk);

 private:
  // returns the itemRemoved_ set size
  // it is the number of items were both in dram and nvm
  // and were removed from dram but not yet removed from nvm
  uint64_t getNvmItemRemovedSize() const;

  bool checkAndUnmarkItemRemovedLocked(HashedKey hk);

  detail::Stats& stats() { return CacheAPIWrapperForNvm<C>::getStats(cache_); }

  // creates the RAM item from NvmItem.
  //
  // @param key   key for the nvm item
  // @param nvmItem contents for the key
  //
  // @param   return an item handle allocated and initialized to the right state
  //          based on the NvmItem
  WriteHandle createItem(folly::StringPiece key, const NvmItem& nvmItem);

  // creates the item into IOBuf from NvmItem, if the item has chained items,
  // chained IOBufs will be created.
  // @param key   key for the dipper item
  // @param nvmItem contents for the key
  // @param parentOnly create item and IOBuf only for the parent item
  //
  // @return an IOBuf allocated for the item and initialized the memory to Item
  //          based on the NvmItem
  std::unique_ptr<folly::IOBuf> createItemAsIOBuf(folly::StringPiece key,
                                                  const NvmItem& nvmItem,
                                                  bool parentOnly = false);
  // Returns an iterator to the item's chained IOBufs. The order of
  // iteration on the item will be LIFO of the addChainedItem calls.
  // This is only used when we have to create cache items on heap (IOBuf) for
  // the purpose of ItemDestructor.
  folly::Range<ChainedItemIter> viewAsChainedAllocsRange(folly::IOBuf*) const;

  // returns true if there is tombstone entry for the key.
  bool hasTombStone(HashedKey hk);

  std::unique_ptr<NvmItem> makeNvmItem(const WriteHandle& handle);

  // wrap an item into a blob for writing into navy.
  Blob makeBlob(const Item& it);
  uint32_t getStorageSizeInNvm(const Item& it);

  // Holds all the necessary data to do an async navy get
  // All of the supported operations aren't thread safe. The caller
  // needs to ensure thread safety
  struct GetCtx {
    NvmCache& cache;       //< the NvmCache instance
    const std::string key; //< key being fetched
    std::vector<std::shared_ptr<WaitContext<ReadHandle>>> waiters; // list of
                                                                   // waiters
    WriteHandle it; // will be set when Context is being filled
    util::LatencyTracker tracker_;
    bool valid_;

    GetCtx(NvmCache& c,
           folly::StringPiece k,
           std::shared_ptr<WaitContext<ReadHandle>> ctx,
           util::LatencyTracker tracker)
        : cache(c),
          key(k.toString()),
          tracker_(std::move(tracker)),
          valid_(true) {
      it.markWentToNvm();
      addWaiter(std::move(ctx));
    }

    ~GetCtx() {
      // prevent any further enqueue to waiters
      // Note: we don't need to hold locks since no one can enqueue
      // after this point.
      wakeUpWaiters();
    }

    // @return  key as StringPiece
    folly::StringPiece getKey() const { return {key.data(), key.length()}; }

    // record the item handle. Upon destruction we will wake up the waiters
    // and pass a clone of the handle to the callBack. By default we pass
    // a null handle
    void setWriteHandle(WriteHandle _it) { it = std::move(_it); }

    // enqueue a waiter into the waiter list
    // @param  waiter       WaitContext
    void addWaiter(std::shared_ptr<WaitContext<ReadHandle>> waiter) {
      XDCHECK(waiter);
      waiters.push_back(std::move(waiter));
    }

    // notify all pending waiters that are waiting for the fetch.
    void wakeUpWaiters() {
      bool refcountOverflowed = false;
      for (auto& w : waiters) {
        // If refcount overflowed earlier, then we will return miss to
        // all subsequent waitors.
        if (refcountOverflowed) {
          w->set(WriteHandle{});
          continue;
        }

        try {
          w->set(it.clone());
        } catch (const exception::RefcountOverflow&) {
          // We'll return a miss to the user's pending read,
          // so we should enqueue a delete via NvmCache.
          cache.cache_.remove(it);
          refcountOverflowed = true;
        }
      }
    }

    void invalidate() { valid_ = false; }

    bool isValid() const { return valid_; }
  };

  // Erase entry for the ctx from the fill map
  // @param     ctx   ctx to erase
  void removeFromFillMap(HashedKey hk) {
    std::unique_ptr<GetCtx> to_delete;
    {
      auto lock = getFillLock(hk);
      auto& map = getFillMap(hk);
      auto it = map.find(hk.key());
      if (it == map.end()) {
        return;
      }
      to_delete = std::move(it->second);
      map.erase(it);
    }
  }

  // Erase entry for the ctx from the fill map
  // @param     key   item key
  void invalidateFill(HashedKey hk) {
    auto shard = getShardForKey(hk);
    auto lock = getFillLockForShard(shard);
    auto& map = getFillMapForShard(shard);
    auto it = map.find(hk.key());
    if (it != map.end() && it->second) {
      it->second->invalidate();
    }
  }

  // Logs and disables navy usage
  void disableNavy(const std::string& msg);

  // map of concurrent fills by key. The key is a string piece wrapper around
  // GetCtx's std::string. This makes the lookups possible without
  // constructing a string key.
  using FillMap =
      folly::F14ValueMap<folly::StringPiece, std::unique_ptr<GetCtx>>;

  static size_t getShardForKey(HashedKey hk) { return hk.keyHash() % kShards; }

  static size_t getShardForKey(folly::StringPiece key) {
    return getShardForKey(HashedKey{key});
  }

  FillMap& getFillMapForShard(size_t shard) { return fills_[shard].fills_; }

  FillMap& getFillMap(HashedKey hk) {
    return getFillMapForShard(getShardForKey(hk));
  }

  std::unique_lock<std::mutex> getFillLockForShard(size_t shard) {
    return std::unique_lock<std::mutex>(fillLock_[shard].fillLock_);
  }

  std::unique_lock<std::mutex> getFillLock(HashedKey hk) {
    return getFillLockForShard(getShardForKey(hk));
  }

  void onGetComplete(GetCtx& ctx,
                     navy::Status s,
                     HashedKey key,
                     navy::BufferView value);

  void evictCB(HashedKey hk, navy::BufferView val, navy::DestructorEvent e);

  static navy::BufferView makeBufferView(folly::ByteRange b) {
    return navy::BufferView{b.size(), b.data()};
  }

  const Config config_;
  C& cache_;                            //< cache allocator
  std::atomic<bool> navyEnabled_{true}; //< switch to turn off/on navy

  static constexpr size_t kShards = 8192;

  // a map of all pending fills to prevent thundering herds
  struct {
    alignas(folly::hardware_destructive_interference_size) FillMap fills_;
  } fills_[kShards];

  // a map of fill locks for each shard
  struct {
    alignas(folly::hardware_destructive_interference_size) std::mutex fillLock_;
  } fillLock_[kShards];

  // currently queued put operations to navy.
  std::array<PutContexts, kShards> putContexts_;

  // currently queued delete operations to navy.
  std::array<DelContexts, kShards> delContexts_;

  // co-ordination between in-flight evictions from cache that are not queued
  // to navy and in-flight gets into nvmcache that are not yet queued.
  std::array<InFlightPuts, kShards> inflightPuts_;
  std::array<TombStones, kShards> tombstones_;

  const ItemDestructor itemDestructor_;

  mutable std::array<std::mutex, kShards> itemDestructorMutex_;
  // Used to track the keys of items present in NVM that should be excluded for
  // executing Destructor upon eviction from NVM, if the item is not present in
  // DRAM. The ownership of item destructor is already managed elsewhere for
  // these keys. This data struct is updated prior to issueing NvmCache::remove
  // to handle any racy eviction from NVM before the NvmCache::remove is
  // finished.
  std::array<folly::F14FastSet<std::string>, kShards> itemRemoved_;

  std::unique_ptr<cachelib::navy::AbstractCache> navyCache_;

  friend class tests::NvmCacheTest;
  FRIEND_TEST(CachelibAdminTest, WorkingSetAnalysisLoggingTest);
};

} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/nvmcache/NvmCache-inl.h"
