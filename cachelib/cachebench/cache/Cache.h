/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <folly/hash/Hash.h>
#include <gflags/gflags.h>

#include <atomic>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/HitsPerSlabStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/RandomStrategy.h"
#include "cachelib/cachebench/cache/CacheStats.h"
#include "cachelib/cachebench/cache/CacheValue.h"
#include "cachelib/cachebench/cache/ItemRecords.h"
#include "cachelib/cachebench/cache/TimeStampTicker.h"
#include "cachelib/cachebench/consistency/LogEventStream.h"
#include "cachelib/cachebench/consistency/ValueTracker.h"
#include "cachelib/cachebench/util/CacheConfig.h"

DECLARE_bool(report_api_latency);

namespace facebook {
namespace cachelib {
namespace cachebench {

// A specialized Cache for cachebench use, backed by Cachelib.
// Items value in this cache follows CacheValue schema, which
// contains a few integers for sanity checks use. So it is invalid
// to use item.getMemory and item.getSize APIs directly and caller must use
// getMemory() through this cache instance.
template <typename Allocator>
class Cache {
 public:
  using Item = typename Allocator::Item;
  using ItemHandle = typename Allocator::ItemHandle;
  using Key = typename Item::Key;
  using RemoveRes = typename Allocator::RemoveRes;
  using SyncObj = typename Allocator::SyncObj;
  using ChainedItemMovingSync = typename Allocator::ChainedItemMovingSync;
  using ChainedAllocs = typename Allocator::ChainedAllocs;

  template <typename U>
  using TypedHandle = LruAllocator::TypedHandle<U>;

  // instantiate a cachelib cache instance.
  //
  // @param config        the configuration for the cache.
  // @param movingSync    workload specific moving sync for constructing the
  //                      cache.
  // @param cacheDir      optional directory for the cache to enable
  //                      persistence across restarts.
  explicit Cache(const CacheConfig& config,
                 ChainedItemMovingSync movingSync = {},
                 std::string cacheDir = "");

  ~Cache();

  // allocate an item from the cache.
  //
  // @param pid     the pool id to allocate from
  // @param key     the key for the item
  // @param size    size of the item
  // @param ttlSecs ttl for the item in seconds (optional)
  ItemHandle allocate(PoolId pid,
                      folly::StringPiece key,
                      size_t size,
                      uint32_t ttlSecs = 0);

  // inserts the item into the cache and tracks it.
  ItemHandle insertOrReplace(ItemHandle& handle);

  // inserts the handle into cache and returns true if the insert was
  // successful, false otherwise. Insert operation can not be performed when
  // consistency checking is enabled.
  bool insert(ItemHandle& handle);

  // perform lookup in the cache and if consistency checking is enabled,
  // ensure that the lookup result is consistent with the past actions and
  // concurrent actions. If NVM is enabled, waits for the Item to become ready
  // before returning. If key is found to be inconsistent, it is marked as
  // invalid.
  //
  // @param key   the key for lookup
  // @param mode  the access mode
  //
  // @return handle for the item if present or null handle.
  ItemHandle find(Key key, AccessMode mode = AccessMode::kRead);

  // removes the key from the cache and returns corresponding cache result.
  // Tracks the operation for checking consistency if enabled.
  RemoveRes remove(Key key);
  // same as above, but takes an item handle.
  RemoveRes remove(const ItemHandle& it);

  // allocates a chained item for this parent.
  //
  // @param parent   the parent for the chain
  // @param size     the size of the allocation
  //
  // @return item handle allocated.
  ItemHandle allocateChainedItem(const ItemHandle& parent, size_t size);

  // replace the oldItem belonging to the parent with a new one.
  // @param oldItem         the item being replaced
  // @param newItemHandle   handle to the new item
  // @param parent          the parent item
  //
  // @return handle to the item that was replaced.
  ItemHandle replaceChainedItem(Item& oldItem,
                                ItemHandle newItemHandle,
                                Item& parent);

  // Adds a chained item to the parent.
  // @param  parent   the parent item's handle
  // @param child     handle to the child
  void addChainedItem(ItemHandle& parent, ItemHandle child);

  template <typename... Params>
  auto viewAsChainedAllocs(Params&&... args) {
    return cache_->viewAsChainedAllocs(std::forward<Params>(args)...);
  }

  // Item specific accessors for the cache. This is needed since cachebench's
  // cache adds some overheads on top of Cache::Item.

  // Return the readonly memory
  const void* getMemory(const ItemHandle& handle) const noexcept {
    return handle == nullptr ? nullptr : getMemory(*handle);
  }

  // Return the writable memory
  void* getMemory(ItemHandle& handle) noexcept {
    return handle == nullptr ? nullptr : getMemory(*handle);
  }

  // Return the readonly memory
  const void* getMemory(const Item& item) const noexcept {
    return item.template getMemoryAs<CacheValue>()->getData();
  }

  // Return the writable memory
  void* getMemory(Item& item) noexcept {
    return item.template getMemoryAs<CacheValue>()->getData();
  }

  // return the allocation size for the item.
  uint32_t getSize(const ItemHandle& item) const noexcept {
    return getSize(item.get());
  }

  // returns the size of the item, taking into account ItemRecords could be
  // enabled.
  uint32_t getSize(const Item* item) const noexcept;

  // set's a random value for the item when consistency check is enabled.
  void setUint64ToItem(ItemHandle& handle, uint64_t num) const;

  // For chained items, tracks the state of the chain by using the combination
  // of unique checksum value per item in chain into one value.
  void trackChainChecksum(const ItemHandle& handle);

  // set's the string value to the item, stripping the tail if the input
  // string is longer than the item's storage space.
  //
  // @param handle   the handle for the item
  // @param str      the string value to be set.
  void setStringItem(ItemHandle& handle, const std::string& str);

  // when item records are enabled, updates the version for the item and
  // correspondingly invalidates the nvm cache.
  void updateItemRecordVersion(ItemHandle& it);

  // the following three are helper functions to support the cachelib map
  // integration stress tests. These expose the same interface as
  // CacheAllocator. TODO (sathya) expose the underlying cache instance
  // instead of forwarding the apis.
  template <typename... Params>
  auto getUsableSize(Params&&... args) {
    return cache_->getUsableSize(std::forward<Params>(args)...);
  }

  template <typename... Params>
  auto getAllocInfo(Params&&... args) {
    return cache_->getAllocInfo(std::forward<Params>(args)...);
  }

  template <typename... Params>
  const auto& getPool(Params&&... args) {
    return cache_->getPool(std::forward<Params>(args)...);
  }

  // true if the cache only uses DRAM. false if the cache is configured to
  // have NVM as well (even if it is mocked by DRAM underneath).
  bool isRamOnly() const { return config_.nvmCacheSizeMB == 0; }

  // if NVM is being used, checks if there were any errors that could have
  // disable the NVM engine.
  bool isNvmCacheDisabled() const {
    return !isRamOnly() && !cache_->isNvmCacheEnabled();
  }

  // enables consistency checking for the cache. This should be done before
  // any find/insert/remove is called.
  //
  // @param keys  list of keys that the stressor uses for the workload.
  void enableConsistencyCheck(const std::vector<std::string>& keys);

  // returns true if the consistency checking is enabled.
  bool consistencyCheckEnabled() const { return valueTracker_ != nullptr; }

  // return true if the key was previously detected to be inconsistent. This
  // is useful only when consistency checking is enabled by calling
  // enableConsistencyCheck()
  bool isInvalidKey(const std::string& key) {
    return invalidKeys_[key].load(std::memory_order_relaxed);
  }

  // Get overall stats on the whole cache allocator
  Stats getStats() const;

  // return the stats for the pool.
  PoolStats getPoolStats(PoolId pid) const { return cache_->getPoolStats(pid); }

  // return the total number of inconsistent operations detected since start.
  unsigned int getInconsistencyCount() const {
    return inconsistencyCount_.load(std::memory_order_relaxed);
  }

  // return the number of times Item destructor was called inconsistently.
  uint64_t getInvalidDestructorCount() { return invalidDestructor_; }

  // return the number of pools the cache was configured with.
  uint64_t numPools() const noexcept { return config_.numPools; }

  // reports the number of handles held by the thread calling this api.
  int getHandleCountForThread() const {
    return cache_->getHandleCountForThread();
  }

  // TODO(sathya) clean up this api by making this part of the find api
  // implementation.
  void recordAccess(folly::StringPiece key) {
    if (nvmAdmissionPolicy_) {
      nvmAdmissionPolicy_->trackAccess(key);
    }
  }

  // returns the initialized size of the cache.
  // TODO (sathya) deprecate this after cleaning up FastShutdownStressor
  size_t getCacheSize() const {
    return cache_->getCacheMemoryStats().cacheSize;
  }

  // empties the cache entries by removing the keys, this will schedule the
  // destructor call backs to be executed.
  void clearCache(uint64_t errorLimit);

  // shuts down the cache for persistence. User shall not access the instance
  // until the cache is re-attached using reAttach() below.
  void shutDown();

  // reinitialize the cache. This assumes the cache was shutdown using
  // shutDown() api.
  //
  // @throw   std::invalid_argument if the cache can not be re-attached.
  void reAttach();

  // cleanup the cache resources if the cache was persistent one, initialized
  // with a cache directory. TODO (sathya) merge this with shutDown()
  void cleanupSharedMem();

 private:
  // checks for the consistency of the operation for the item
  //
  // @param opId    the operation id
  // @param it      the item to check
  bool checkGet(ValueTracker::Index opId, const ItemHandle& it);

  // fetches the value stored in the item for consistency tracking purposes.
  // Only called if consistency checking is enabled.
  uint64_t getUint64FromItem(const Item& item) const {
    auto ptr = item.template getMemoryAs<CacheValue>();
    return ptr->getConsistencyNum();
  }

  // generates a hash corresponding to the handle and its's chain, based on
  // each item's hash value. Used for consistency tracking purposes.
  uint64_t genHashForChain(const ItemHandle& handle) const;

  // get the nand writes for the SSD device if enabled.
  uint64_t fetchNandWrites() const;

  // original input config for the cache used to derive the
  // CacheAllocatorConfig.
  const CacheConfig config_;

  // config for the currently initialized cache. This is also used to
  // re-attach after shutdown()
  typename Allocator::Config allocatorConfig_;

  // tracks if the cache instance should cleanup the NVM cache. This is done
  // if the resources were allocated by the cache instance in the constructor
  // instead of being provided by the user through the config as a
  // file/device. If this is an empty string, no cleanup is performed.
  std::string nvmCacheFilePath_;

  // The admission policy that tracks the accesses.
  std::shared_ptr<NvmAdmissionPolicy<Allocator>> nvmAdmissionPolicy_;

  // instance of the cache.
  std::unique_ptr<Allocator> cache_;

  // the monitor for the cache. This is a facebook specific functionality to
  // pull stats from the cachebench directly into facebook monitoring systems.
  std::unique_ptr<CacheMonitor> monitor_;

  // list of pools present in the cache.
  std::vector<PoolId> pools_;

  // this is used to check if the key has been previously reported to have
  // gotten into a inconsistent state and avoiding it for future requests.
  // Since this can be accessed from multiple threads, the map is initialized
  // during start up and only the value is updated by flipping the bit
  // atomically.
  std::unordered_map<std::string, std::atomic<bool>> invalidKeys_;

  // number of inconsistency detected so far with the operations
  std::atomic<unsigned int> inconsistencyCount_{0};

  // tracker for consistency monitoring.
  std::unique_ptr<ValueTracker> valueTracker_;

  // reading of the nand bytes written for the benchmark if enabled.
  const uint64_t nandBytesBegin_{0};

  // latency stats of cachelib APIs inside cachebench
  mutable util::PercentileStats cacheFindLatency_;

  // when enabled, tracks the keys in the cache independently to validate
  // features like ItemDestructor.
  ItemRecords<Allocator> itemRecords_;

  // number of times we detected an in-correct invocation of the item
  // destructor.
  std::atomic<uint64_t> invalidDestructor_{0};

  // number of times the item destructor was called in the cache, if enabled.
  std::atomic<int64_t> totalDestructor_{0};
};

// Specializations are required for each MMType
template <typename MMConfigType>
MMConfigType makeMMConfig(CacheConfig const&);

// LRU
template <>
inline typename LruAllocator::MMConfig makeMMConfig(CacheConfig const& config) {
  return LruAllocator::MMConfig(config.lruRefreshSec,
                                config.lruRefreshRatio,
                                config.lruUpdateOnWrite,
                                config.lruUpdateOnRead,
                                config.tryLockUpdate,
                                static_cast<uint8_t>(config.lruIpSpec));
}

// LRU
template <>
inline typename Lru2QAllocator::MMConfig makeMMConfig(
    CacheConfig const& config) {
  return Lru2QAllocator::MMConfig(config.lruRefreshSec,
                                  config.lruRefreshRatio,
                                  config.lruUpdateOnWrite,
                                  config.lruUpdateOnRead,
                                  config.tryLockUpdate,
                                  false,
                                  config.lru2qHotPct,
                                  config.lru2qColdPct);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
#include "cachelib/cachebench/cache/Cache-inl.h"
