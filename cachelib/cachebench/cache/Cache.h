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
// to use item.getMemory and item.getSize APIs.
template <typename Allocator>
class Cache {
 public:
  using Item = typename Allocator::Item;
  using Config = typename Allocator::Config;
  using ItemHandle = typename Allocator::ItemHandle;
  using Key = typename Item::Key;
  using RemoveRes = typename Allocator::RemoveRes;
  using SyncObj = typename Allocator::SyncObj;
  using ChainedItemMovingSync = typename Allocator::ChainedItemMovingSync;

  template <typename U>
  using TypedHandle = LruAllocator::TypedHandle<U>;
  using ChainedAllocs = LruAllocator::ChainedAllocs;
  using ChainedItemIter = LruAllocator::ChainedItemIter;

  explicit Cache(CacheConfig config,
                 ChainedItemMovingSync movingSync = nullptr,
                 std::string cacheDir = "");

  ~Cache();

  // true if the cache only uses DRAM. false if the cache is configured to
  // have NVM as well (even if it is mocked by DRAM underneath).
  bool isRamOnly() const { return config_.nvmCacheSizeMB == 0; }

  void flushNvmCache() { cache_->flushNvmCache(); }

  bool isNvmCacheDisabled() const {
    return !isRamOnly() && !cache_->isNvmCacheEnabled();
  }

  void enableConsistencyCheck(const std::vector<std::string>& keys);

  bool consistencyCheckEnabled() const { return valueTracker_ != nullptr; }

  uint64_t getNumAbortedReleases() const {
    return cache_->getGlobalCacheStats().numAbortedSlabReleases;
  }

  void cleanupSharedMem() {
    if (!cacheDir_.empty()) {
      cache_->cleanupStrayShmSegments(allocatorConfig_.cacheDir,
                                      allocatorConfig_.usePosixShm);
      util::removePath(cacheDir_);
    }
  }

  unsigned int getInconsistencyCount() const {
    // Because we compare on it
    return inconsistencyCount_.load(std::memory_order_release);
  }

  // Return the readonly memory
  const void* getMemory(const ItemHandle& item) const noexcept {
    return item == nullptr ? nullptr : getMemory(*item);
  }

  // Return the writable memory
  void* getWritableMemory(ItemHandle& item) const noexcept {
    return item == nullptr ? nullptr : getWritableMemory(*item);
  }

  // Return the readonly memory
  const void* getMemory(const Item& item) const noexcept {
    return item.template getMemoryAs<CacheValue>()->getData();
  }

  // Return the writable memory
  void* getWritableMemory(Item& item) const noexcept {
    return item.template getWritableMemoryAs<CacheValue>()->getWritableData();
  }

  uint32_t getSize(const ItemHandle& item) const noexcept {
    return item == nullptr ? 0 : getSize(item.get());
  }

  uint32_t getSize(const Item* item) const noexcept {
    if (item == nullptr) {
      return 0;
    }
    return item->template getMemoryAs<CacheValue>()->getDataSize(
        item->getSize());
  }

  uint64_t getUint64FromItem(const Item& item) {
    auto ptr = item.template getMemoryAs<CacheValue>();
    return ptr->getConsistencyNum();
  }

  void setUint64ToItem(ItemHandle& handle, uint64_t num) {
    auto ptr = handle->template getWritableMemoryAs<CacheValue>();
    ptr->setConsistencyNum(num);
  }

  void setStringItem(ItemHandle& handle, const std::string& str) {
    auto ptr = reinterpret_cast<uint8_t*>(getWritableMemory(handle));
    std::memcpy(ptr, str.data(), std::min<size_t>(str.size(), getSize(handle)));
  }

  auto allocateAccessible(PoolId poolId,
                          Key key,
                          uint32_t size,
                          uint32_t ttlSecs = 0) {
    return util::allocateAccessible(
        *cache_, poolId, key, CacheValue::getSize(size), ttlSecs);
  }

  template <typename... Params>
  auto getPoolStats(Params&&... args) {
    return cache_->getPoolStats(std::forward<Params>(args)...);
  }

  auto allocate(PoolId pid,
                folly::StringPiece key,
                size_t size,
                uint32_t ttlSecs = 0) {
    ItemHandle handle;
    try {
      handle = cache_->allocate(pid, key, CacheValue::getSize(size), ttlSecs);
      if (handle) {
        CacheValue::initialize(handle->getWritableMemory());
      }
    } catch (const std::invalid_argument& e) {
      XLOGF(DBG, "Unable to allocate, reason: {}", e.what());
    }

    return handle;
  }

  int getHandleCountForThread() const {
    return cache_->getHandleCountForThread();
  }

  bool isShutDownInProgress() const { return cache_->isShutDownInProgress(); }

  size_t getCacheSize() const {
    return cache_->getCacheMemoryStats().cacheSize;
  }

  void releaseSlab(PoolId pid, ClassId v, ClassId r, SlabReleaseMode mode) {
    cache_->releaseSlab(pid, v, r, mode);
  }

  ItemHandle insertOrReplace(const ItemHandle& handle) {
    itemRecords_.addItemRecord(handle);

    if (!consistencyCheckEnabled()) {
      try {
        return cache_->insertOrReplace(handle);
      } catch (const cachelib::exception::RefcountOverflow& ex) {
        XLOGF(DBG, "overflow exception: {}", ex.what());
      }
    }

    auto checksum = getUint64FromItem(*handle);
    auto opId = valueTracker_->beginSet(handle->getKey(), checksum);
    auto rv = cache_->insertOrReplace(handle);
    valueTracker_->endSet(opId);
    return rv;
  }

  void shutDown() {
    monitor_.reset();
    cache_->shutDown();
  }

  uint64_t genHashForChain(const ItemHandle& handle) {
    auto chainedAllocs = cache_->viewAsChainedAllocs(handle);
    uint64_t hash = getUint64FromItem(*handle);
    for (const auto& item : chainedAllocs.getChain()) {
      hash = folly::hash::hash_128_to_64(hash, getUint64FromItem(item));
    }
    return hash;
  }

  void trackChainChecksum(const ItemHandle& handle) {
    assert(consistencyCheckEnabled());
    auto checksum = genHashForChain(handle);
    auto opId = valueTracker_->beginSet(handle->getKey(), checksum);
    valueTracker_->endSet(opId);
  }

  auto insert(const ItemHandle& handle) {
    // Insert is not supported in consistency checking mode because consistency
    // checking assumes a Set always succeeds and overrides existing value.
    XDCHECK(!consistencyCheckEnabled());
    itemRecords_.addItemRecord(handle);
    return cache_->insert(handle);
  }

  ItemHandle find(Key key, AccessMode mode = AccessMode::kRead) {
    auto findFn = [&]() {
      util::LatencyTracker tracker;
      if (FLAGS_report_api_latency) {
        tracker = util::LatencyTracker(cacheFindLatency_);
      }
      // find from cache and wait for the result to be ready.
      auto it = cache_->find(key, mode);
      it.wait();
      return it;
    };

    if (!consistencyCheckEnabled()) {
      return findFn();
    }

    auto opId = valueTracker_->beginGet(key);
    auto it = findFn();
    if (checkGet(opId, it)) {
      invalidKeys_[key.str()].store(true, std::memory_order_acquire);
    }
    return it;
  }

  RemoveRes remove(Key key) {
    if (!consistencyCheckEnabled()) {
      return cache_->remove(key);
    }

    auto opId = valueTracker_->beginDelete(key);
    auto rv = cache_->remove(key);
    valueTracker_->endDelete(opId);
    return rv;
  }

  RemoveRes remove(const ItemHandle& it) {
    if (!consistencyCheckEnabled()) {
      return cache_->remove(it);
    }

    auto opId = valueTracker_->beginDelete(it->getKey());
    auto rv = cache_->remove(it->getKey());
    valueTracker_->endDelete(opId);
    return rv;
  }

  auto allocateChainedItem(const ItemHandle& it, size_t size) {
    auto handle = cache_->allocateChainedItem(it, CacheValue::getSize(size));
    if (handle) {
      CacheValue::initialize(handle->getWritableMemory());
    }
    return handle;
  }

  auto addChainedItem(const ItemHandle& parent, ItemHandle child) {
    itemRecords_.updateItemVersion(*parent);
    return cache_->addChainedItem(parent, std::move(child));
  }

  template <typename... Params>
  auto viewAsChainedAllocs(Params&&... args) {
    return cache_->viewAsChainedAllocs(std::forward<Params>(args)...);
  }

  auto replaceChainedItem(Item& oldItem,
                          ItemHandle newItemHandle,
                          Item& parent) {
    itemRecords_.updateItemVersion(parent);
    return cache_->replaceChainedItem(
        oldItem, std::move(newItemHandle), parent);
  }

  void updateItem(ItemHandle& it) {
    itemRecords_.updateItemVersion(*it);
    cache_->invalidateNvm(*it);
  }

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

  uint64_t numPools() const noexcept { return config_.numPools; }

  const std::vector<PoolId>& poolIds() const noexcept { return pools_; }

  bool isInvalidKey(const std::string& key) {
    return invalidKeys_[key].load(std::memory_order_release);
  }

  // Get overall stats on the whole cache allocator
  Stats getStats() const;

  PoolStats getPoolStats(PoolId pid) const { return cache_->getPoolStats(pid); }

  void reattach();

  void recordAccess(folly::StringPiece key) {
    if (nvmAdmissionPolicy_) {
      nvmAdmissionPolicy_->trackAccess(key);
    }
  }

  void clearCache();

  uint64_t getInvalidDestructorCount() { return invalidDestructor_; }

 private:
  bool checkGet(ValueTracker::Index opId, const ItemHandle& it);
  uint64_t fetchNandWrites() const;

  const CacheConfig config_;
  std::vector<std::string> nvmCacheFilePaths_;
  const std::string cacheDir_;
  // The admission policy that tracks the accesses.
  std::shared_ptr<NvmAdmissionPolicy<Allocator>> nvmAdmissionPolicy_;
  std::atomic<unsigned int> inconsistencyCount_{0};
  std::unique_ptr<ValueTracker> valueTracker_;
  std::unique_ptr<Allocator> cache_;
  std::unique_ptr<CacheMonitor> monitor_;
  std::vector<PoolId> pools_;
  std::unordered_map<std::string, std::atomic<bool>> invalidKeys_;
  ChainedItemMovingSync movingSync_;
  Config allocatorConfig_;
  // reading of the nand bytes written for the benchmark if enabled.
  const uint64_t nandBytesBegin_{0};

  bool shouldCleanupFiles_{false};

  // latency stats of cachelib APIs inside cachebench
  mutable util::PercentileStats cacheFindLatency_;

  ItemRecords<Allocator> itemRecords_;
  std::atomic<uint64_t> invalidDestructor_{0};
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
