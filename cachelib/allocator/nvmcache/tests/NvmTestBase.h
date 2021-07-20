#pragma once

#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace tests {

using AllocatorT = LruAllocator;
using Item = AllocatorT::Item;
using ItemHandle = AllocatorT::ItemHandle;
using ChainedAllocs = AllocatorT::ChainedAllocs;

class NvmCacheTest : public testing::Test {
 public:
  NvmCacheTest();
  ~NvmCacheTest();

  AllocatorT& makeCache();
  AllocatorT::Config& getConfig() { return allocConfig_; }

  // return a reference to the cache instance
  AllocatorT& cache() const noexcept { return *cache_; }

  // pool id for the cache allocations
  PoolId poolId() const noexcept { return id_; }

  // fetch the key. if _ramOnly_ then we only fetch it if it is in RAM.
  ItemHandle fetch(folly::StringPiece key, bool ramOnly);

  // similar to fetch, but only check if it exists
  bool checkKeyExists(folly::StringPiece key, bool ramOnly);

  // internal eviction count
  size_t evictionCount() const noexcept { return nEvictions_; }

  GlobalCacheStats getStats() const;

  // convert our cache instance to be on top of shared memory to support
  // persistence tests
  void convertToShmCache();

  void warmRoll();
  void coldRoll();
  void iceRoll();
  void iceColdRoll();
  auto shutDownCache() { return cache_->shutDown(); }

  void insertOrReplace(ItemHandle& handle) {
    cache_->insertOrReplace(handle);
    // enforce nvm to complete remove job (triggered by insertOrReplace).
    // o/w it will cause an immediate eviction's put job  to fail.
    // Using large items in the test triggers eviction immediately after
    // insertOrReplace, and some items may be missing without the flushNvm. this
    // should be safe in production since the eviction of the item won't happen
    // within a short duration.
    cache_->flushNvmCache();
  }

  void removeFromRamForTesting(folly::StringPiece key) {
    cache_->removeFromRamForTesting(key);
  }

  void removeFromNvmForTesting(folly::StringPiece key) {
    cache_->removeFromNvmForTesting(key);
  }

  bool pushToNvmCacheFromRamForTesting(folly::StringPiece key) {
    // a typical test case is insertOrReplace then push to nvm immediately.
    // but pending remove job (triggered by insertOrReplace) will fail
    // the put job due to active TombStone.
    cache_->flushNvmCache();
    return cache_->pushToNvmCacheFromRamForTesting(key);
  }

  std::pair<ItemHandle, ItemHandle> inspectCache(folly::StringPiece key) {
    return cache_->inspectCache(key);
  }

  void releaseSlabFor(void* memory) {
    auto allocInfo = cache_->getAllocInfo(memory);
    cache_->releaseSlab(allocInfo.poolId, allocInfo.classId,
                        SlabReleaseMode::kRebalance);
  }

 protected:
  // Helper for ShardHashIsNotFillMapHash because we're the friend of NvmCache.
  std::pair<size_t, size_t> getNvmShardAndHashForKey(folly::StringPiece key) {
    using NvmCacheT = typename AllocatorT::NvmCacheT;
    auto shard = NvmCacheT::getShardForKey(key);
    auto hash =
        typename NvmCacheT::FillMap{}.hash_function()(key) % NvmCacheT::kShards;
    return std::make_pair(shard, hash);
  }

  folly::dynamic options_;
  navy::NavyConfig config_;

  // cache directory for the cache
  std::string cacheDir_;

  // the config for the allocator that also includes the nvm config
  AllocatorT::Config allocConfig_;

  // cache instance
  std::unique_ptr<AllocatorT> cache_;

  size_t poolSize_{8 * 1024 * 1024};

  std::set<uint32_t> poolAllocsizes_{20 * 1024};

  // pool id for allocation
  PoolId id_;

  // internal state for evictions.
  std::atomic<size_t> nEvictions_{0};
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
