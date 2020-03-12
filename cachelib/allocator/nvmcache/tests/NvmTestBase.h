#pragma once

#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
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

  void removeFromRamForTesting(folly::StringPiece key) {
    cache_->removeFromRamForTesting(key);
  }

  void removeFromNvmForTesting(folly::StringPiece key) {
    cache_->removeFromNvmForTesting(key);
  }

  bool pushToNvmCacheFromRamForTesting(folly::StringPiece key) {
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
  folly::dynamic config_;

  // cache directory for the cache
  std::string cacheDir_;

  // the config for the allocator that also includes the dipper config
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
