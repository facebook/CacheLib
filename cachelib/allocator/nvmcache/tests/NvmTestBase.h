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

#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace tests {

using AllocatorT = LruAllocator;
using Item = AllocatorT::Item;
using WriteHandle = AllocatorT::WriteHandle;
using ReadHandle = AllocatorT::ReadHandle;
using ChainedAllocs = AllocatorT::ChainedAllocs;
using DestructorData = typename AllocatorT::DestructorData;
using ChainedItemIter = AllocatorT::ChainedItemIter;
using DestructedData = AllocatorT::DestructorData;

class NvmCacheTest : public testing::Test {
 public:
  using NvmCacheT = typename AllocatorT::NvmCacheT;

  NvmCacheTest();
  ~NvmCacheTest();

  AllocatorT& makeCache();
  AllocatorT::Config& getConfig() { return allocConfig_; }

  // return a reference to the cache instance
  AllocatorT& cache() const noexcept { return *cache_; }

  // pool id for the cache allocations
  PoolId poolId() const noexcept { return id_; }

  // fetch the key. if _ramOnly_ then we only fetch it if it is in RAM.
  WriteHandle fetch(folly::StringPiece key, bool ramOnly);

  // fetch the key to write. if _ramOnly_ then we only fetch it if it is in RAM.
  WriteHandle fetchToWrite(folly::StringPiece key, bool ramOnly);

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

  void insertOrReplace(WriteHandle& handle) {
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

  bool pushToNvmCacheFromRamForTesting(folly::StringPiece key,
                                       bool flush = true) {
    if (flush) {
      // a typical test case is insertOrReplace then push to nvm immediately.
      // but pending remove job (triggered by insertOrReplace) will fail
      // the put job due to active TombStone.
      cache_->flushNvmCache();
    }
    return cache_->pushToNvmCacheFromRamForTesting(key);
  }

  void pushToNvmCacheFromRamForTesting(WriteHandle& handle) {
    auto nvmCache = getNvmCache();
    if (nvmCache) {
      nvmCache->put(handle, nvmCache->createPutToken(handle->getKey()));
    }
  }

  std::pair<ReadHandle, ReadHandle> inspectCache(folly::StringPiece key) {
    return cache_->inspectCache(key);
  }

  void releaseSlabFor(void* memory) {
    auto allocInfo = cache_->getAllocInfo(memory);
    cache_->releaseSlab(allocInfo.poolId, allocInfo.classId,
                        SlabReleaseMode::kRebalance);
  }

  NvmCacheT* getNvmCache() {
    return cache_ ? cache_->nvmCache_.get() : nullptr;
  }

  std::unique_ptr<NvmItem> makeNvmItem(const WriteHandle& handle) {
    return getNvmCache()->makeNvmItem(handle);
  }

  std::unique_ptr<folly::IOBuf> createItemAsIOBuf(folly::StringPiece key,
                                                  const NvmItem& dItem) {
    return getNvmCache()->createItemAsIOBuf(key, dItem);
  }

  template <typename... Params>
  void evictCB(Params&&... args) {
    getNvmCache()->evictCB(std::forward<Params>(args)...);
  }

  folly::Range<ChainedItemIter> viewAsChainedAllocsRange(folly::IOBuf* parent) {
    return getNvmCache()->viewAsChainedAllocsRange(parent);
  }

 protected:
  // Helper for ShardHashIsNotFillMapHash because we're the friend of NvmCache.
  std::pair<size_t, size_t> getNvmShardAndHashForKey(folly::StringPiece key) {
    auto shard = NvmCacheT::getShardForKey(key);
    auto hash =
        typename NvmCacheT::FillMap{}.hash_function()(key) % NvmCacheT::kShards;
    return std::make_pair(shard, hash);
  }

  void verifyItemInIOBuf(const std::string& key,
                         const ReadHandle& handle,
                         folly::IOBuf* iobuf);

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
