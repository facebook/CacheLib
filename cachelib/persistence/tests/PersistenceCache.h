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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/NvmTestUtils.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/compact_cache/CCacheCreator.h"
#include "cachelib/persistence/PersistenceManager.h"
#include "cachelib/persistence/tests/PersistenceManagerMock.h"

namespace facebook {
namespace cachelib {

namespace tests {

class PersistenceCache {
 public:
  PersistenceCache()
      : buffer_(folly::IOBuf::create(kCapacity)),
        cacheDir_("/tmp/persistence_test" +
                  folly::to<std::string>(folly::Random::rand32())) {
    util::makeDir(cacheDir_);
    config_
        .setCacheSize(kCacheSize) // 100MB
        .setCacheName("test")
        .enableCachePersistence(cacheDir_)
        .usePosixForShm()
        .setAccessConfig(
            {25 /* bucket power */, 10 /* lock power */}) // assuming caching 20
                                                          // million items
        .setDefaultAllocSizes(std::set<uint32_t>{64, 128, 1024, 2048, 4096})
        // Disable slab rebalancing
        .enablePoolRebalancing(nullptr, std::chrono::seconds{0})
        .validate(); // will throw if bad config
  }

  ~PersistenceCache() {
    Cache::ShmManager::cleanup(cacheDir_, config_.usePosixShm);
    util::removePath(cacheDir_);
  }

  std::set<std::string> cacheSetup(
      std::vector<std::pair<std::string, std::string>> items,
      uint32_t numPools,
      uint32_t numChained,
      bool testNvm) {
    std::set<std::string> evictedKeys;

    auto removeCB = [&](const Cache::RemoveCbData& data) {
      evictedKeys.insert(data.item.getKey().toString());
    };
    config_.setRemoveCallback(removeCB);
    if (numChained > 0) {
      config_.configureChainedItems();
    }

    Cache cache(Cache::SharedMemNew, config_);
    std::vector<PoolId> pools;
    for (uint32_t i = 0; i < numPools; ++i) {
      pools.push_back(
          cache.addPool(folly::sformat("pool_{}", i),
                        cache.getCacheMemoryStats().ramCacheSize / numPools));
    }

    for (uint32_t i = 0; i < items.size(); ++i) {
      auto pool = pools[i % numPools];
      auto& key = items[i].first;
      auto& val = items[i].second;
      auto handle = cache.allocate(pool, key, val.size());
      EXPECT_NE(handle, nullptr);
      std::memcpy(handle->getMemory(), val.data(), val.size());

      for (uint32_t j = 0; j < numChained; ++j) {
        std::string chainedData = folly::sformat("{}_Chained_{}", val, j);
        auto chainedHandle =
            cache.allocateChainedItem(handle, chainedData.length());
        EXPECT_NE(chainedHandle, nullptr);
        std::memcpy(chainedHandle->getMemory(),
                    chainedData.data(),
                    chainedData.length());
        cache.addChainedItem(handle, std::move(chainedHandle));
      }

      cache.insertOrReplace(handle);
      handle.reset();
    }

    if (testNvm) {
      cache.flushNvmCache();
      // push all items to nvm
      for (uint32_t i = 0; i < items.size(); ++i) {
        auto& key = items[i].first;
        if (cache.pushToNvmCacheFromRamForTesting(key)) {
          cache.removeFromRamForTesting(key);
          auto res = cache.inspectCache(key);
          // must not exist in RAM
          EXPECT_EQ(nullptr, res.first);
          // must be in nvmcache
          EXPECT_NE(nullptr, res.second);
        }
      }
      // do another round of lookup for find evicted items
      // currently the removeCb doesn't work for Navy
      evictedKeys.clear();
      for (uint32_t i = 0; i < items.size(); ++i) {
        auto res = cache.inspectCache(items[i].first);
        if (res.second == nullptr) {
          evictedKeys.insert(items[i].first);
        }
      }
    }
    cache.shutDown();
    return evictedKeys;
  }

  void cacheCleanup() {
    Cache::ShmManager::cleanup(cacheDir_, config_.usePosixShm);
    AllocatorTest<Cache>::testShmIsRemoved(config_);
    util::removePath(cacheDir_);
    util::makeDir(cacheDir_);
    util::makeDir(cacheDir_ + "/navy");
  }

  void cacheVerify(std::vector<std::pair<std::string, std::string>> items,
                   uint32_t numChained,
                   std::set<std::string> evictedKeys) {
    Cache cache(Cache::SharedMemAttach, config_);

    for (auto& key : items) {
      auto handle = cache.find(key.first);
      if (handle) {
        auto data =
            std::string(reinterpret_cast<const char*>(handle->getMemory()),
                        handle->getSize());
        ASSERT_EQ(data, key.second);
        if (numChained > 0) {
          auto chained_allocs = cache.viewAsChainedAllocs(handle);
          ASSERT_EQ(numChained, chained_allocs.computeChainLength());
          for (uint32_t j = 0; j < numChained; ++j) {
            auto chained_item =
                chained_allocs.getNthInChain(numChained - j - 1);
            auto chained_data = std::string(
                reinterpret_cast<const char*>(chained_item->getMemory()),
                chained_item->getSize());
            ASSERT_EQ(chained_data,
                      folly::sformat("{}_Chained_{}", key.second, j));
          }
        }
      } else {
        // if item not in restored cache, it must be evicted items.
        ASSERT_TRUE(evictedKeys.count(key.first));
      }
    }

    cache.shutDown();
  }

  void test(std::vector<std::pair<std::string, std::string>> items,
            uint32_t numPools,
            uint32_t numChained,
            bool testNvm) {
    PersistenceManager manager(config_);
    // setup cache, insert data
    auto evictedKeys = cacheSetup(items, numPools, numChained, testNvm);
    // don't allow too many evicted items
    EXPECT_LE(evictedKeys.size(), items.size() / 2);

    // persist cache
    MockPersistenceStreamWriter writer(buffer_.get());
    manager.saveCache(writer);

    // clean up memory and disk cache data
    cacheCleanup();

    // restore cache
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    manager.restoreCache(reader);

    // verify restored cache data
    cacheVerify(items, numChained, evictedKeys);
  }

  std::vector<std::pair<std::string, std::string>> getKeyValuePairs(
      uint32_t numKeys) {
    std::vector<std::pair<std::string, std::string>> keys;
    for (uint32_t i = 0; i < numKeys; ++i) {
      std::string k = folly::sformat("key_{}", i);
      std::string v;
      v.resize(folly::Random::rand32(2048));
      folly::Random::secureRandom(v.data(), v.length());
      keys.emplace_back(k, v);
    }
    return keys;
  }

 public:
  const uint32_t kNumKeys = 1024 * 1024;    // 1 million
  const size_t kCacheSize = 100 * kNumKeys; // 100MB
  const size_t kCapacity = 5 * kCacheSize;  // 500MB

  std::unique_ptr<folly::IOBuf> buffer_;
  std::string cacheDir_;
  CacheConfig config_;
};

} // namespace tests
} // namespace cachelib
} // namespace facebook
