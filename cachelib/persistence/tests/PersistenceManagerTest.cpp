// Copyright 2004-present Facebook. All Rights Reserved.

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/NvmTestUtils.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/compact_cache/CCacheCreator.h"
#include "cachelib/persistence/tests/PersistenceManagerMock.h"

namespace facebook {
namespace cachelib {

namespace tests {

class PersistenceManagerTest : public ::testing::Test {
 public:
  PersistenceManagerTest()
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

    for (uint32_t i = 0; i < kNumKeys; ++i) {
      std::string k = folly::sformat("key_{}", i);
      std::string v;
      v.resize(folly::Random::rand32(2048));
      folly::Random::secureRandom(v.data(), v.length());
      keys_.emplace_back(k, v);
    }
  }

  ~PersistenceManagerTest() { util::removePath(cacheDir_); }

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
                        cache.getCacheMemoryStats().cacheSize / numPools));
    }

    for (uint32_t i = 0; i < items.size(); ++i) {
      auto pool = pools[i % numPools];
      auto& key = items[i].first;
      auto& val = items[i].second;
      auto handle = cache.allocate(pool, key, val.size());
      EXPECT_NE(handle, nullptr);
      std::memcpy(handle->getWritableMemory(), val.data(), val.size());

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

      if (testNvm) {
        EXPECT_TRUE(cache.pushToNvmCacheFromRamForTesting(key));
        EXPECT_TRUE(cache.removeFromRamForTesting(key));
        auto res = cache.inspectCache(key);
        // must not exist in RAM
        EXPECT_EQ(nullptr, res.first);
        // must be in nvmcache
        EXPECT_NE(nullptr, res.second);
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

 protected:
  const uint32_t kNumKeys = 1024 * 1024;    // 1 million
  const size_t kCacheSize = 100 * kNumKeys; // 100MB
  const size_t kCapacity = 4 * kCacheSize;  // 400MB

  std::unique_ptr<folly::IOBuf> buffer_;
  std::string cacheDir_;
  CacheConfig config_;
  std::vector<std::pair<std::string, std::string>> keys_;
};

TEST_F(PersistenceManagerTest, testConfigChange) {
  std::vector<std::pair<std::string, std::string>> items = {keys_.begin(),
                                                            keys_.begin() + 3};
  auto evictedKeys = cacheSetup(items, 1, 0, false);
  {
    PersistenceManager manager(config_);
    MockPersistenceStreamWriter writer(buffer_.get());
    manager.saveCache(writer);
  }
  {
    // change cache size is ok for restorCache
    auto config = config_;
    config.setCacheSize(kCacheSize + 1);
    PersistenceManager manager(config);
    // restore cache will fail
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    EXPECT_NO_THROW(manager.restoreCache(reader));
  }

  {
    // change cache name
    auto config = config_;
    config.setCacheName("failure");
    PersistenceManager manager(config);
    // restore cache will fail
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    EXPECT_THROW(manager.restoreCache(reader), std::invalid_argument);
  }

  {
    // change cache dir is ok
    auto config = config_;
    config.enableCachePersistence("success");
    PersistenceManager manager(config);
    // restore cache will not fail
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    EXPECT_NO_THROW(manager.restoreCache(reader));
  }

  // clean up memory and disk cache data
  cacheCleanup();

  {
    // change cache config
    auto config = config_;
    // non-default page size is not allowed
    config.setAccessConfig(Cache::AccessConfig{10, 10, PageSizeT::TWO_MB});
    EXPECT_THROW(PersistenceManager manager(config), std::invalid_argument);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
