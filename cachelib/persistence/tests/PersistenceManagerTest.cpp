// Copyright 2004-present Facebook. All Rights Reserved.

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
  }

  ~PersistenceManagerTest() {
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

    if (testNvm) {
      // for nvm test, do another round of lookup for find evicted items
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
        auto data = std::string(reinterpret_cast<char*>(handle->getMemory()),
                                handle->getSize());
        ASSERT_EQ(data, key.second);
        if (numChained > 0) {
          auto chained_allocs = cache.viewAsChainedAllocs(handle);
          ASSERT_EQ(numChained, chained_allocs.computeChainLength());
          for (uint32_t j = 0; j < numChained; ++j) {
            auto chained_item =
                chained_allocs.getNthInChain(numChained - j - 1);
            auto chained_data =
                std::string(reinterpret_cast<char*>(chained_item->getMemory()),
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

 protected:
  const uint32_t kNumKeys = 1024 * 1024;    // 1 million
  const size_t kCacheSize = 100 * kNumKeys; // 100MB
  const size_t kCapacity = 4 * kCacheSize;  // 400MB

  std::unique_ptr<folly::IOBuf> buffer_;
  std::string cacheDir_;
  CacheConfig config_;
};

TEST_F(PersistenceManagerTest, testConfigChange) {
  auto items = getKeyValuePairs(3);
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
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "Config doesn't match: test|failure");
  }

  {
    // change cache dir is ok
    auto config = config_;
    config.enableCachePersistence(cacheDir_ + "success");
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
    ASSERT_THROW_WITH_MSG(PersistenceManager manager(config),
                          std::invalid_argument,
                          "Only default PageSize is supported to persist");
  }
}

TEST_F(PersistenceManagerTest, testSmallSinglePool) {
  std::string key = "key";
  std::string data = "Repalce the data associated with key key";
  // test single item, one pool
  test({{key, data}}, 1, 0, false);
}

TEST_F(PersistenceManagerTest, testSmallMultiplePools) {
  // test three items, two pools
  test(getKeyValuePairs(3), 2, 0, false);
}

TEST_F(PersistenceManagerTest, testChained) {
  // test three items, three chained item
  test(getKeyValuePairs(3), 1, 3, false);
}

TEST_F(PersistenceManagerTest, testNvmSingle) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cacheDir_);
  nvmConfig.navyConfig.setSimpleFile(cacheDir_ + "/navy_CACHE",
                                     nvmConfig.navyConfig.getFileSize());
  config_.enableNvmCache(nvmConfig);

  // test ten items, three chained item, nvm
  test(getKeyValuePairs(10), 1, 3, true);
}

TEST_F(PersistenceManagerTest, testNvmRaidSmall) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cacheDir_);
  util::makeDir(cacheDir_ + "/navy");

  nvmConfig.navyConfig.setSimpleFile("", 0);
  nvmConfig.navyConfig.setRaidFiles(
      {cacheDir_ + "/navy/CACHE0", cacheDir_ + "/navy/CACHE1",
       cacheDir_ + "/navy/CACHE2", cacheDir_ + "/navy/CACHE3"},
      25 * 1024ULL * 1024ULL /*25MB*/);

  config_.enableNvmCache(nvmConfig);
  // test ten items, three chained item, nvm
  test(getKeyValuePairs(10), 1, 3, true);
}

TEST_F(PersistenceManagerTest, testLarge) {
  config_.setCacheSize(kCacheSize * 8); // 800MB
  buffer_->reserve(0, kCapacity * 3);   // 1.2GB
  // test 500k items, 4 pools
  test(getKeyValuePairs(500 * 1000), 4, 0, false);
}

TEST_F(PersistenceManagerTest, testNvmRaidLarge) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cacheDir_);
  util::makeDir(cacheDir_ + "/navy");

  // 100MB - 1 byte to test non-fullMB navy file size
  nvmConfig.navyConfig.setSimpleFile("", 0);
  nvmConfig.navyConfig.setRaidFiles(
      {cacheDir_ + "/navy/CACHE0", cacheDir_ + "/navy/CACHE1",
       cacheDir_ + "/navy/CACHE2", cacheDir_ + "/navy/CACHE3"},
      100 * 1024ULL * 1024ULL - 1, true);

  nvmConfig.navyConfig.setDeviceMetadataSize(10 * 1024ULL * 1024ULL);

  buffer_->reserve(0, kCapacity * 2);

  config_.enableNvmCache(nvmConfig);
  // test 100 K items, nvm
  test(getKeyValuePairs(100 * 1000), 1, 0, true);
}

TEST_F(PersistenceManagerTest, testCompactCache) {
  config_.enableCompactCache();

  struct Key {
    int id;
    // need these two functions for key comparison
    bool operator==(const Key& other) const { return id == other.id; }
    bool isEmpty() const { return id == 0; }
    explicit Key(int i) : id(i) {}
  };
  using IntValueCCache =
      typename CCacheCreator<CCacheAllocator, Key, int>::type;

  int k1 = 1, k2 = 2, k3 = 3;
  std::string ccacheName = "test compact cache";

  PersistenceManager manager(config_);
  {
    Cache cache(Cache::SharedMemNew, config_);

    auto* cc = cache.template addCompactCache<IntValueCCache>(
        ccacheName, cache.getCacheMemoryStats().cacheSize);
    ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(k1), &k2));
    ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(k2), &k3));
    ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(k3), &k1));

    cache.shutDown();

    MockPersistenceStreamWriter writer(buffer_.get());
    manager.saveCache(writer);
  }

  cacheCleanup();

  {
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    manager.restoreCache(reader);

    Cache cache(Cache::SharedMemAttach, config_);
    auto* cc = cache.template attachCompactCache<IntValueCCache>(ccacheName);

    int out;
    ASSERT_EQ(CCacheReturn::FOUND, cc->get(Key(k1), &out));
    ASSERT_EQ(out, k2);
    ASSERT_EQ(CCacheReturn::FOUND, cc->get(Key(k2), &out));
    ASSERT_EQ(out, k3);
    ASSERT_EQ(CCacheReturn::FOUND, cc->get(Key(k3), &out));
    ASSERT_EQ(out, k1);

    cache.shutDown();
  }
}

TEST_F(PersistenceManagerTest, testWriteFailAndRetry) {
  PersistenceManager manager(config_);
  auto items = getKeyValuePairs(3);
  auto evictedKeys = cacheSetup(items, 1, 0, false);

  {
    // limited buffer size will cause write fail with exception
    MockPersistenceStreamWriter writer(buffer_.get(), 10);
    ASSERT_THROW_WITH_MSG(manager.saveCache(writer), std::invalid_argument,
                          "over capacity");
  }
  {
    // retry with enough buffer should success
    MockPersistenceStreamWriter writer(buffer_.get());
    manager.saveCache(writer);
  }
  {
    // verify the cache after persistence still attachable and valid
    cacheVerify(items, 0, evictedKeys);
  }
  {
    // clean up memory and disk cache data
    cacheCleanup();

    // restore cache
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    manager.restoreCache(reader);

    // verify restored cache data
    cacheVerify(items, 0, evictedKeys);
  }
}

TEST_F(PersistenceManagerTest, testReadFailAndRetry) {
  PersistenceManager manager(config_);
  auto items = getKeyValuePairs(3);
  auto evictedKeys = cacheSetup(items, 1, 0, false);
  MockPersistenceStreamWriter writer(buffer_.get());
  manager.saveCache(writer);
  // clean up memory and disk cache data
  cacheCleanup();
  {
    // restore cache with wrong buffer
    MockPersistenceStreamReader reader(buffer_->data(),
                                       buffer_->length() - 100);
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "invalid data");
  }

  {
    // restore cache with reader exception
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    reader.ExpectCallAt(Invoke([&](uint32_t) -> folly::IOBuf {
                          throw std::runtime_error("mock error");
                        }),
                        10);
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::runtime_error,
                          "mock error");
  }

  {
    // retry restore cache with correct buffer
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    manager.restoreCache(reader);
    // verify restored cache data
    cacheVerify(items, 0, evictedKeys);
  }
}

TEST_F(PersistenceManagerTest, testCacheDirChange) {
  auto items = getKeyValuePairs(10);
  auto evictedKeys = cacheSetup(items, 1, 0, false);
  {
    PersistenceManager manager(config_);
    MockPersistenceStreamWriter writer(buffer_.get());
    manager.saveCache(writer);
  }

  // clean up memory and disk cache data
  cacheCleanup();
  util::removePath(cacheDir_);
  // change cache dir
  cacheDir_ += "changed";

  config_.enableCachePersistence(cacheDir_);
  {
    PersistenceManager manager(config_);
    // restore cache
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    manager.restoreCache(reader);
    // verify restored cache data
    cacheVerify(items, 0, evictedKeys);
  }
}

TEST_F(PersistenceManagerTest, testNvmFail) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cacheDir_);
  nvmConfig.navyConfig.setSimpleFile(cacheDir_ + "/navy_CACHE",
                                     nvmConfig.navyConfig.getFileSize());
  config_.enableNvmCache(nvmConfig);

  auto items = getKeyValuePairs(10);
  auto evictedKeys = cacheSetup(items, 1, 0, false);
  {
    PersistenceManager manager(config_);
    MockPersistenceStreamWriter writer(buffer_.get());
    manager.saveCache(writer);
  }

  cacheCleanup();

  // make coruppted data
  std::memset(buffer_->writableTail() - kDataBlockSize - 16, 0, 10);

  {
    PersistenceManager manager(config_);
    // restore cache
    MockPersistenceStreamReader reader(buffer_->data(), buffer_->length());
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "invalid checksum");
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
