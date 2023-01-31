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
#include "cachelib/persistence/tests/PersistenceCache.h"
#include "cachelib/persistence/tests/PersistenceManagerMock.h"

namespace facebook {
namespace cachelib {

namespace tests {

class PersistenceManagerTest : public ::testing::Test {
 public:
  PersistenceCache cache_;
};

TEST_F(PersistenceManagerTest, testConfigChange) {
  auto items = cache_.getKeyValuePairs(3);
  auto evictedKeys = cache_.cacheSetup(items, 1, 0, false);
  {
    PersistenceManager manager(cache_.config_);
    MockPersistenceStreamWriter writer(cache_.buffer_.get());
    manager.saveCache(writer);
  }
  {
    // change cache size is ok for restorCache
    auto config = cache_.config_;
    config.setCacheSize(cache_.kCacheSize + 1);
    PersistenceManager manager(config);
    // restore cache will fail
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    EXPECT_NO_THROW(manager.restoreCache(reader));
  }

  {
    // change cache name
    auto config = cache_.config_;
    config.setCacheName("failure");
    PersistenceManager manager(config);
    // restore cache will fail
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "Config doesn't match: test|failure");
  }

  {
    // change cache dir is ok
    auto config = cache_.config_;
    config.enableCachePersistence(cache_.cacheDir_ + "success");
    PersistenceManager manager(config);
    // restore cache will not fail
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    EXPECT_NO_THROW(manager.restoreCache(reader));
  }

  // clean up memory and disk cache data
  cache_.cacheCleanup();

  {
    // change cache config
    auto config = cache_.config_;
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
  cache_.test({{key, data}}, 1, 0, false);
}

TEST_F(PersistenceManagerTest, testSmallMultiplePools) {
  // test three items, two pools
  cache_.test(cache_.getKeyValuePairs(3), 2, 0, false);
}

TEST_F(PersistenceManagerTest, testChained) {
  // test three items, three chained item
  cache_.test(cache_.getKeyValuePairs(3), 1, 3, false);
}

TEST_F(PersistenceManagerTest, testNvmSingle) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cache_.cacheDir_);
  nvmConfig.navyConfig.setSimpleFile(cache_.cacheDir_ + "/navy_CACHE",
                                     nvmConfig.navyConfig.getFileSize());
  cache_.config_.enableNvmCache(nvmConfig);

  // test ten items, three chained item, nvm
  cache_.test(cache_.getKeyValuePairs(10), 1, 3, true);
}

TEST_F(PersistenceManagerTest, testNvmRaidSmall) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cache_.cacheDir_);
  util::makeDir(cache_.cacheDir_ + "/navy");

  nvmConfig.navyConfig.setSimpleFile("", 0);
  nvmConfig.navyConfig.setRaidFiles(
      {cache_.cacheDir_ + "/navy/CACHE0", cache_.cacheDir_ + "/navy/CACHE1",
       cache_.cacheDir_ + "/navy/CACHE2", cache_.cacheDir_ + "/navy/CACHE3"},
      25 * 1024ULL * 1024ULL /*25MB*/);

  cache_.config_.enableNvmCache(nvmConfig);
  // test ten items, three chained item, nvm
  cache_.test(cache_.getKeyValuePairs(10), 1, 3, true);
}

TEST_F(PersistenceManagerTest, testLarge) {
  cache_.config_.setCacheSize(cache_.kCacheSize * 8); // 800MB
  cache_.buffer_->reserve(0, cache_.kCapacity * 3);   // 1.2GB
  // test 500k items, 4 pools
  cache_.test(cache_.getKeyValuePairs(500 * 1000), 4, 0, false);
}

TEST_F(PersistenceManagerTest, testNvmRaidLarge) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cache_.cacheDir_);
  util::makeDir(cache_.cacheDir_ + "/navy");

  // 100MB - 1 byte to test non-fullMB navy file size
  nvmConfig.navyConfig.setSimpleFile("", 0);
  nvmConfig.navyConfig.setRaidFiles(
      {cache_.cacheDir_ + "/navy/CACHE0", cache_.cacheDir_ + "/navy/CACHE1",
       cache_.cacheDir_ + "/navy/CACHE2", cache_.cacheDir_ + "/navy/CACHE3"},
      100 * 1024ULL * 1024ULL - 1, true);

  nvmConfig.navyConfig.setDeviceMetadataSize(10 * 1024ULL * 1024ULL);

  cache_.buffer_->reserve(0, cache_.kCapacity * 2);

  cache_.config_.enableNvmCache(nvmConfig);
  // test 100 K items, nvm
  cache_.test(cache_.getKeyValuePairs(100 * 1000), 1, 0, true);
}

TEST_F(PersistenceManagerTest, testCompactCache) {
  cache_.config_.enableCompactCache();

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

  PersistenceManager manager(cache_.config_);
  {
    Cache cache(Cache::SharedMemNew, cache_.config_);

    auto* cc = cache.template addCompactCache<IntValueCCache>(
        ccacheName, cache.getCacheMemoryStats().ramCacheSize);
    ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(k1), &k2));
    ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(k2), &k3));
    ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(k3), &k1));

    cache.shutDown();

    MockPersistenceStreamWriter writer(cache_.buffer_.get());
    manager.saveCache(writer);
  }

  cache_.cacheCleanup();

  {
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    manager.restoreCache(reader);

    Cache cache(Cache::SharedMemAttach, cache_.config_);
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
  PersistenceManager manager(cache_.config_);
  auto items = cache_.getKeyValuePairs(3);
  auto evictedKeys = cache_.cacheSetup(items, 1, 0, false);

  {
    // limited buffer size will cause write fail with exception
    MockPersistenceStreamWriter writer(cache_.buffer_.get(), 10);
    ASSERT_THROW_WITH_MSG(manager.saveCache(writer), std::invalid_argument,
                          "over capacity");
  }
  {
    // retry with enough buffer should success
    MockPersistenceStreamWriter writer(cache_.buffer_.get());
    manager.saveCache(writer);
  }
  {
    // verify the cache after persistence still attachable and valid
    cache_.cacheVerify(items, 0, evictedKeys);
  }
  {
    // clean up memory and disk cache data
    cache_.cacheCleanup();

    // restore cache
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    manager.restoreCache(reader);

    // verify restored cache data
    cache_.cacheVerify(items, 0, evictedKeys);
  }
}

TEST_F(PersistenceManagerTest, testReadFailAndRetry) {
  PersistenceManager manager(cache_.config_);
  auto items = cache_.getKeyValuePairs(3);
  auto evictedKeys = cache_.cacheSetup(items, 1, 0, false);
  MockPersistenceStreamWriter writer(cache_.buffer_.get());
  manager.saveCache(writer);
  // clean up memory and disk cache data
  cache_.cacheCleanup();
  {
    // restore cache with wrong buffer
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length() - 100);
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "invalid data");
  }

  {
    // restore cache with reader exception
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    reader.ExpectCallAt(Invoke([&](uint32_t) -> folly::IOBuf {
                          throw std::runtime_error("mock error");
                        }),
                        10);
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::runtime_error,
                          "mock error");
  }

  {
    // retry restore cache with correct buffer
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    manager.restoreCache(reader);
    // verify restored cache data
    cache_.cacheVerify(items, 0, evictedKeys);
  }
}

TEST_F(PersistenceManagerTest, testCacheDirChange) {
  auto items = cache_.getKeyValuePairs(10);
  auto evictedKeys = cache_.cacheSetup(items, 1, 0, false);
  {
    PersistenceManager manager(cache_.config_);
    MockPersistenceStreamWriter writer(cache_.buffer_.get());
    manager.saveCache(writer);
  }

  // clean up memory and disk cache data
  cache_.cacheCleanup();
  util::removePath(cache_.cacheDir_);
  // change cache dir
  cache_.cacheDir_ += "changed";

  cache_.config_.enableCachePersistence(cache_.cacheDir_);
  {
    PersistenceManager manager(cache_.config_);
    // restore cache
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    manager.restoreCache(reader);
    // verify restored cache data
    cache_.cacheVerify(items, 0, evictedKeys);
  }
}

TEST_F(PersistenceManagerTest, testNvmFail) {
  LruAllocator::NvmCacheConfig nvmConfig;
  nvmConfig.navyConfig = utils::getNvmTestConfig(cache_.cacheDir_);
  nvmConfig.navyConfig.setSimpleFile(cache_.cacheDir_ + "/navy_CACHE",
                                     nvmConfig.navyConfig.getFileSize());
  cache_.config_.enableNvmCache(nvmConfig);

  auto items = cache_.getKeyValuePairs(10);
  auto evictedKeys = cache_.cacheSetup(items, 1, 0, false);
  {
    PersistenceManager manager(cache_.config_);
    MockPersistenceStreamWriter writer(cache_.buffer_.get());
    manager.saveCache(writer);
  }

  cache_.cacheCleanup();

  // make coruppted data
  std::memset(cache_.buffer_->writableTail() - kDataBlockSize - 16, 0, 10);

  {
    PersistenceManager manager(cache_.config_);
    // restore cache
    MockPersistenceStreamReader reader(cache_.buffer_->data(),
                                       cache_.buffer_->length());
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "invalid checksum");
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
