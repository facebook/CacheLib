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
#include <folly/coro/GtestHelpers.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/interface/components/RAMCacheComponent.h"
#include "cachelib/interface/tests/Utils.h"

using namespace ::testing;
using namespace facebook::cachelib;
using namespace facebook::cachelib::interface;

namespace {

static const std::string kCacheName{"TestRAMCacheComponent"};

class RAMCacheComponentTest : public ::testing::Test {
 protected:
  static LruAllocatorConfig createValidConfig() {
    LruAllocatorConfig config;
    config.setCacheName(kCacheName);
    // 1 per alloc class + extra for metadata
    config.setCacheSize(6 * Slab::kSize);
    config.defaultPoolRebalanceStrategy = nullptr;
    return config;
  }

  static RAMCacheComponent::PoolConfig createValidPoolConfig() {
    static std::set<uint32_t> allocSizes = {64, 128, 256, 512, 1024};
    static MMLru::Config mmConfig{};
    return RAMCacheComponent::PoolConfig{
        .name_ = "test_pool",
        .size_ = allocSizes.size() * Slab::kSize,
        .allocSizes_ = allocSizes,
        .mmConfig_ = mmConfig,
        .ensureProvisionable_ = true,
    };
  }

  static Result<RAMCacheComponent> createCache(
      LruAllocatorConfig&& config = createValidConfig(),
      RAMCacheComponent::PoolConfig&& poolConfig = createValidPoolConfig()) {
    return RAMCacheComponent::create(std::move(config), std::move(poolConfig));
  }

  uint32_t now() const {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

TEST_F(RAMCacheComponentTest, CreateWithValidConfig) {
  auto cache = ASSERT_OK(createCache());
  EXPECT_EQ(cache.getName(), kCacheName);
}

TEST_F(RAMCacheComponentTest, CreateFailsWithInvalidConfig) {
  // Set NvmCacheConfig
  auto config = createValidConfig();
  LruAllocatorConfig::NvmCacheConfig nvmConfig;
  config.enableNvmCache(nvmConfig);
  EXPECT_ERROR(createCache(std::move(config)), Error::Code::INVALID_CONFIG);

  config = createValidConfig();
  config.enablePoolRebalancing(std::make_shared<RebalanceStrategy>(),
                               std::chrono::seconds(1));
  EXPECT_ERROR(createCache(std::move(config)), Error::Code::INVALID_CONFIG);

  // Set invalid cache size (0)
  config = createValidConfig();
  config.setCacheSize(0);
  EXPECT_ERROR(createCache(std::move(config)), Error::Code::INVALID_CONFIG);

  // Set pool size larger than cache size
  config = createValidConfig();
  auto poolConfig = createValidPoolConfig();
  poolConfig.size_ = 2 * config.getCacheSize();
  EXPECT_ERROR(createCache(std::move(config), std::move(poolConfig)),
               Error::Code::INVALID_CONFIG);
}

CO_TEST_F(RAMCacheComponentTest, BasicAllocateAndInsert) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "test_key";
  const uint32_t size = 100;
  const uint32_t creationTime = now();
  const uint32_t ttlSecs = 3600;

  // Allocate
  auto allocHandle =
      ASSERT_OK(co_await cache.allocate(key, size, creationTime, ttlSecs));

  EXPECT_EQ(allocHandle->getKey(), key);
  EXPECT_GE(allocHandle->getMemorySize(), size);
  EXPECT_EQ(allocHandle->getCreationTime(), creationTime);

  // Insert
  ASSERT_OK(co_await cache.insert(std::move(allocHandle)));
}

CO_TEST_F(RAMCacheComponentTest, AllocateWithInvalidArguments) {
  auto cache = ASSERT_OK(createCache());

  // Empty key should fail
  EXPECT_ERROR(co_await cache.allocate("", 100, now(), 3600),
               Error::Code::INVALID_ARGUMENTS);

  // Try to allocate a full slab size (bigger than any alloc classes)
  const uint32_t hugeSize = Slab::kSize;
  EXPECT_ERROR(co_await cache.allocate("huge_key", hugeSize, now(), 3600),
               Error::Code::INVALID_ARGUMENTS);
}

CO_TEST_F(RAMCacheComponentTest, InsertTwice) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "duplicate_key";

  // First allocation and insertion
  auto allocHandle1 = ASSERT_OK(co_await cache.allocate(key, 100, now(), 3600));
  EXPECT_OK(co_await cache.insert(std::move(allocHandle1)));

  // Second allocation and insertion with same key should fail
  auto allocHandle2 = ASSERT_OK(co_await cache.allocate(key, 100, now(), 3600));
  EXPECT_ERROR(co_await cache.insert(std::move(allocHandle2)),
               Error::Code::ALREADY_INSERTED);
}

CO_TEST_F(RAMCacheComponentTest, InsertOrReplace) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "replace_key";

  // First insertion
  auto allocHandle1 = ASSERT_OK(co_await cache.allocate(key, 100, now(), 3600));
  auto replaceResult1 =
      ASSERT_OK(co_await cache.insertOrReplace(std::move(allocHandle1)));
  EXPECT_FALSE(replaceResult1.has_value()); // No item was replaced

  // Second insertion should replace the first
  auto allocHandle2 = ASSERT_OK(co_await cache.allocate(key, 200, now(), 3600));
  auto replaceResult2 =
      ASSERT_OK(co_await cache.insertOrReplace(std::move(allocHandle2)));
  EXPECT_TRUE(replaceResult2.has_value()); // Old item was replaced

  auto& replacedHandle = replaceResult2.value();
  EXPECT_EQ(replacedHandle->getKey(), key);
  EXPECT_GE(replacedHandle->getMemorySize(), 100); // Original size
}

CO_TEST_F(RAMCacheComponentTest, FindExistingItem) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "find_key";
  const std::string data = "test_data";

  // Insert an item
  auto handle =
      ASSERT_OK(co_await cache.allocate(key, data.size(), now(), 3600));
  std::memcpy(handle->getMemory(), data.c_str(), data.size());
  ASSERT_OK(co_await cache.insert(std::move(handle)));

  // Find the item
  auto findResult = ASSERT_OK(co_await cache.find(key));
  EXPECT_TRUE(findResult.has_value());

  auto& foundHandle = findResult.value();
  EXPECT_EQ(foundHandle->getKey(), key);
  EXPECT_GE(foundHandle->getMemorySize(), data.size());

  // Verify data integrity
  std::string retrievedData(static_cast<const char*>(foundHandle->getMemory()),
                            data.size());
  EXPECT_EQ(retrievedData, data);
}

CO_TEST_F(RAMCacheComponentTest, FindNonExistentItem) {
  auto cache = ASSERT_OK(createCache());
  auto findResult = ASSERT_OK(co_await cache.find("nonexistent_key"));
  EXPECT_FALSE(findResult.has_value());
}

CO_TEST_F(RAMCacheComponentTest, FindExpiredItem) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "expired_key";
  const std::string data = "expired_data";

  // Insert an item
  auto handle =
      ASSERT_OK(co_await cache.allocate(key, data.size(), now() - 3601, 3600));
  std::memcpy(handle->getMemory(), data.c_str(), data.size());
  EXPECT_OK(co_await cache.insert(std::move(handle)));

  // Item should already be expired so we should not find it
  auto findResult = ASSERT_OK(co_await cache.find(key));
  EXPECT_FALSE(findResult.has_value());
}

CO_TEST_F(RAMCacheComponentTest, FindToWrite) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "write_key";
  const std::string originalData = "original";
  const std::string modifiedData = "modified";

  // Insert an item
  auto allocHandle = ASSERT_OK(co_await cache.allocate(
      key, std::max(originalData.size(), modifiedData.size()), now(), 3600));
  std::memcpy(allocHandle->getMemory(), originalData.c_str(),
              originalData.size());
  EXPECT_OK(co_await cache.insert(std::move(allocHandle)));

  // Find for write and modify
  {
    auto writeResult = ASSERT_OK(co_await cache.findToWrite(key));
    CO_ASSERT_TRUE(writeResult.has_value());
    auto& writeHandle = writeResult.value();
    std::memcpy(writeHandle->getMemory(), modifiedData.c_str(),
                modifiedData.size());
  }

  // Verify modification
  auto readResult = ASSERT_OK(co_await cache.find(key));
  CO_ASSERT_TRUE(readResult.has_value());
  const auto& readHandle = readResult.value();
  std::string retrievedData(static_cast<const char*>(readHandle->getMemory()),
                            modifiedData.size());
  EXPECT_EQ(retrievedData, modifiedData);
}

CO_TEST_F(RAMCacheComponentTest, RemoveByKey) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "remove_key";

  // Insert an item
  auto allocResult = ASSERT_OK(co_await cache.allocate(key, 100, now(), 3600));
  EXPECT_OK(co_await cache.insert(std::move(allocResult)));

  // Remove the item
  auto removeResult = ASSERT_OK(co_await cache.remove(key));
  EXPECT_TRUE(removeResult);

  // Verify item is gone
  auto findResult = ASSERT_OK(co_await cache.find(key));
  EXPECT_FALSE(findResult.has_value());

  // Remove again should return false
  auto removeAgainResult = ASSERT_OK(co_await cache.remove(key));
  EXPECT_FALSE(removeAgainResult); // Item was not removed (didn't exist)
}

CO_TEST_F(RAMCacheComponentTest, RemoveByHandle) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "remove_handle_key";

  // Insert an item
  auto allocResult = ASSERT_OK(co_await cache.allocate(key, 100, now(), 3600));
  EXPECT_OK(co_await cache.insert(std::move(allocResult)));

  // Find and remove by handle
  auto findResult = ASSERT_OK(co_await cache.find(key));
  CO_ASSERT_TRUE(findResult.has_value());
  ASSERT_OK(co_await cache.remove(std::move(findResult.value())));

  // Verify item is gone
  auto findAgainResult = ASSERT_OK(co_await cache.find(key));
  EXPECT_FALSE(findAgainResult.has_value());
}

CO_TEST_F(RAMCacheComponentTest, ItemProperties) {
  auto cache = ASSERT_OK(createCache());
  const std::string key = "props_key";
  const uint32_t size = 200;
  const uint32_t creationTime = now();
  const uint32_t ttlSecs = 3600;

  // Insert an item
  auto handle =
      ASSERT_OK(co_await cache.allocate(key, size, creationTime, ttlSecs));

  // Check properties before insertion
  EXPECT_EQ(handle->getKey(), key);
  EXPECT_GE(handle->getMemorySize(), size);
  EXPECT_EQ(handle->getCreationTime(), creationTime);
  EXPECT_EQ(handle->getExpiryTime(), creationTime + ttlSecs);

  EXPECT_OK(co_await cache.insert(std::move(handle)));

  // Check properties after insertion
  auto findResult = ASSERT_OK(co_await cache.find(key));
  CO_ASSERT_TRUE(findResult.has_value());
  auto& foundHandle = findResult.value();
  EXPECT_EQ(foundHandle->getKey(), key);
  EXPECT_GE(foundHandle->getMemorySize(), size);
  EXPECT_EQ(foundHandle->getCreationTime(), creationTime);
  EXPECT_EQ(foundHandle->getExpiryTime(), creationTime + ttlSecs);
}

CO_TEST_F(RAMCacheComponentTest, MultipleItems) {
  auto cache = ASSERT_OK(createCache());

  const int numItems = 10;
  std::vector<std::string> keys;
  keys.reserve(numItems);

  // Insert multiple items
  for (int i = 0; i < numItems; ++i) {
    auto& key = keys.emplace_back("multi_key_" + std::to_string(i));

    auto allocHandle =
        ASSERT_OK(co_await cache.allocate(key, 100, now(), 3600));
    ASSERT_OK(co_await cache.insert(std::move(allocHandle)));
  }

  // Verify all items exist
  for (const auto& key : keys) {
    auto findResult = ASSERT_OK(co_await cache.find(key));
    CO_ASSERT_TRUE(findResult.has_value());
    EXPECT_EQ(findResult.value()->getKey(), key);
  }

  // Remove all items
  for (const auto& key : keys) {
    auto removeResult = ASSERT_OK(co_await cache.remove(key));
    EXPECT_TRUE(removeResult);
  }

  // Verify all items are gone
  for (const auto& key : keys) {
    auto findResult = ASSERT_OK(co_await cache.find(key));
    EXPECT_FALSE(findResult.has_value());
  }
}

TEST_F(RAMCacheComponentTest, GetUnderlyingAllocator) {
  auto cache = ASSERT_OK(createCache());
  LruAllocator& allocator = cache.get();

  // Basic sanity check that we can use the allocator
  EXPECT_GT(allocator.getCacheMemoryStats().ramCacheSize, 0);
}

} // namespace
