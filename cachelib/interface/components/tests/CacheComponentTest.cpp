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
#include <folly/coro/BlockingWait.h>
#include <folly/coro/GtestHelpers.h>
#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/interface/CacheComponent.h"
#include "cachelib/interface/components/FlashCacheComponent.h"
#include "cachelib/interface/components/RAMCacheComponent.h"
#include "cachelib/interface/tests/Utils.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"
#include "cachelib/navy/common/Device.h"

using namespace ::testing;
using namespace facebook::cachelib;
using namespace facebook::cachelib::interface;

namespace {

/**
 * Factory interface for creating CacheComponent instances for testing.  Add a
 * sub-class for new CacheComponents to test them.
 */
class CacheFactory {
 public:
  virtual ~CacheFactory() = default;
  virtual std::unique_ptr<CacheComponent> create() = 0;
};

class RAMCacheFactory : public CacheFactory {
 public:
  std::unique_ptr<CacheComponent> create() override {
    auto config = createConfig();
    auto poolConfig = createPoolConfig();
    auto ramCache = ASSERT_OK(
        RAMCacheComponent::create(std::move(config), std::move(poolConfig)));
    return std::make_unique<RAMCacheComponent>(std::move(ramCache));
  }

 private:
  static LruAllocatorConfig createConfig() {
    LruAllocatorConfig config;
    config.setCacheName("CacheComponentTest");
    config.setCacheSize(6 * Slab::kSize);
    config.defaultPoolRebalanceStrategy = nullptr;
    return config;
  }

  static RAMCacheComponent::PoolConfig createPoolConfig() {
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
};

class FlashCacheFactory : public CacheFactory {
 public:
  FlashCacheFactory()
      : hits_(/* count */ kDeviceSize / kRegionSize, /* value */ 0) {}

  std::unique_ptr<CacheComponent> create() override {
    device_ = navy::createMemoryDevice(kDeviceSize, /* encryptor */ nullptr);
    auto flashCache = ASSERT_OK(
        FlashCacheComponent::create("CacheComponentTest", makeConfig()));
    return std::make_unique<FlashCacheComponent>(std::move(flashCache));
  }

 private:
  using BlockCache = navy::BlockCache;

  static constexpr uint64_t kRegionSize{/* 16KB */ 16 * 1024};
  static constexpr uint64_t kDeviceSize{/* 256KB */ 256 * 1024};

  std::unique_ptr<navy::Device> device_;
  std::vector<uint32_t> hits_;

  BlockCache::Config makeConfig() {
    BlockCache::Config config;
    config.regionSize = kRegionSize;
    config.cacheSize = kDeviceSize;
    config.device = device_.get();
    config.evictionPolicy =
        std::make_unique<NiceMock<navy::MockPolicy>>(&hits_);
    return config;
  }
};

template <typename FactoryType>
class CacheComponentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    factory_ = std::make_unique<FactoryType>();
    cache_ = factory_->create();
    ASSERT_NE(cache_, nullptr) << "Failed to create cache";
  }

  uint32_t now() const {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  std::unique_ptr<CacheComponent> cache_;

 private:
  std::unique_ptr<FactoryType> factory_;
};

// Define the list of factory types to test
using FactoryTypes = ::testing::Types<RAMCacheFactory, FlashCacheFactory>;
TYPED_TEST_SUITE(CacheComponentTest, FactoryTypes);

// ============================================================================
// getName() Tests
// ============================================================================

TYPED_TEST(CacheComponentTest, GetName) {
  const std::string& name = this->cache_->getName();
  EXPECT_EQ(name, "CacheComponentTest");
}

// ============================================================================
// allocate() Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, AllocateBasic) {
  const std::string key = "test_allocate";
  const uint32_t size = 100;
  const uint32_t creationTime = this->now();
  const uint32_t ttlSecs = 3600;

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, size, creationTime, ttlSecs));

  EXPECT_EQ(handle->getKey(), key);
  EXPECT_GE(handle->getMemorySize(), size);
  EXPECT_EQ(handle->getCreationTime(), creationTime);
  EXPECT_EQ(handle->getExpiryTime(), creationTime + ttlSecs);
}

CO_TYPED_TEST(CacheComponentTest, AllocateVariousSizes) {
  const std::vector<uint32_t> sizes = {1, 10, 50, 100, 200, 500};
  const uint32_t creationTime = this->now();
  const uint32_t ttlSecs = 3600;

  for (uint32_t size : sizes) {
    const std::string key = "size_test_" + std::to_string(size);
    auto handle = ASSERT_OK(
        co_await this->cache_->allocate(key, size, creationTime, ttlSecs));
    EXPECT_GE(handle->getMemorySize(), size);
  }
}

CO_TYPED_TEST(CacheComponentTest, AllocateZeroTTL) {
  const std::string key = "zero_ttl";
  const uint32_t size = 100;
  const uint32_t creationTime = this->now();
  const uint32_t ttlSecs = 0;

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, size, creationTime, ttlSecs));
  // zero TTL = infinite TTL
  EXPECT_EQ(handle->getExpiryTime(), 0);
}

CO_TYPED_TEST(CacheComponentTest, AllocateEmptyKey) {
  EXPECT_ERROR(co_await this->cache_->allocate("", 100, this->now(), 3600),
               Error::Code::INVALID_ARGUMENTS);
}

// ============================================================================
// insert() Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, InsertBasic) {
  const std::string key = "insert_basic";
  auto handle =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));
}

CO_TYPED_TEST(CacheComponentTest, InsertWithData) {
  const std::string key = "insert_data";
  const std::string data = "test_data_content";

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, data.size(), this->now(), 3600));
  std::memcpy(handle->getMemory(), data.c_str(), data.size());
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult.has_value());

  std::string retrievedData(
      findResult.value()->template getMemoryAs<const char>(), data.size());
  EXPECT_EQ(retrievedData, data);
}

CO_TYPED_TEST(CacheComponentTest, InsertDuplicateKey) {
  const std::string key = "duplicate";

  auto handle1 =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle1)));

  auto handle2 =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  EXPECT_ERROR(co_await this->cache_->insert(std::move(handle2)),
               Error::Code::ALREADY_INSERTED);
}

// ============================================================================
// insertOrReplace() Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, InsertOrReplaceNewItem) {
  const std::string key = "replace_new";
  auto handle =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  auto result =
      ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle)));

  EXPECT_FALSE(result.has_value());
}

CO_TYPED_TEST(CacheComponentTest, InsertOrReplaceExistingItem) {
  const std::string key = "replace_existing";
  const std::string data1 = "original_data";
  const std::string data2 = "replaced_data";

  auto handle1 = ASSERT_OK(
      co_await this->cache_->allocate(key, data1.size(), this->now(), 3600));
  std::memcpy(handle1->getMemory(), data1.c_str(), data1.size());
  auto result1 =
      ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle1)));
  EXPECT_FALSE(result1.has_value());

  auto handle2 = ASSERT_OK(
      co_await this->cache_->allocate(key, data2.size(), this->now(), 3600));
  std::memcpy(handle2->getMemory(), data2.c_str(), data2.size());
  auto result2 =
      ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle2)));

  // Some implementations may not return the old data -- and that's ok!
  if (result2.has_value()) {
    auto& replacedHandle = result2.value();
    EXPECT_EQ(replacedHandle->getKey(), key);

    std::string replacedData(replacedHandle->template getMemoryAs<const char>(),
                             data1.size());
    EXPECT_EQ(replacedData, data1);
  }

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult.has_value());
  std::string currentData(
      findResult.value()->template getMemoryAs<const char>(), data2.size());
  EXPECT_EQ(currentData, data2);
}

CO_TYPED_TEST(CacheComponentTest, InsertOrReplaceMultipleTimes) {
  const std::string key = "replace_multiple";

  for (int i = 0; i < 5; ++i) {
    auto handle =
        ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
    *handle->template getMemoryAs<uint32_t>() = i;
    auto result =
        ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle)));

    if (i == 0) {
      EXPECT_FALSE(result.has_value());
    } else if (result.has_value()) {
      EXPECT_EQ(*result.value()->template getMemoryAs<uint32_t>(), i - 1);
    }
  }
}

CO_TYPED_TEST(CacheComponentTest, InsertOrReplaceDifferentSizes) {
  const std::string key = "replace_sizes";
  const std::string smallData = "small";
  const std::string largeData = "this_is_a_much_larger_data_string";

  // Insert small item first
  auto handle1 = ASSERT_OK(co_await this->cache_->allocate(
      key, smallData.size(), this->now(), 3600));
  std::memcpy(handle1->getMemory(), smallData.c_str(), smallData.size());
  auto result1 =
      ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle1)));
  EXPECT_FALSE(result1.has_value());

  // Replace with larger item
  auto handle2 = ASSERT_OK(co_await this->cache_->allocate(
      key, largeData.size(), this->now(), 3600));
  std::memcpy(handle2->getMemory(), largeData.c_str(), largeData.size());
  auto result2 =
      ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle2)));

  if (result2.has_value()) {
    auto& replacedSmallHandle = result2.value();
    EXPECT_EQ(replacedSmallHandle->getKey(), key);
    EXPECT_GE(replacedSmallHandle->getMemorySize(), smallData.size());
    std::string retrievedSmall(
        replacedSmallHandle->template getMemoryAs<const char>(),
        smallData.size());
    EXPECT_EQ(retrievedSmall, smallData);
  }

  // Verify large data is now in cache
  auto findResult1 = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult1.has_value());
  EXPECT_GE(findResult1.value()->getMemorySize(), largeData.size());
  std::string retrievedLarge1(
      findResult1.value()->template getMemoryAs<const char>(),
      largeData.size());
  EXPECT_EQ(retrievedLarge1, largeData);

  // Replace large item with smaller item
  auto handle3 = ASSERT_OK(co_await this->cache_->allocate(
      key, smallData.size(), this->now(), 3600));
  std::memcpy(handle3->getMemory(), smallData.c_str(), smallData.size());
  auto result3 =
      ASSERT_OK(co_await this->cache_->insertOrReplace(std::move(handle3)));

  if (result3.has_value()) {
    auto& replacedLargeHandle = result3.value();
    EXPECT_EQ(replacedLargeHandle->getKey(), key);
    EXPECT_GE(replacedLargeHandle->getMemorySize(), largeData.size());
    std::string retrievedLarge2(
        replacedLargeHandle->template getMemoryAs<const char>(),
        largeData.size());
    EXPECT_EQ(retrievedLarge2, largeData);
  }

  // Verify small data is now in cache
  auto findResult2 = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult2.has_value());
  EXPECT_GE(findResult2.value()->getMemorySize(), smallData.size());
  std::string retrievedSmall2(
      findResult2.value()->template getMemoryAs<const char>(),
      smallData.size());
  EXPECT_EQ(retrievedSmall2, smallData);
}

// ============================================================================
// find() Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, FindExistingItem) {
  const std::string key = "find_existing";
  const std::string data = "find_test_data";

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, data.size(), this->now(), 3600));
  std::memcpy(handle->getMemory(), data.c_str(), data.size());
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult.has_value());

  auto& foundHandle = findResult.value();
  EXPECT_EQ(foundHandle->getKey(), key);

  std::string retrievedData(foundHandle->template getMemoryAs<const char>(),
                            data.size());
  EXPECT_EQ(retrievedData, data);
}

CO_TYPED_TEST(CacheComponentTest, FindNonExistentItem) {
  auto findResult = ASSERT_OK(co_await this->cache_->find("nonexistent"));
  EXPECT_FALSE(findResult.has_value());
}

CO_TYPED_TEST(CacheComponentTest, FindMultipleItems) {
  const int numItems = 10;
  std::vector<std::string> keys;
  keys.reserve(numItems);

  for (int i = 0; i < numItems; ++i) {
    auto& key = keys.emplace_back("multi_" + std::to_string(i));
    auto handle =
        ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
    EXPECT_OK(co_await this->cache_->insert(std::move(handle)));
  }

  for (const auto& key : keys) {
    auto findResult = ASSERT_OK(co_await this->cache_->find(key));
    CO_ASSERT_TRUE(findResult.has_value());
    EXPECT_EQ(findResult.value()->getKey(), key);
  }
}

CO_TYPED_TEST(CacheComponentTest, FindExpiredItem) {
  const std::string key = "expired";
  const uint32_t creationTime = this->now() - 7200;
  const uint32_t ttlSecs = 3600;

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, 100, creationTime, ttlSecs));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  EXPECT_FALSE(findResult.has_value());
}

CO_TYPED_TEST(CacheComponentTest, FindPropertiesMatch) {
  const std::string key = "props";
  const uint32_t size = 200;
  const uint32_t creationTime = this->now();
  const uint32_t ttlSecs = 7200;

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, size, creationTime, ttlSecs));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult.has_value());

  auto& foundHandle = findResult.value();
  EXPECT_EQ(foundHandle->getKey(), key);
  EXPECT_GE(foundHandle->getMemorySize(), size);
  EXPECT_EQ(foundHandle->getCreationTime(), creationTime);
  EXPECT_EQ(foundHandle->getExpiryTime(), creationTime + ttlSecs);
}

// ============================================================================
// findToWrite() Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, FindToWriteExistingItem) {
  const std::string key = "write_existing";
  const std::string originalData = "original";

  auto handle =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  std::memcpy(handle->getMemory(), originalData.c_str(), originalData.size());
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto writeResult = ASSERT_OK(co_await this->cache_->findToWrite(key));
  CO_ASSERT_TRUE(writeResult.has_value());

  auto& writeHandle = writeResult.value();
  EXPECT_EQ(writeHandle->getKey(), key);
}

CO_TYPED_TEST(CacheComponentTest, FindToWriteNonExistentItem) {
  auto writeResult =
      ASSERT_OK(co_await this->cache_->findToWrite("nonexistent_write"));
  EXPECT_FALSE(writeResult.has_value());
}

CO_TYPED_TEST(CacheComponentTest, FindToWriteAndModify) {
  const std::string key = "modify";
  const std::string originalData = "original";
  const std::string modifiedData = "modified";

  auto handle = ASSERT_OK(co_await this->cache_->allocate(
      key, std::max(originalData.size(), modifiedData.size()), this->now(),
      3600));
  std::memcpy(handle->getMemory(), originalData.c_str(), originalData.size());
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  {
    auto writeResult = ASSERT_OK(co_await this->cache_->findToWrite(key));
    CO_ASSERT_TRUE(writeResult.has_value());
    auto& writeHandle = writeResult.value();
    std::memcpy(writeHandle->getMemory(), modifiedData.c_str(),
                modifiedData.size());
    writeHandle.markDirty();
  }

  auto readResult = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(readResult.has_value());
  std::string retrievedData(
      readResult.value()->template getMemoryAs<const char>(),
      modifiedData.size());
  EXPECT_EQ(retrievedData, modifiedData);
}

CO_TYPED_TEST(CacheComponentTest, FindToWriteExpiredItem) {
  const std::string key = "expired_write";
  const uint32_t creationTime = this->now() - 7200;
  const uint32_t ttlSecs = 3600;

  auto handle = ASSERT_OK(
      co_await this->cache_->allocate(key, 100, creationTime, ttlSecs));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto writeResult = ASSERT_OK(co_await this->cache_->findToWrite(key));
  EXPECT_FALSE(writeResult.has_value());
}

// ============================================================================
// remove(Key) Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, RemoveByKeyExistingItem) {
  const std::string key = "remove_key";

  auto handle =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto removeResult = ASSERT_OK(co_await this->cache_->remove(key));
  EXPECT_TRUE(removeResult);

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  EXPECT_FALSE(findResult.has_value());
}

CO_TYPED_TEST(CacheComponentTest, RemoveByKeyNonExistentItem) {
  auto removeResult =
      ASSERT_OK(co_await this->cache_->remove("nonexistent_remove"));
  EXPECT_FALSE(removeResult);
}

CO_TYPED_TEST(CacheComponentTest, RemoveByKeyTwice) {
  const std::string key = "remove_twice";

  auto handle =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  EXPECT_OK(co_await this->cache_->insert(std::move(handle)));

  auto removeResult1 = ASSERT_OK(co_await this->cache_->remove(key));
  EXPECT_TRUE(removeResult1);

  auto removeResult2 = ASSERT_OK(co_await this->cache_->remove(key));
  EXPECT_FALSE(removeResult2);
}

CO_TYPED_TEST(CacheComponentTest, RemoveMultipleItemsByKey) {
  const int numItems = 10;
  std::vector<std::string> keys;
  keys.reserve(numItems);

  for (int i = 0; i < numItems; ++i) {
    auto& key = keys.emplace_back("remove_multi_" + std::to_string(i));
    auto handle =
        ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
    EXPECT_OK(co_await this->cache_->insert(std::move(handle)));
  }

  for (const auto& key : keys) {
    auto removeResult = ASSERT_OK(co_await this->cache_->remove(key));
    EXPECT_TRUE(removeResult);
  }

  for (const auto& key : keys) {
    auto findResult = ASSERT_OK(co_await this->cache_->find(key));
    EXPECT_FALSE(findResult.has_value());
  }
}

// ============================================================================
// remove(ReadHandle) Tests
// ============================================================================

CO_TYPED_TEST(CacheComponentTest, RemoveByHandle) {
  const std::string key = "remove_handle";

  auto allocHandle =
      ASSERT_OK(co_await this->cache_->allocate(key, 100, this->now(), 3600));
  EXPECT_OK(co_await this->cache_->insert(std::move(allocHandle)));

  auto findResult = ASSERT_OK(co_await this->cache_->find(key));
  CO_ASSERT_TRUE(findResult.has_value());
  EXPECT_OK(co_await this->cache_->remove(std::move(findResult.value())));

  auto findAgainResult = ASSERT_OK(co_await this->cache_->find(key));
  EXPECT_FALSE(findAgainResult.has_value());
}

// ============================================================================
// Multi-threaded Tests
// ============================================================================

TYPED_TEST(CacheComponentTest, MultiThreadedOperations) {
  constexpr int kNumThreads = 8;
  constexpr int kOperationsPerThread = 1000;
  constexpr int kNumSharedKeys = 20;

  std::vector<std::string> sharedKeys;
  sharedKeys.reserve(kNumSharedKeys);
  for (int i = 0; i < kNumSharedKeys; ++i) {
    sharedKeys.emplace_back("shared_key_" + std::to_string(i));
  }

  std::atomic<int> successfulInserts{0};
  std::atomic<int> successfulReplacements{0};
  std::atomic<int> successfulFinds{0};
  std::atomic<int> successfulRemoves{0};
  std::atomic<int> dataCorruptions{0};

  // Make sure we have enough space to modify values in findToWrite
  constexpr size_t maxValueSize = 32;

  auto workerTask = [&](int threadId) -> folly::coro::Task<void> {
    for (int i = 0; i < kOperationsPerThread; ++i) {
      const auto& key = sharedKeys[folly::Random::rand32() % sharedKeys.size()];
      const std::string value =
          "thread_" + std::to_string(threadId) + "_op_" + std::to_string(i);

      int operation = folly::Random::rand32() % 6;
      try {
        switch (operation) {
        case 0: { // allocate + insert
          auto allocResult = co_await this->cache_->allocate(key, maxValueSize,
                                                             this->now(), 3600);
          if (allocResult.hasValue()) {
            std::memcpy(allocResult.value()->getMemory(), value.c_str(),
                        value.size());
            auto insertResult =
                co_await this->cache_->insert(std::move(allocResult.value()));
            if (insertResult.hasValue()) {
              successfulInserts.fetch_add(1, std::memory_order_relaxed);
            }
          }
          break;
        }

        case 1: { // allocate + insertOrReplace
          auto allocResult = co_await this->cache_->allocate(key, maxValueSize,
                                                             this->now(), 3600);
          if (allocResult.hasValue()) {
            std::memcpy(allocResult.value()->getMemory(), value.c_str(),
                        value.size());
            auto replaceResult = co_await this->cache_->insertOrReplace(
                std::move(allocResult.value()));
            if (replaceResult.hasValue()) {
              successfulReplacements.fetch_add(1, std::memory_order_relaxed);
            }
          }
          break;
        }

        case 2: { // find
          auto findResult = co_await this->cache_->find(key);
          if (findResult.hasValue() && findResult.value().has_value()) {
            successfulFinds.fetch_add(1, std::memory_order_relaxed);

            const auto& handle = findResult.value().value();
            const char* data = handle->template getMemoryAs<const char>();
            const size_t dataSize = handle->getMemorySize();

            std::string_view retrievedData(data, dataSize);
            if (!retrievedData.starts_with("thread_") &&
                !retrievedData.starts_with("modified_")) {
              dataCorruptions.fetch_add(1, std::memory_order_relaxed);
            }
          }
          break;
        }

        case 3: { // findToWrite + modify
          auto writeResult = co_await this->cache_->findToWrite(key);
          if (writeResult.hasValue() && writeResult.value().has_value()) {
            auto& writeHandle = writeResult.value().value();
            const std::string modifiedValue = "modified_" + value;
            CO_ASSERT_GE(writeHandle->getMemorySize(), modifiedValue.size());
            std::memcpy(writeHandle->getMemory(), modifiedValue.c_str(),
                        modifiedValue.size());
            writeHandle.markDirty();
          }
          break;
        }

        case 4: { // remove(key)
          auto removeResult = co_await this->cache_->remove(key);
          if (removeResult.hasValue() && removeResult.value()) {
            successfulRemoves.fetch_add(1, std::memory_order_relaxed);
          }
          break;
        }

        case 5: { // find + remove(handle)
          auto findResult = co_await this->cache_->find(key);
          if (findResult.hasValue() && findResult.value().has_value()) {
            auto removeResult = co_await this->cache_->remove(
                std::move(findResult.value().value()));
            if (removeResult.hasValue()) {
              successfulRemoves.fetch_add(1, std::memory_order_relaxed);
            }
          }
          break;
        }

        default:
          throw std::runtime_error("Invalid operation");
          break;
        }
      } catch (const std::exception& e) {
        XLOG(FATAL) << "Thread " << threadId
                    << " caught exception: " << e.what();
      }
    }

    co_return;
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, threadId = i]() {
      folly::coro::blockingWait(workerTask(threadId));
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_GT(successfulInserts.load(), 0);
  EXPECT_GT(successfulReplacements.load(), 0);
  EXPECT_GT(successfulFinds.load(), 0);
  EXPECT_EQ(dataCorruptions.load(), 0);
}

} // namespace
