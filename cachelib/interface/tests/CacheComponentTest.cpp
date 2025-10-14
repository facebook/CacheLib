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

#include <folly/coro/GtestHelpers.h>
#include <gtest/gtest.h>

#include "cachelib/interface/CacheComponent.h"

using namespace ::testing;
using namespace facebook::cachelib::interface;

class TestCacheItem : public CacheItem {
 public:
  TestCacheItem(Key key,
                uint32_t valueSize,
                uint32_t creationTime = 0,
                uint32_t ttlSecs = 0)
      : creationTime_(creationTime),
        expiryTime_(creationTime + ttlSecs),
        key_(key),
        memory_(valueSize),
        refCount_(0) {}

  uint32_t getCreationTime() const noexcept override { return creationTime_; }
  uint32_t getExpiryTime() const noexcept override { return expiryTime_; }
  void incrementRefCount() noexcept override { ++refCount_; }
  bool decrementRefCount() noexcept override { return --refCount_ == 0; }
  Key getKey() const noexcept override { return Key(key_); }
  void* getMemory() const noexcept override {
    return const_cast<uint8_t*>(memory_.data());
  }
  uint32_t getMemorySize() const noexcept override { return memory_.size(); }
  uint32_t getTotalSize() const noexcept override {
    // Note: this doesn't try to account for SSO
    return sizeof(*this) + key_.size() + memory_.size();
  }

 private:
  uint32_t creationTime_;
  uint32_t expiryTime_;
  std::string key_;
  std::vector<uint8_t> memory_;
  std::atomic_size_t refCount_;
};

class TestCacheComponent : public CacheComponent {
 public:
  // TODO make private after implementing allocate/insert
  folly::F14FastMap<std::string, TestCacheItem*> cachedItems_;

  const std::string& getName() const noexcept override {
    static const std::string name_{"TestCacheComponent"};
    return name_;
  }

 private:
  folly::coro::Task<void> release(CacheItem& item,
                                  bool /* inserted */) override {
    // No double frees
    CO_ASSERT_GT(cachedItems_.erase(item.getKey()), 0);
    delete &item;
    co_return;
  }
};

class CacheComponentTest : public ::testing::Test {
 public:
  void TearDown() override { EXPECT_TRUE(cache_.cachedItems_.empty()); }

  template <typename HandleT>
  void checkItemFields(const HandleT& handle) {
    ASSERT_TRUE(handle);
    EXPECT_EQ(handle->getCreationTime(), 1000);
    EXPECT_EQ(handle->getExpiryTime(), 1010);
    EXPECT_EQ(handle->getKey(), this->key_);
    EXPECT_EQ(handle->getKeySize(), this->key_.size());
    EXPECT_EQ(
        std::memcmp(handle->getMemory(), this->data_, sizeof(this->data_)), 0);
    EXPECT_EQ(handle->getMemorySize(), 128);
    EXPECT_EQ(handle->getTotalSize(),
              sizeof(TestCacheItem) + this->key_.size() + 128);
  }

  std::string keyData_{"test_key"};
  Key key_{keyData_};
  const char data_[16]{"my test data"};
  TestCacheComponent cache_;
};

template <typename HandleType>
class HandleTest : public CacheComponentTest {
 public:
  TestCacheItem* makeItem() {
    auto* item = new TestCacheItem(key_,
                                   /* valueSize */ 128,
                                   /* creationTime */ 1000,
                                   /* ttlSecs */ 10);
    std::memcpy(item->getMemory(), data_, sizeof(data_));
    cache_.cachedItems_.try_emplace(key_, item);
    return item;
  }

  void checkReleased() { EXPECT_FALSE(cache_.cachedItems_.contains(key_)); }
};

using HandleTypes = ::testing::Types<WriteHandle, AllocatedHandle, ReadHandle>;
TYPED_TEST_SUITE(HandleTest, HandleTypes);

TYPED_TEST(HandleTest, basic) {
  auto* item = this->makeItem();
  {
    TypeParam handle(this->cache_, *item);
    this->checkItemFields(handle);
  }
  this->checkReleased();
}

TYPED_TEST(HandleTest, move) {
  auto* item = this->makeItem();
  {
    TypeParam handle1(this->cache_, *item);
    TypeParam handle2(std::move(handle1));
    EXPECT_FALSE(handle1);
    this->checkItemFields(handle2);
  }
  this->checkReleased();
}
