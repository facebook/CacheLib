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
#include "cachelib/interface/tests/Utils.h"

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
  const std::string& getName() const noexcept override {
    static const std::string name_{"TestCacheComponent"};
    return name_;
  }

  folly::coro::Task<Result<AllocatedHandle>> allocate(
      Key key,
      uint32_t size,
      uint32_t creationTime,
      uint32_t ttlSecs) override {
    auto& allocated = allocatedItems_.emplace_back(
        new TestCacheItem(key, size, creationTime, ttlSecs));
    co_return AllocatedHandle(*this, *allocated);
  }

  folly::coro::Task<UnitResult> insert(AllocatedHandle&& handle) override {
    auto itemIt = findAllocatedItem(handle.get());
    if (itemIt == allocatedItems_.end()) {
      co_return makeError(Error::Code::INVALID_ARGUMENTS,
                          "item was not allocated");
    }

    auto [_, inserted] = cachedItems_.try_emplace(handle->getKey(), *itemIt);
    if (!inserted) {
      co_return makeError(Error::Code::ALREADY_INSERTED, "duplicate key");
    }

    allocatedItems_.erase(itemIt);
    releaseHandle(std::move(handle));
    co_return folly::unit;
  }

  folly::coro::Task<Result<std::optional<AllocatedHandle>>> insertOrReplace(
      AllocatedHandle&& handle) override {
    auto itemIt = findAllocatedItem(handle.get());
    if (itemIt == allocatedItems_.end()) {
      co_return makeError(Error::Code::INVALID_ARGUMENTS,
                          "item was not allocated");
    }

    auto [it, inserted] = cachedItems_.try_emplace(handle->getKey(), *itemIt);
    releaseHandle(std::move(handle));
    if (!inserted) {
      // Replaced item is moved back to the allocated list
      std::swap(it->second, *itemIt);
      (*itemIt)->decrementRefCount();
      co_return AllocatedHandle(*this, **itemIt);
    }

    allocatedItems_.erase(itemIt);
    co_return std::nullopt;
  }

  template <typename HandleT>
  std::optional<HandleT> findImpl(Key key) {
    auto it = cachedItems_.find(key);
    if (it != cachedItems_.end()) {
      return HandleT(*this, *it->second);
    }
    return std::nullopt;
  }

  folly::coro::Task<Result<std::optional<ReadHandle>>> find(Key key) override {
    co_return findImpl<ReadHandle>(key);
  }

  folly::coro::Task<Result<std::optional<WriteHandle>>> findToWrite(
      Key key) override {
    co_return findImpl<WriteHandle>(key);
  }

  folly::coro::Task<Result<bool>> remove(Key key) override {
    auto itemIt = cachedItems_.find(key);
    if (itemIt == cachedItems_.end()) {
      co_return false;
    }
    if (itemIt->second->decrementRefCount()) {
      delete itemIt->second;
    }
    removedItems_.emplace(itemIt->first);
    cachedItems_.erase(itemIt);
    co_return true;
  }

  folly::coro::Task<UnitResult> remove(ReadHandle&& handle) override {
    auto itemIt = cachedItems_.find(handle->getKey());
    if (itemIt == cachedItems_.end()) {
      co_return makeError(Error::Code::INVALID_ARGUMENTS,
                          "item not found in cache");
    }
    // We want need to make the handle unusable but we also need to remove its
    // ownership. Decrement the refcount twice - once for the handle, once for
    // the component.
    releaseHandle(std::move(handle));
    itemIt->second->decrementRefCount();
    if (itemIt->second->decrementRefCount()) {
      delete itemIt->second;
    }
    removedItems_.emplace(itemIt->first);
    cachedItems_.erase(itemIt);
    co_return folly::unit;
  }

  /**
   * Helper to find an item in the allocated list.
   * @param item the item to find
   * @return an iterator to the item if found, otherwise end()
   */
  std::vector<TestCacheItem*>::iterator findAllocatedItem(
      const CacheItem* item) {
    return std::find(allocatedItems_.begin(), allocatedItems_.end(), item);
  }

  size_t writeBacks_{0};
  std::vector<TestCacheItem*> allocatedItems_;
  folly::F14FastMap<std::string, TestCacheItem*> cachedItems_;
  folly::F14FastSet<std::string> removedItems_;

 private:
  UnitResult writeBack(CacheItem& /* item */) override {
    writeBacks_++;
    return folly::unit;
  }

  folly::coro::Task<void> release(CacheItem& item, bool inserted) override {
    if (inserted) {
      CO_ASSERT_TRUE(cachedItems_.erase(item.getKey()) > 0 ||
                     removedItems_.contains(item.getKey()));
    } else {
      auto it = findAllocatedItem(&item);
      CO_ASSERT_NE(it, allocatedItems_.end());
      allocatedItems_.erase(it);
    }
    delete &item;
    co_return;
  }
};

class InterfaceTest : public ::testing::Test {
 public:
  void TearDown() override {
    EXPECT_TRUE(cache_.allocatedItems_.empty());
    EXPECT_TRUE(cache_.cachedItems_.empty());
  }

  folly::coro::Task<void> allocateAndInsertItem() {
    auto allocHandle = ASSERT_OK(co_await cache_.allocate(
        key_, /* valueSize */ 128, /* creationTime */ 1000, /* ttl */ 10));
    memcpy(allocHandle->getMemory(), data_, sizeof(data_));
    EXPECT_OK(co_await cache_.insert(std::move(allocHandle)));
  }

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
class HandleTest : public InterfaceTest {
 public:
  template <typename HandleT>
  TestCacheItem* makeItem() {
    auto* item = new TestCacheItem(key_,
                                   /* valueSize */ 128,
                                   /* creationTime */ 1000,
                                   /* ttlSecs */ 10);
    std::memcpy(item->getMemory(), data_, sizeof(data_));
    if constexpr (std::is_same_v<HandleT, AllocatedHandle>) {
      cache_.allocatedItems_.emplace_back(item);
    } else {
      cache_.cachedItems_.try_emplace(key_, item);
    }
    return item;
  }

  template <typename HandleT>
  void checkReleased(TestCacheItem* item, bool removedFromCache = true) {
    // Note: can't deref item since it's been deleted
    if constexpr (std::is_same_v<HandleT, AllocatedHandle>) {
      EXPECT_EQ(cache_.findAllocatedItem(item), cache_.allocatedItems_.end());
    } else {
      EXPECT_FALSE(cache_.cachedItems_.contains(key_));
      if (removedFromCache) {
        EXPECT_TRUE(cache_.removedItems_.contains(key_));
      }
    }
  }
};

using HandleTypes = ::testing::Types<WriteHandle, AllocatedHandle, ReadHandle>;
TYPED_TEST_SUITE(HandleTest, HandleTypes);

TYPED_TEST(HandleTest, basic) {
  auto* item = this->template makeItem<TypeParam>();
  {
    TypeParam handle(this->cache_, *item);
    this->checkItemFields(handle);
  }
  this->template checkReleased<TypeParam>(item, /* removedFromCache */ false);
}

TYPED_TEST(HandleTest, move) {
  auto* item = this->template makeItem<TypeParam>();
  {
    TypeParam handle1(this->cache_, *item);
    TypeParam handle2(std::move(handle1));
    EXPECT_FALSE(handle1); // NOLINT(bugprone-use-after-move)
    this->checkItemFields(handle2);
  }
  this->template checkReleased<TypeParam>(item, /* removedFromCache */ false);
}

CO_TEST_F(InterfaceTest, basic) {
  co_await allocateAndInsertItem();

  auto readHandle = ASSERT_OK(co_await cache_.find(key_));
  CO_ASSERT_TRUE(readHandle.has_value());
  checkItemFields(readHandle.value());

  {
    auto writeHandle = ASSERT_OK(co_await cache_.findToWrite(key_));
    CO_ASSERT_TRUE(writeHandle.has_value());
    checkItemFields(writeHandle.value());
    writeHandle->markDirty();
  }
  EXPECT_EQ(cache_.writeBacks_, 1);

  auto removed = ASSERT_OK(co_await cache_.remove(key_));
  EXPECT_TRUE(removed);
  EXPECT_FALSE(this->cache_.cachedItems_.contains(this->key_));
}

CO_TEST_F(InterfaceTest, replace) {
  co_await allocateAndInsertItem();

  auto allocHandle = ASSERT_OK(co_await cache_.allocate(
      key_, /* valueSize */ 128, /* creationTime */ 1000, /* ttl */ 10));
  const char data2[]{"my test data 2"};
  memcpy(allocHandle->getMemory(), data2, sizeof(data2));
  auto oldHandleOpt =
      ASSERT_OK(co_await cache_.insertOrReplace(std::move(allocHandle)));

  CO_ASSERT_TRUE(oldHandleOpt.has_value());
  const auto& oldHandle = oldHandleOpt.value();
  checkItemFields(oldHandle);
  EXPECT_NE(this->cache_.cachedItems_[this->key_], oldHandle.get());

  auto readHandleOpt = ASSERT_OK(co_await cache_.find(key_));
  CO_ASSERT_TRUE(readHandleOpt.has_value());
  auto& readHandle = readHandleOpt.value();
  EXPECT_EQ(std::memcmp(readHandle->getMemory(), data2, sizeof(data2)), 0);

  ASSERT_OK(co_await cache_.remove(std::move(readHandle)));
}

CO_TEST_F(InterfaceTest, removeHandle) {
  co_await allocateAndInsertItem();
  auto readHandle = ASSERT_OK(co_await cache_.find(key_));
  ASSERT_OK(co_await cache_.remove(std::move(readHandle.value())));
  EXPECT_FALSE(this->cache_.cachedItems_.contains(this->key_));
}

CO_TEST_F(InterfaceTest, duplicate) {
  co_await allocateAndInsertItem();
  auto allocHandle = ASSERT_OK(co_await cache_.allocate(
      key_, /* valueSize */ 128, /* creationTime */ 1000, /* ttl */ 10));
  EXPECT_ERROR(co_await cache_.insert(std::move(allocHandle)),
               Error::Code::ALREADY_INSERTED);
  EXPECT_OK(co_await cache_.remove(key_));
}
