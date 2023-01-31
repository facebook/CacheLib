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
#include <gmock/gmock.h>

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/tests/DataTypeTest.h"

namespace facebook {
namespace cachelib {
namespace detail {
void testBufferSlot() {
  using facebook::cachelib::detail::Buffer;
  Buffer::Slot slot(100);
  ASSERT_EQ(100, slot.getSize());
  ASSERT_EQ(Buffer::Slot::getAllocSize(100), slot.getAllocSize());
  ASSERT_FALSE(slot.isRemoved());
  slot.markRemoved();
  ASSERT_TRUE(slot.isRemoved());
}
} // namespace detail

namespace tests {
TEST(Buffer, Slot) { detail::testBufferSlot(); }

TEST(Buffer, Basic) {
  using facebook::cachelib::detail::Buffer;
  auto buf = std::make_unique<uint8_t[]>(Buffer::computeStorageSize(100));

  Buffer* buffer = new (buf.get()) Buffer(100);
  ASSERT_EQ(100, buffer->capacity());

  const uint32_t allocOffset1 = buffer->allocate(30);
  ASSERT_EQ(100, buffer->capacity());
  ASSERT_EQ(100 - Buffer::getAllocSize(30), buffer->remainingBytes());
  ASSERT_EQ(0, buffer->wastedBytes());
  ASSERT_TRUE(buffer->getData(allocOffset1));

  const uint32_t allocOffset2 = buffer->allocate(60);
  ASSERT_EQ(100, buffer->capacity());
  ASSERT_EQ(100 - Buffer::getAllocSize(30) - Buffer::getAllocSize(60),
            buffer->remainingBytes());
  ASSERT_EQ(0, buffer->wastedBytes());
  ASSERT_TRUE(buffer->getData(allocOffset2));

  // No more memory to allocate
  ASSERT_EQ(Buffer::kInvalidOffset, buffer->allocate(20));

  // Test deletion
  buffer->remove(allocOffset2);
  ASSERT_EQ(100 - Buffer::getAllocSize(30) - Buffer::getAllocSize(60),
            buffer->remainingBytes());
  ASSERT_EQ(Buffer::getAllocSize(60), buffer->wastedBytes());
  ASSERT_EQ(100, buffer->capacity());

  // Still cannot allocate because deletion does not reclaim space
  ASSERT_EQ(Buffer::kInvalidOffset, buffer->allocate(20));
}

TEST(Buffer, Compaction) {
  using facebook::cachelib::detail::Buffer;

  auto buf = std::make_unique<uint8_t[]>(Buffer::computeStorageSize(100));
  Buffer* buffer = new (buf.get()) Buffer(100);
  ASSERT_EQ(100, buffer->capacity());

  const uint32_t allocOffset1 = buffer->allocate(25);
  ASSERT_NE(Buffer::kInvalidOffset, allocOffset1);
  ASSERT_NE(Buffer::kInvalidOffset, buffer->allocate(25));
  ASSERT_NE(Buffer::kInvalidOffset, buffer->allocate(25));

  // Full, so we cannot allocate anymore
  ASSERT_FALSE(buffer->canAllocate(25));
  ASSERT_EQ(Buffer::kInvalidOffset, buffer->allocate(25));

  // Remove an existing allocation, but verify we still cannot allocate
  buffer->remove(allocOffset1);
  ASSERT_TRUE(buffer->canAllocate(25));
  ASSERT_EQ(Buffer::kInvalidOffset, buffer->allocate(25));

  // Run compaction. Verify we can allocate now
  {
    auto buf2 = std::make_unique<uint8_t[]>(Buffer::computeStorageSize(100));
    Buffer* buffer2 = new (buf2.get()) Buffer(100);
    ASSERT_EQ(100, buffer2->capacity());

    ASSERT_NO_THROW(buffer->compact(*buffer2));

    // Verify new buffer has room for this allocation now
    ASSERT_NE(Buffer::kInvalidOffset, buffer2->allocate(25));
  }

  // Run compaction on a buffer just small enough will also succeed
  {
    auto buf2 = std::make_unique<uint8_t[]>(Buffer::computeStorageSize(75));
    Buffer* buffer2 = new (buf2.get()) Buffer(75);
    ASSERT_EQ(75, buffer2->capacity());
    ASSERT_NO_THROW(buffer->compact(*buffer2));

    // Too small to make this allocation
    ASSERT_EQ(Buffer::kInvalidOffset, buffer2->allocate(25));
  }

  // Run compaction on a buffer too small will throw
  {
    auto buf2 = std::make_unique<uint8_t[]>(Buffer::computeStorageSize(50));
    Buffer* buffer2 = new (buf2.get()) Buffer(50);
    ASSERT_EQ(50, buffer2->capacity());
    ASSERT_THROW(buffer->compact(*buffer2), std::invalid_argument);
  }
}

template <typename AllocatorT>
class BufferManagerTest : public ::testing::Test {
 public:
  void testFixedSize() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);
    auto parent = cache->allocate(pid, "my_parent", 0);

    using BufferManager = detail::BufferManager<AllocatorT>;
    auto mgr = BufferManager{*cache, parent, 1000};

    // Allocate until we have several chained items
    auto addr1 = mgr.allocate(100);
    ASSERT_NE(nullptr, addr1);
    ASSERT_EQ(0, addr1.getItemOffset());
    ASSERT_EQ(detail::Buffer::getAllocSize(0), addr1.getByteOffset());
    ASSERT_EQ(1000 - detail::Buffer::getAllocSize(100), mgr.remainingBytes());
    ASSERT_EQ(0, mgr.wastedBytes());

    while (true) {
      auto addr = mgr.allocate(100);
      if (!addr) {
        mgr.expand(100);
        addr = mgr.allocate(100);
      }
      ASSERT_NE(nullptr, addr);
      ASSERT_EQ(0, mgr.wastedBytes());

      // Stop when we get our allocation from the second chained item
      if (addr.getItemOffset() == 1) {
        break;
      }
    }

    ASSERT_NO_THROW(mgr.remove(addr1));
    ASSERT_EQ(detail::Buffer::getAllocSize(100), mgr.wastedBytes());
    // The remaining capacity is the left-over space from the first buffer,
    // and the second buffer (which we just made a single allocation).
    ASSERT_EQ(
        BufferManager::kMaxBufferCapacity % detail::Buffer::getAllocSize(100) +
            BufferManager::kMaxBufferCapacity -
            detail::Buffer::getAllocSize(100),
        mgr.remainingBytes());

    auto addr2 = mgr.allocate(100);
    ASSERT_NE(nullptr, addr2);
    ASSERT_EQ(1, addr2.getItemOffset());
    ASSERT_EQ(detail::Buffer::getAllocSize(100), mgr.wastedBytes());
  }

  void testVariableSize() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);
    auto parent = cache->allocate(pid, "my_parent", 0);

    const uint32_t loopLimit = 100 * 1000;

    using BufferManager = detail::BufferManager<AllocatorT>;
    auto mgr = BufferManager{*cache, parent, 1000};

    // Allocate variable size until we're at two chained items,
    // free all of them, and allocate again
    std::vector<std::pair<detail::BufferAddr, uint32_t>> allocs;
    bool succeed = false;
    for (uint32_t i = 0; i < loopLimit; ++i) {
      const uint32_t size = 100 + folly::Random::rand32() % 1000;
      auto addr = mgr.allocate(size);
      if (!addr) {
        mgr.expand(size);
        addr = mgr.allocate(size);
      }
      ASSERT_NE(nullptr, addr);
      allocs.push_back(std::make_pair(addr, size));
      if (addr.getItemOffset() == 1) {
        succeed = true;
        break;
      }
    }
    ASSERT_TRUE(succeed);

    // Delete all existing allocations
    for (auto& alloc : allocs) {
      ASSERT_NO_THROW(mgr.remove(alloc.first));
    }
    allocs.clear();

    // Keep allocating until a third chained item is allocated
    succeed = false;
    for (uint32_t i = 0; i < loopLimit; ++i) {
      const uint32_t size = 100 + folly::Random::rand32() % 1000;
      auto addr = mgr.allocate(size);
      if (!addr) {
        mgr.expand(size);
        addr = mgr.allocate(size);
      }
      ASSERT_NE(nullptr, addr);
      if (addr.getItemOffset() == 2) {
        succeed = true;
        break;
      }
    }
    ASSERT_TRUE(succeed);

    ASSERT_NO_THROW(mgr.compact());

    succeed = false;
    for (uint32_t i = 0; i < loopLimit; ++i) {
      const uint32_t size = 100 + folly::Random::rand32() % 1000;
      auto addr = mgr.allocate(size);
      if (!addr) {
        mgr.expand(size);
        addr = mgr.allocate(size);
      }
      ASSERT_NE(nullptr, addr);
      if (addr.getItemOffset() == 0) {
        succeed = true;
        break;
      }
    }
    ASSERT_TRUE(succeed);
  }

  void testUpperBound() {
    typename AllocatorT::Config config;
    config.configureChainedItems();
    config.setCacheSize(400 * Slab::kSize);
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    const auto pid = cache->addPool("default", numBytes);

    auto parent = cache->allocate(pid, "my_parent", 0);

    // Allocate until we cannot allocate anymore and verify we have reached
    // kMaxNumChainedItems for the length of our chain.
    {
      using BufferManager = detail::BufferManager<AllocatorT>;
      auto mgr = BufferManager{*cache, parent, 1000};

      while (true) {
        auto addr = mgr.allocate(100 * 1000);
        if (!addr) {
          mgr.expand(100 * 1000);
          addr = mgr.allocate(100 * 1000);
        }
        if (!addr) {
          break;
        }
      }
    }

    auto allocs = cache->viewAsChainedAllocs(std::move(parent));
    ASSERT_EQ(detail::BufferAddr::kMaxNumChainedItems,
              allocs.computeChainLength());
  }

  void testMaxBufferSize() {
    typename AllocatorT::Config config;
    config.configureChainedItems();
    config.setCacheSize(400 * Slab::kSize);
    config.setDefaultAllocSizes(util::generateAllocSizes(2, 1024 * 1024));
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    const auto pid = cache->addPool("default", numBytes);
    auto parent = cache->allocate(pid, "my_parent", 0);

    using BufferManager = detail::BufferManager<AllocatorT>;
    auto mgr = BufferManager{*cache, parent, 1000};
    ASSERT_TRUE(mgr.expand(1024 * 1024 - detail::Buffer::getAllocSize(100)));
    auto addr = mgr.allocate(1024 * 1024 - detail::Buffer::getAllocSize(100) -
                             sizeof(detail::Buffer));
    ASSERT_TRUE(addr);
    EXPECT_EQ(1, addr.getItemOffset());
    EXPECT_EQ(4, addr.getByteOffset());
  }

  void testClone() {
    typename AllocatorT::Config config;
    config.configureChainedItems();
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    const auto pid = cache->addPool("default", numBytes);
    const std::string data = "abcdefghijklmnopqrstuvwxyz";

    auto parent = cache->allocate(pid, "parent", 0);
    ASSERT_NE(nullptr, parent);

    auto item1 = cache->allocateChainedItem(parent, 1);
    ASSERT_NE(nullptr, item1);
    std::memcpy(item1->getMemory(), data.data(), 1);
    cache->addChainedItem(parent, std::move(item1));

    auto item2 = cache->allocateChainedItem(parent, 2);
    ASSERT_NE(nullptr, item2);
    std::memcpy(item2->getMemory(), data.data() + 1, 2);
    cache->addChainedItem(parent, std::move(item2));

    using BufferManager = detail::BufferManager<AllocatorT>;
    auto mgr = BufferManager{*cache, parent};

    auto newParent = cache->allocate(pid, "new_parent", 0);
    ASSERT_NE(nullptr, newParent);
    auto mgrClone = mgr.clone(newParent);
    ASSERT_FALSE(mgrClone.empty());

    EXPECT_EQ(2, mgr.buffers_.size());
    EXPECT_EQ(2, mgrClone.buffers_.size());

    EXPECT_EQ(1, mgr.buffers_[0]->getSize());
    EXPECT_EQ(1, mgrClone.buffers_[0]->getSize());
    EXPECT_EQ(0, memcmp(mgr.buffers_[0]->getMemory(), data.data(), 1));
    EXPECT_EQ(0, memcmp(mgrClone.buffers_[0]->getMemory(), data.data(), 1));
    EXPECT_NE(mgr.buffers_[0]->getMemory(), mgrClone.buffers_[0]->getMemory());

    EXPECT_EQ(2, mgr.buffers_[1]->getSize());
    EXPECT_EQ(2, mgrClone.buffers_[1]->getSize());
    EXPECT_EQ(0, memcmp(mgr.buffers_[1]->getMemory(), data.data() + 1, 2));
    EXPECT_EQ(0, memcmp(mgrClone.buffers_[1]->getMemory(), data.data() + 1, 2));
    EXPECT_NE(mgr.buffers_[1]->getMemory(), mgrClone.buffers_[1]->getMemory());
  }

  void testInitialCapacity() {
    typename AllocatorT::Config config;
    config.setCacheSize(10 * Slab::kSize);
    config.setDefaultAllocSizes(util::generateAllocSizes(2, 1024 * 1024));
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    const auto pid = cache->addPool("default", numBytes);

    using BufferManager = detail::BufferManager<AllocatorT>;

    {
      auto parent = cache->allocate(pid, "my_parent", 0);
      ASSERT_NO_THROW(
          (BufferManager{*cache, parent, BufferManager::kMaxBufferCapacity}));
    }
    {
      auto parent = cache->allocate(pid, "my_parent", 0);
      ASSERT_THROW((BufferManager{*cache, parent,
                                  BufferManager::kMaxBufferCapacity + 1}),
                   std::invalid_argument);
    }
    {
      // Allocate all memory so buffer manager creation will fail
      std::vector<typename AllocatorT::WriteHandle> handles;
      for (int i = 0;; i++) {
        auto handle = cache->allocate(pid,
                                      folly::sformat("key_{}", i),
                                      BufferManager::kMaxBufferCapacity);
        if (!handle) {
          break;
        }
        handles.push_back(std::move(handle));
      }
      auto parent = cache->allocate(pid, "my_parent", 0);
      ASSERT_THROW(
          (BufferManager{*cache, parent, BufferManager::kMaxBufferCapacity}),
          cachelib::exception::OutOfMemory);
    }
  }
};
TYPED_TEST_CASE(BufferManagerTest, AllocatorTypes);
TYPED_TEST(BufferManagerTest, FixedSize) { this->testFixedSize(); }
TYPED_TEST(BufferManagerTest, VariableSize) { this->testVariableSize(); }
TYPED_TEST(BufferManagerTest, UpperBound) { this->testUpperBound(); }
TYPED_TEST(BufferManagerTest, Clone) { this->testClone(); }
TYPED_TEST(BufferManagerTest, MaxBufferSize) { this->testMaxBufferSize(); }
TYPED_TEST(BufferManagerTest, InitialCapacity) { this->testInitialCapacity(); }
} // namespace tests
} // namespace cachelib
} // namespace facebook
