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

#include "cachelib/compact_cache/tests/CCacheTests.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/TestUtils.h"
#include "cachelib/compact_cache/allocators/TestAllocator.h"

namespace facebook {
namespace cachelib {
namespace tests {

struct CACHELIB_PACKED_ATTR Int {
  int value;
  /* implicit */ Int(int v = 0) : value(v) {}
  bool operator==(const Int& other) const { return value == other.value; }
  bool operator!=(const Int& other) const { return value != other.value; }
  bool isEmpty() const { return value == 0; }
};

template <size_t SIZE>
struct CACHELIB_PACKED_ATTR Buffer {
  std::array<uint8_t, SIZE> value;
  /* implicit */ Buffer(int v = 0) {
    // In order to maintain a large space of keys for some tests (e.g.
    // testPurgeCallback), we apply the int to the buffer 8 bytes by 8 bytes
    for (unsigned long i = 0; i < SIZE; i += sizeof(int)) {
      memcpy(&value[i], &v, i + sizeof(int) <= SIZE ? sizeof(int) : SIZE - i);
    }
  }
  bool operator==(const Buffer& other) const { return value == other.value; }
  bool operator!=(const Buffer& other) const { return value != other.value; }
  bool isEmpty() const { return *this == Buffer(); }
};

template <typename T>
class CompactCacheTests : public ::testing::Test {
 public:
  void testInt2Empty() {
    using CC = typename CCacheCreator<T, Int>::type;
    CompactCacheRunBasicTests<CC>();
  }

  void testInt2Int() {
    using CC = typename CCacheCreator<T, Int, Int>::type;
    CompactCacheRunBasicTests<CC>();
  }

  void testInt2Str() {
    using CC = typename CCacheCreator<T, Int, Buffer<51>>::type;
    CompactCacheRunBasicTests<CC>();
  }

  void testStr2Empty() {
    using CC = typename CCacheCreator<T, Buffer<67>>::type;
    CompactCacheRunBasicTests<CC>();
  }

  void testStr2Int() {
    using CC = typename CCacheCreator<T, Buffer<17>, Int>::type;
    CompactCacheRunBasicTests<CC>();
  }

  void testStr2Str() {
    using CC = typename CCacheCreator<T, Buffer<93>, Buffer<13>>::type;
    CompactCacheRunBasicTests<CC>();
  }
};

using Allocators = ::testing::Types<TestAllocator>;
TYPED_TEST_CASE(CompactCacheTests, Allocators);

TYPED_TEST(CompactCacheTests, Int2Empty) { this->testInt2Empty(); }

TYPED_TEST(CompactCacheTests, Int2Int) { this->testInt2Int(); }

TYPED_TEST(CompactCacheTests, Int2Str) { this->testInt2Str(); }

TYPED_TEST(CompactCacheTests, Str2Empty) { this->testStr2Empty(); }

TYPED_TEST(CompactCacheTests, Str2Int) { this->testStr2Int(); }

TYPED_TEST(CompactCacheTests, Str2Str) { this->testStr2Str(); }

template <typename T>
class CompactCacheAllocatorTests : public ::testing::Test {};

using CompactCacheTypes = ::testing::Types<
    typename CCacheCreator<CCacheAllocator, Int>::type,
    typename CCacheCreator<CCacheAllocator, Int, Int>::type,
    typename CCacheCreator<CCacheAllocator, Int, Buffer<51>>::type,
    typename CCacheCreator<CCacheAllocator, Buffer<67>>::type,
    typename CCacheCreator<CCacheAllocator, Buffer<17>, Int>::type,
    typename CCacheCreator<CCacheAllocator, Buffer<93>, Buffer<13>>::type>;
TYPED_TEST_CASE(CompactCacheAllocatorTests, CompactCacheTypes);

TYPED_TEST(CompactCacheAllocatorTests, warmroll) {
  const folly::StringPiece poolName = "test";

  LruAllocator::Config config;
  config.size = 2 * Slab::kSize;
  config.enableCompactCache();
  config.cacheDir =
      "/tmp/ccache_warmroll_" + folly::to<std::string>(folly::Random::rand32());

  for (int i = 1; i < 10; ++i) {
    std::unique_ptr<LruAllocator> cacheAllocator =
        i == 1
            ? std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, config)
            : std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach,
                                             config);
    auto& ccache =
        i == 1
            ? *cacheAllocator->addCompactCache<TypeParam>(poolName, Slab::kSize)
            : *cacheAllocator->attachCompactCache<TypeParam>(poolName);

    // check all key from 1 to i-2 does not exist
    for (int j = 1; j < i - 1; ++j) {
      ASSERT_EQ(CCacheReturn::NOTFOUND, ccache.get(j));
    }

    // check key i-1 exists then delete it if i > 1
    if (i > 1) {
      typename TypeParam::Value out;
      ASSERT_EQ(CCacheReturn::FOUND, ccache.get(i - 1, &out));
      ASSERT_EQ(typename TypeParam::Value(i - 1), out);
      ASSERT_EQ(CCacheReturn::FOUND, ccache.del(i - 1));
    }

    // add key i into ccache
    typename TypeParam::Value value(i);
    typename TypeParam::Value out;
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache.get(i));
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache.set(i, &value));
    ASSERT_EQ(CCacheReturn::FOUND, ccache.get(i, &out));
    ASSERT_EQ(value, out);

    cacheAllocator->shutDown();
  }
}

TYPED_TEST(CompactCacheAllocatorTests, resize) {
  LruAllocator::Config config;
  config.size = 4 * Slab::kSize;
  config.poolResizeInterval = std::chrono::seconds(1);
  config.enableCompactCache();
  config.cacheDir =
      "/tmp/ccache_resize_" + folly::to<std::string>(folly::Random::rand32());

  typename TypeParam::Value v1(1);
  typename TypeParam::Value v2(2);
  typename TypeParam::Value out;

  {
    std::unique_ptr<LruAllocator> cacheAllocator =
        std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, config);

    auto& ccache1 =
        *cacheAllocator->addCompactCache<TypeParam>("1", 2 * Slab::kSize);
    ASSERT_EQ(2 * Slab::kSize, ccache1.getSize());
    auto& ccache2 =
        *cacheAllocator->addCompactCache<TypeParam>("2", 1 * Slab::kSize);
    ASSERT_EQ(1 * Slab::kSize, ccache2.getSize());

    ccache1.set(1, &v1);
    ASSERT_EQ(CCacheReturn::FOUND, ccache1.get(1, &out));
    ASSERT_EQ(v1, out);

    ccache2.set(2, &v2);
    ASSERT_EQ(CCacheReturn::FOUND, ccache2.get(2, &out));
    ASSERT_EQ(v2, out);

    ASSERT_FALSE(cacheAllocator->growPool(cacheAllocator->getPoolId("2"),
                                          1 * Slab::kSize));
    ASSERT_TRUE(cacheAllocator->shrinkPool(cacheAllocator->getPoolId("1"),
                                           1 * Slab::kSize));
    ASSERT_TRUE(cacheAllocator->growPool(cacheAllocator->getPoolId("2"),
                                         1 * Slab::kSize));

    // Wait for poolResizer
    ASSERT_EVENTUALLY_TRUE([&] {
      return ccache1.getSize() == 1 * Slab::kSize &&
             ccache2.getSize() == 2 * Slab::kSize;
    });

    ASSERT_EQ(CCacheReturn::FOUND, ccache1.get(1, &out));
    ASSERT_EQ(v1, out);
    ASSERT_EQ(CCacheReturn::FOUND, ccache2.get(2, &out));
    ASSERT_EQ(v2, out);

    cacheAllocator->shutDown();
  }

  {
    // do a warmroll
    std::unique_ptr<LruAllocator> cacheAllocator =
        std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach, config);

    // compact caches should keep the same size as before warmroll
    auto& ccache1 = *cacheAllocator->attachCompactCache<TypeParam>(
        "1", /* auto-resize */ false);
    ASSERT_EQ(1 * Slab::kSize, ccache1.getSize());
    auto& ccache2 = *cacheAllocator->attachCompactCache<TypeParam>(
        "2", /* auto-resize */ false);
    ASSERT_EQ(2 * Slab::kSize, ccache2.getSize());

    ASSERT_EQ(CCacheReturn::FOUND, ccache1.get(1, &out));
    ASSERT_EQ(v1, out);
    ASSERT_EQ(CCacheReturn::FOUND, ccache2.get(2, &out));
    ASSERT_EQ(v2, out);

    ASSERT_TRUE(cacheAllocator->resizePools(cacheAllocator->getPoolId("2"),
                                            cacheAllocator->getPoolId("1"),
                                            1 * Slab::kSize));

    // Wait for poolResizer
    ASSERT_EVENTUALLY_TRUE([&] {
      return ccache1.getSize() == 2 * Slab::kSize &&
             ccache2.getSize() == 1 * Slab::kSize;
    });

    ASSERT_EQ(CCacheReturn::FOUND, ccache1.get(1, &out));
    ASSERT_EQ(v1, out);
    ASSERT_EQ(CCacheReturn::FOUND, ccache2.get(2, &out));
    ASSERT_EQ(v2, out);

    // Disable the ccache2
    ASSERT_TRUE(cacheAllocator->shrinkPool(cacheAllocator->getPoolId("2"),
                                           1 * Slab::kSize));
    ASSERT_EVENTUALLY_TRUE([&] { return ccache2.getSize() == 0; });
    ASSERT_EQ(CCacheReturn::ERROR, ccache2.get(2, &out));

    // enable it again
    ASSERT_TRUE(cacheAllocator->growPool(cacheAllocator->getPoolId("2"),
                                         1 * Slab::kSize));
    ASSERT_EVENTUALLY_TRUE(
        [&] { return ccache2.getSize() == 1 * Slab::kSize; });
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache2.get(2, &out));

    cacheAllocator->shutDown();
  }

  {
    // do a warmroll
    std::unique_ptr<LruAllocator> cacheAllocator =
        std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach, config);

    // compact caches should keep the same size as before warmroll
    auto& ccache1 = *cacheAllocator->attachCompactCache<TypeParam>(
        "1", /* auto-resize */ false);
    ASSERT_EQ(2 * Slab::kSize, ccache1.getSize());

    // disable ccache2
    cacheAllocator->shrinkPool(cacheAllocator->getPoolId("2"), 1 * Slab::kSize);

    // add a regular pool
    auto pid = cacheAllocator->addPool("regular", 1 * Slab::kSize);

    bool testDone = false;

    // have a thread allocate the memory all the time
    std::thread t([&]() {
      while (!testDone) {
        util::allocateAccessible(
            *cacheAllocator, pid,
            folly::to<std::string>(folly::Random::rand32()), 10000);
      }
    });

    ASSERT_EVENTUALLY_TRUE([&] {
      return cacheAllocator->getPool(pid).getCurrentUsedSize() ==
             1 * Slab::kSize;
    });

    // move 1 slab from ccache1 to regular
    ASSERT_TRUE(cacheAllocator->resizePools(cacheAllocator->getPoolId("1"), pid,
                                            1 * Slab::kSize));

    // Wait for poolResizer
    ASSERT_EVENTUALLY_TRUE([&] {
      return ccache1.getSize() == 1 * Slab::kSize &&
             cacheAllocator->getPool(pid).getCurrentUsedSize() ==
                 2 * Slab::kSize;
    });

    // move 1 slab from regular to ccache1
    ASSERT_TRUE(cacheAllocator->resizePools(pid, cacheAllocator->getPoolId("1"),
                                            1 * Slab::kSize));

    // Wait for poolResizer
    ASSERT_EVENTUALLY_TRUE([&] {
      return ccache1.getSize() == 2 * Slab::kSize &&
             cacheAllocator->getPool(pid).getCurrentUsedSize() ==
                 1 * Slab::kSize;
    });

    testDone = true;
    t.join();

    cacheAllocator->shutDown();
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv);
  return RUN_ALL_TESTS();
}
