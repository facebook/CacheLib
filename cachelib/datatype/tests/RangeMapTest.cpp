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

#include <algorithm>

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/RangeMap.h"
#include "cachelib/datatype/tests/DataTypeTest.h"

namespace facebook {
namespace cachelib {
namespace tests {
namespace {
detail::BufferAddr makeAddr(uint32_t itemOffset, uint32_t byteOffset) {
  return detail::BufferAddr{itemOffset, byteOffset};
}

std::unique_ptr<LruAllocator> createCache() {
  LruAllocator::Config config;
  config.setCacheSize(80 * 1024 * 1024);
  auto cache = std::make_unique<LruAllocator>(config);
  cache->addPool("default", 76 * 1024 * 1024);
  return cache;
}
} // namespace

TEST(BinaryIndex, Basic) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(10);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 10);

  EXPECT_EQ(bi->end(), bi->lookup(1));

  EXPECT_TRUE(bi->insert(1, makeAddr(1, 1)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);

  EXPECT_FALSE(bi->insert(1, makeAddr(1, 2)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);

  bi->insertOrReplace(1, makeAddr(1, 2));
  EXPECT_EQ(makeAddr(1, 2), bi->lookup(1)->addr);

  EXPECT_EQ(makeAddr(1, 2), bi->remove(1));
  EXPECT_EQ(bi->end(), bi->lookup(1));

  EXPECT_FALSE(bi->remove(0));
}

TEST(BinaryIndex, Insert) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  EXPECT_EQ(3, bi->capacity());

  EXPECT_TRUE(bi->insert(1, makeAddr(1, 1)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(1, bi->numEntries());
  EXPECT_FALSE(bi->overLimit());

  EXPECT_TRUE(bi->insert(2, makeAddr(2, 2)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(makeAddr(2, 2), bi->lookup(2)->addr);
  EXPECT_EQ(2, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());

  EXPECT_TRUE(bi->insert(3, makeAddr(3, 3)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(makeAddr(2, 2), bi->lookup(2)->addr);
  EXPECT_EQ(makeAddr(3, 3), bi->lookup(3)->addr);
  EXPECT_EQ(3, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());
}

TEST(BinaryIndex, InsertOutOfOrder) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  EXPECT_EQ(3, bi->capacity());

  EXPECT_TRUE(bi->insert(1, makeAddr(1, 1)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(1, bi->numEntries());
  EXPECT_FALSE(bi->overLimit());

  EXPECT_TRUE(bi->insert(3, makeAddr(3, 3)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(makeAddr(3, 3), bi->lookup(3)->addr);
  EXPECT_EQ(2, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());

  EXPECT_TRUE(bi->insert(2, makeAddr(2, 2)));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(makeAddr(2, 2), bi->lookup(2)->addr);
  EXPECT_EQ(makeAddr(3, 3), bi->lookup(3)->addr);
  EXPECT_EQ(3, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());
}

TEST(BinaryIndex, Replace) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  EXPECT_EQ(3, bi->capacity());

  bi->insert(1, makeAddr(1, 1));
  bi->insert(2, makeAddr(2, 2));
  EXPECT_EQ(2, bi->numEntries());

  bi->insertOrReplace(1, makeAddr(11, 11));
  bi->insertOrReplace(2, makeAddr(22, 22));
  bi->insertOrReplace(3, makeAddr(33, 33));
  EXPECT_EQ(makeAddr(11, 11), bi->lookup(1)->addr);
  EXPECT_EQ(makeAddr(22, 22), bi->lookup(2)->addr);
  EXPECT_EQ(makeAddr(33, 33), bi->lookup(3)->addr);
  EXPECT_EQ(3, bi->numEntries());
}

TEST(BinaryIndex, Remove) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  EXPECT_EQ(3, bi->capacity());

  bi->insert(1, makeAddr(1, 1));
  bi->insert(2, makeAddr(2, 2));
  bi->insert(3, makeAddr(3, 3));
  EXPECT_EQ(3, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());

  EXPECT_EQ(makeAddr(2, 2), bi->remove(2));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(bi->end(), bi->lookup(2));
  EXPECT_EQ(makeAddr(3, 3), bi->lookup(3)->addr);
  EXPECT_EQ(2, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());
  EXPECT_FALSE(bi->remove(2));

  EXPECT_EQ(makeAddr(3, 3), bi->remove(3));
  EXPECT_EQ(makeAddr(1, 1), bi->lookup(1)->addr);
  EXPECT_EQ(bi->end(), bi->lookup(2));
  EXPECT_EQ(bi->end(), bi->lookup(3));
  EXPECT_EQ(1, bi->numEntries());
  EXPECT_FALSE(bi->overLimit());
  EXPECT_FALSE(bi->remove(3));

  EXPECT_EQ(makeAddr(1, 1), bi->remove(1));
  EXPECT_EQ(bi->end(), bi->lookup(1));
  EXPECT_EQ(bi->end(), bi->lookup(2));
  EXPECT_EQ(bi->end(), bi->lookup(3));
  EXPECT_EQ(0, bi->numEntries());
  EXPECT_FALSE(bi->overLimit());
  EXPECT_FALSE(bi->remove(1));
}

TEST(BinaryIndex, RemoveBefore) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);
  auto deleteFunc = [](auto) {};

  EXPECT_EQ(3, bi->capacity());

  bi->insert(1, makeAddr(1, 1));
  bi->insert(2, makeAddr(2, 2));
  bi->insert(3, makeAddr(3, 3));
  EXPECT_EQ(3, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());

  EXPECT_EQ(3, bi->removeBefore(10, deleteFunc));

  bi->insert(1, makeAddr(1, 1));
  bi->insert(2, makeAddr(2, 2));
  bi->insert(3, makeAddr(3, 3));
  EXPECT_EQ(3, bi->numEntries());

  EXPECT_EQ(0, bi->removeBefore(0, deleteFunc));

  EXPECT_EQ(2, bi->removeBefore(3, deleteFunc));
  EXPECT_EQ(bi->end(), bi->lookup(1));
  EXPECT_EQ(bi->end(), bi->lookup(2));
  EXPECT_EQ(makeAddr(3, 3), bi->lookup(3)->addr);
  EXPECT_EQ(1, bi->numEntries());
  EXPECT_FALSE(bi->overLimit());

  EXPECT_EQ(1, bi->removeBefore(5, deleteFunc));
  EXPECT_EQ(bi->begin(), bi->end());
  EXPECT_EQ(0, bi->numEntries());
  EXPECT_EQ(bi->end(), bi->lookup(1));
  EXPECT_EQ(bi->end(), bi->lookup(2));
  EXPECT_EQ(bi->end(), bi->lookup(3));
  EXPECT_FALSE(bi->overLimit());
}

TEST(BinaryIndex, LookupLowerbound) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  EXPECT_EQ(3, bi->capacity());

  bi->insert(1, makeAddr(1, 1));
  bi->insert(5, makeAddr(5, 5));
  bi->insert(9, makeAddr(9, 9));
  EXPECT_EQ(makeAddr(1, 1), bi->lookupLowerbound(1)->addr);
  EXPECT_EQ(makeAddr(5, 5), bi->lookupLowerbound(2)->addr);
  EXPECT_EQ(makeAddr(5, 5), bi->lookupLowerbound(5)->addr);
  EXPECT_EQ(makeAddr(9, 9), bi->lookupLowerbound(6)->addr);
  EXPECT_EQ(makeAddr(9, 9), bi->lookupLowerbound(9)->addr);
  EXPECT_EQ(bi->end(), bi->lookupLowerbound(10));
}

TEST(BinaryIndex, Clone) {
  using BI = detail::BinaryIndex<uint64_t>;
  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  EXPECT_EQ(3, bi->capacity());

  bi->insert(1, makeAddr(1, 1));
  bi->insert(2, makeAddr(2, 2));
  bi->insert(3, makeAddr(3, 3));
  EXPECT_EQ(3, bi->numEntries());
  EXPECT_TRUE(bi->overLimit());

  auto storageSize2 = BI::computeStorageSize(6);
  std::unique_ptr<uint8_t[]> buffer2 =
      std::make_unique<uint8_t[]>(storageSize2);
  auto* bi2 = BI::createNewIndex(buffer2.get(), 6);
  BI::cloneIndex(*bi2, *bi);

  EXPECT_EQ(makeAddr(1, 1), bi2->lookup(1)->addr);
  EXPECT_EQ(makeAddr(2, 2), bi2->lookup(2)->addr);
  EXPECT_EQ(makeAddr(3, 3), bi2->lookup(3)->addr);
  EXPECT_EQ(3, bi2->numEntries());
  EXPECT_EQ(6, bi2->capacity());
  EXPECT_FALSE(bi2->overLimit());
}

TEST(BinaryIndexIterator, Basic) {
  using BI = detail::BinaryIndex<uint64_t>;
  using BufManager = detail::BufferManager<LruAllocator>;

  struct TestValue {
    uint64_t val;
  };
  using BIterator =
      detail::BinaryIndexIterator<uint64_t, TestValue, BufManager>;

  auto storageSize = BI::computeStorageSize(3);
  std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(storageSize);
  auto* bi = BI::createNewIndex(buffer.get(), 3);

  auto cache = createCache();
  auto parentHandle = cache->allocate(0, "parent", 1000);
  BufManager manager{*cache, parentHandle, 1000};

  auto addr1 = manager.allocate(sizeof(TestValue));
  manager.template get<TestValue>(addr1)->val = 11111;
  EXPECT_TRUE(bi->insert(1, addr1));

  auto addr2 = manager.allocate(sizeof(TestValue));
  manager.template get<TestValue>(addr2)->val = 22222;
  EXPECT_TRUE(bi->insert(2, addr2));

  auto addr3 = manager.allocate(sizeof(TestValue));
  manager.template get<TestValue>(addr3)->val = 33333;
  EXPECT_TRUE(bi->insert(3, addr3));

  BIterator itr{&manager, bi, bi->begin()};
  EXPECT_EQ(11111, itr->val);
  ++itr;
  EXPECT_EQ(22222, itr->val);
  ++itr;
  EXPECT_EQ(33333, itr->val);

  EXPECT_EQ(addr2, bi->remove(2));
  BIterator itr2{&manager, bi, bi->begin()};
  EXPECT_EQ(11111, itr2->val);
  ++itr2;
  EXPECT_EQ(33333, itr2->val);
  ++itr2;
}

TEST(RangeMap, Basic) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map");
  EXPECT_FALSE(rm.isNullWriteHandle());

  EXPECT_EQ(0, rm.wastedBytes());
  EXPECT_EQ(160, rm.remainingBytes());

  auto rm2 = std::move(rm);
  EXPECT_FALSE(rm2.isNullWriteHandle());

  EXPECT_EQ(rm2.begin(), rm2.end());
  EXPECT_EQ(rm2.end(), rm2.lookup(1));

  EXPECT_TRUE(rm2.insert(1, 11));
  EXPECT_EQ(11, rm2.lookup(1)->value);
  EXPECT_EQ(1, rm2.size());
  EXPECT_EQ(420, rm2.sizeInBytes());

  EXPECT_FALSE(rm2.insert(1, 22));
  EXPECT_EQ(11, rm2.lookup(1)->value);
  rm2.insertOrReplace(1, 22);
  EXPECT_EQ(22, rm2.lookup(1)->value);
  EXPECT_EQ(1, rm2.size());
  EXPECT_EQ(420, rm2.sizeInBytes());

  EXPECT_TRUE(rm2.remove(1));
  EXPECT_EQ(rm2.end(), rm2.lookup(1));
  EXPECT_EQ(0, rm2.size());
  EXPECT_EQ(420, rm2.sizeInBytes());
}

TEST(RangeMap, InsertStorageExpansion) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map", 10 /* entries */,
                       50 /* storage bytes */);

  EXPECT_FALSE(rm.isNullWriteHandle());
  EXPECT_EQ(190, rm.sizeInBytes());
  EXPECT_EQ(0, rm.size());
  EXPECT_EQ(10, rm.capacity());

  EXPECT_TRUE(rm.insert(1, 11));
  EXPECT_EQ(11, rm.lookup(1)->value);
  EXPECT_EQ(1, rm.size());
  EXPECT_EQ(10, rm.capacity());
  EXPECT_EQ(190, rm.sizeInBytes());

  EXPECT_TRUE(rm.insert(2, 22));
  EXPECT_EQ(22, rm.lookup(2)->value);
  EXPECT_EQ(2, rm.size());
  EXPECT_EQ(10, rm.capacity());
  EXPECT_EQ(190, rm.sizeInBytes());

  EXPECT_TRUE(rm.insert(3, 33));
  EXPECT_EQ(33, rm.lookup(3)->value);
  EXPECT_EQ(3, rm.size());
  EXPECT_EQ(10, rm.capacity());
  EXPECT_EQ(240, rm.sizeInBytes());

  rm.insertOrReplace(1, 1111);
  EXPECT_EQ(1111, rm.lookup(1)->value);
  EXPECT_EQ(3, rm.size());
  EXPECT_EQ(10, rm.capacity());
  EXPECT_EQ(240, rm.sizeInBytes());
}

TEST(RangeMap, InsertIndexExpansion) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map", 2 /* entries */,
                       50 /* storage bytes */);

  EXPECT_FALSE(rm.isNullWriteHandle());
  EXPECT_EQ(94, rm.sizeInBytes());
  EXPECT_EQ(0, rm.size());
  EXPECT_EQ(2, rm.capacity());

  EXPECT_TRUE(rm.insert(1, 11));
  EXPECT_EQ(11, rm.lookup(1)->value);
  EXPECT_EQ(1, rm.size());
  EXPECT_EQ(2, rm.capacity());
  EXPECT_EQ(94, rm.sizeInBytes());

  EXPECT_TRUE(rm.insert(2, 22));
  EXPECT_EQ(22, rm.lookup(2)->value);
  EXPECT_EQ(2, rm.size());
  EXPECT_EQ(4, rm.capacity());
  EXPECT_EQ(118, rm.sizeInBytes());

  EXPECT_TRUE(rm.insert(3, 33));
  EXPECT_EQ(33, rm.lookup(3)->value);
  EXPECT_EQ(3, rm.size());
  EXPECT_EQ(4, rm.capacity());
  EXPECT_EQ(168, rm.sizeInBytes());

  rm.insertOrReplace(1, 1111);
  EXPECT_EQ(1111, rm.lookup(1)->value);
  EXPECT_EQ(3, rm.size());
  EXPECT_EQ(8, rm.capacity());
  EXPECT_EQ(216, rm.sizeInBytes());
}

TEST(RangeMap, Iteration) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map", 2 /* entries */,
                       50 /* storage bytes */);
  EXPECT_TRUE(rm.insert(5, 55));
  EXPECT_TRUE(rm.insert(6, 66));
  EXPECT_TRUE(rm.insert(7, 77));
  EXPECT_TRUE(rm.insert(8, 88));
  EXPECT_TRUE(rm.insert(4, 44));
  EXPECT_TRUE(rm.insert(3, 33));
  EXPECT_TRUE(rm.insert(9, 99));

  int i = 3;
  for (auto& kv : rm) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
  }
}

TEST(RangeMap, Compaction) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map", 2 /* entries */,
                       50 /* storage bytes */);
  EXPECT_TRUE(rm.insert(5, 55));
  EXPECT_TRUE(rm.insert(6, 66));
  EXPECT_TRUE(rm.insert(7, 77));
  EXPECT_TRUE(rm.insert(8, 88));
  EXPECT_TRUE(rm.insert(4, 44));
  EXPECT_TRUE(rm.insert(3, 33));
  EXPECT_TRUE(rm.insert(9, 99));
  EXPECT_EQ(7, rm.size());
  EXPECT_EQ(16, rm.capacity());
  EXPECT_EQ(412, rm.sizeInBytes());

  EXPECT_TRUE(rm.remove(9));
  EXPECT_TRUE(rm.remove(8));
  EXPECT_TRUE(rm.remove(7));
  EXPECT_EQ(4, rm.size());
  EXPECT_EQ(16, rm.capacity());
  EXPECT_EQ(412, rm.sizeInBytes());

  rm.compact();
  EXPECT_TRUE(rm.insert(7, 77));
  EXPECT_TRUE(rm.insert(8, 88));
  EXPECT_TRUE(rm.insert(9, 99));
  EXPECT_EQ(7, rm.size());
  EXPECT_EQ(16, rm.capacity());
  EXPECT_EQ(412, rm.sizeInBytes());

  EXPECT_EQ(3, rm.removeBefore(6));
  EXPECT_EQ(4, rm.size());
  EXPECT_EQ(16, rm.capacity());
  EXPECT_EQ(412, rm.sizeInBytes());
}

TEST(RangeMap, RangeLookup) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map", 2 /* entries */,
                       50 /* storage bytes */);
  EXPECT_TRUE(rm.insert(5, 55));
  EXPECT_TRUE(rm.insert(6, 66));
  EXPECT_TRUE(rm.insert(7, 77));
  EXPECT_TRUE(rm.insert(8, 88));
  EXPECT_TRUE(rm.insert(4, 44));
  EXPECT_TRUE(rm.insert(3, 33));
  EXPECT_TRUE(rm.insert(9, 99));

  auto range = rm.rangeLookup(5, 7);
  int i = 5;
  for (auto& kv : range) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
  }
  EXPECT_EQ(8, i);

  EXPECT_TRUE(rm.remove(6));
  range = rm.rangeLookup(5, 6);
  EXPECT_EQ(rm.end(), range.begin());
  EXPECT_EQ(rm.end(), range.end());
  range = rm.rangeLookup(6, 7);
  EXPECT_EQ(rm.end(), range.begin());
  EXPECT_EQ(rm.end(), range.end());

  range = rm.rangeLookup(5, 8);
  EXPECT_EQ(rm.lookup(5), range.begin());
  EXPECT_EQ(rm.lookup(9), range.end());
  i = 5;
  for (auto& kv : range) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
    if (i == 6) {
      i++;
    }
  }
  EXPECT_EQ(9, i);

  // Test const rangeLookup
  const auto rm2 = std::move(rm);
  auto constRange = rm2.rangeLookup(3, 5);
  EXPECT_EQ(rm2.lookup(3), constRange.begin());
  EXPECT_EQ(rm2.lookup(7), constRange.end());
  i = 3;
  for (auto& kv : constRange) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
  }
  EXPECT_EQ(6, i);
}

TEST(RangeMap, RangeLookupApprox) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();
  auto rm = RM::create(*cache, 0, "range_map", 2 /* entries */,
                       50 /* storage bytes */);

  auto range = rm.rangeLookupApproximate(4, 9);
  EXPECT_EQ(rm.end(), range.begin());

  EXPECT_TRUE(rm.insert(5, 55));
  EXPECT_TRUE(rm.insert(6, 66));
  EXPECT_TRUE(rm.insert(7, 77));
  EXPECT_TRUE(rm.insert(8, 88));

  range = rm.rangeLookupApproximate(4, 9);
  int i = 5;
  for (auto& kv : range) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
  }
  EXPECT_EQ(9, i);

  EXPECT_TRUE(rm.insert(4, 44));
  EXPECT_TRUE(rm.insert(9, 99));

  range = rm.rangeLookupApproximate(4, 9);
  i = 4;
  for (auto& kv : range) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
  }
  EXPECT_EQ(10, i);

  // Test const rangeLookupApproximate
  const auto rm2 = std::move(rm);
  auto constRange = rm2.rangeLookupApproximate(5, 10);
  i = 5;
  for (auto& kv : constRange) {
    EXPECT_EQ(i, kv.key);
    EXPECT_EQ(i * 11, kv.value);
    i++;
  }
  EXPECT_EQ(10, i);
}

TEST(RangeMap, LargeMap) {
  using RM = RangeMap<uint64_t, uint64_t, LruAllocator>;

  auto cache = createCache();

  // Allocate everything from cache to we will fail to allocate range map
  std::vector<LruAllocator::WriteHandle> handles;
  for (int i = 0;; i++) {
    auto handle = cache->allocate(0, folly::sformat("key_{}", i), 100'000);
    if (!handle) {
      break;
    }
    handles.push_back(std::move(handle));
  }

  auto rm = RM::create(*cache, 0, "range_map", 100'000 /* entries */,
                       1024 * 1024ul - 100 /* storage bytes */);
  EXPECT_TRUE(rm.isNullWriteHandle());
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
