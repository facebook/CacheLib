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
#include "cachelib/datatype/Map.h"
#include "cachelib/datatype/tests/DataTypeTest.h"

namespace facebook {
namespace cachelib {
namespace tests {
TEST(HashTable, Basic) {
  using HTable = detail::HashTable<uint64_t>;
  auto buffer = std::make_unique<uint8_t[]>(HTable::computeStorageSize(100));
  HTable* ht = new (buffer.get()) HTable(100);
  ASSERT_EQ(100, ht->capacity());

  const uint64_t key = 1234;
  const detail::BufferAddr dummyAddr{1 /* item offset */, 0 /* byte offset */};

  {
    ASSERT_EQ(nullptr, ht->find(key));
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    ASSERT_EQ(1, ht->numEntries());
    auto* e = ht->find(key);
    ASSERT_NE(nullptr, e);
    ASSERT_EQ(dummyAddr, e->addr);

    ASSERT_EQ(dummyAddr, ht->insertOrReplace(key, detail::BufferAddr{1, 100}));
    ASSERT_EQ(1, ht->numEntries());
  }

  {
    ASSERT_TRUE(ht->remove(key));
    ASSERT_EQ(nullptr, ht->find(key));
    ASSERT_EQ(0, ht->numEntries());
  }
}

TEST(HashTable, Basic2) {
  using HTable = detail::HashTable<uint64_t>;
  auto buffer = std::make_unique<uint8_t[]>(HTable::computeStorageSize(1000));
  HTable* ht = new (buffer.get()) HTable(1000);

  const detail::BufferAddr dummyAddr{1 /* item offset */, 0 /* byte offset */};

  for (uint64_t key = 0; key < 100; ++key) {
    ASSERT_EQ(nullptr, ht->find(key));
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    auto* e = ht->find(key);
    ASSERT_NE(nullptr, e);
  }
  ASSERT_EQ(100, ht->numEntries());

  for (uint64_t key = 0; key < 100; ++key) {
    ASSERT_TRUE(ht->remove(key));
    ASSERT_EQ(nullptr, ht->find(key));
  }
  ASSERT_EQ(0, ht->numEntries());

  for (uint64_t key = 0; key < 100; ++key) {
    ASSERT_EQ(nullptr, ht->find(key));
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    auto* e = ht->find(key);
    ASSERT_NE(nullptr, e);
  }
  ASSERT_EQ(100, ht->numEntries());
}

TEST(HashTable, Hash) {
  const detail::BufferAddr dummyAddr{1 /* item offset */, 0 /* byte offset */};

  {
    auto buffer = std::make_unique<uint8_t[]>(
        detail::HashTable<uint64_t>::computeStorageSize(1000));
    detail::HashTable<uint64_t>* ht =
        new (buffer.get()) detail::HashTable<uint64_t>(1000);
    ASSERT_EQ(nullptr, ht->insertOrReplace(1234ul, dummyAddr));
  }

  {
    struct Key {
      uint64_t a;
      uint64_t b;
      uint64_t c;
      uint64_t d;

      bool operator==(const Key& rhs) {
        return std::memcmp(this, &rhs, sizeof(Key));
      }
    };
    auto buffer = std::make_unique<uint8_t[]>(
        detail::HashTable<Key>::computeStorageSize(1000));
    detail::HashTable<Key>* ht =
        new (buffer.get()) detail::HashTable<Key>(1000);
    const Key key{1, 2, 3, 4};
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
  }
}

TEST(HashTable, Collision) {
  struct NoopHash : public Hash {
    uint32_t operator()(const void* buf,
                        size_t /* unsed */) const noexcept override {
      return *reinterpret_cast<const uint32_t*>(buf);
    }
    int getMagicId() const noexcept override { return 2; }
  };

  using HTable = detail::HashTable<uint64_t, NoopHash>;

  const detail::BufferAddr dummyAddr{1 /* item offset */, 0 /* byte offset */};

  const uint32_t capacity = 100;

  auto buffer =
      std::make_unique<uint8_t[]>(HTable::computeStorageSize(capacity));
  HTable* ht = new (buffer.get()) HTable(capacity);

  std::set<uint32_t> keys;
  for (uint32_t numKeys = 0; numKeys < 10; ++numKeys) {
    // all these keys will collide on the slot[0]
    const uint32_t key = numKeys * capacity;
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    keys.insert(key);
  }
  for (const auto key : keys) {
    ASSERT_NE(nullptr, ht->find(key));
  }

  for (uint32_t numKeys = 0; numKeys < 10; ++numKeys) {
    // all these keys will collide on the slot[1]
    const uint32_t key = numKeys * capacity + 1;
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    keys.insert(key);
  }
  for (const auto key : keys) {
    ASSERT_NE(nullptr, ht->find(key));
  }

  for (uint32_t numKeys = 0; numKeys < 10; ++numKeys) {
    // all these keys will collide on the slot[2]
    const uint32_t key = numKeys * capacity + 2;
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    keys.insert(key);
  }
  for (const auto key : keys) {
    ASSERT_NE(nullptr, ht->find(key));
  }

  // Remove half of the keys from each collision bucket
  for (uint32_t numKeys = 0; numKeys < 5; ++numKeys) {
    const uint32_t key1 = numKeys * capacity + 0;
    const uint32_t key2 = numKeys * capacity + 1;
    const uint32_t key3 = numKeys * capacity + 2;
    ASSERT_TRUE(ht->remove(key1));
    ASSERT_TRUE(ht->remove(key2));
    ASSERT_TRUE(ht->remove(key3));
    keys.erase(key1);
    keys.erase(key2);
    keys.erase(key3);
  }
  for (const auto key : keys) {
    ASSERT_NE(nullptr, ht->find(key));
  }

  // Reinsert them and make sure we still find everything
  for (uint32_t numKeys = 0; numKeys < 5; ++numKeys) {
    const uint32_t key1 = numKeys * capacity + 0;
    const uint32_t key2 = numKeys * capacity + 1;
    const uint32_t key3 = numKeys * capacity + 2;
    ASSERT_EQ(nullptr, ht->insertOrReplace(key1, dummyAddr));
    ASSERT_EQ(nullptr, ht->insertOrReplace(key2, dummyAddr));
    ASSERT_EQ(nullptr, ht->insertOrReplace(key3, dummyAddr));
    keys.insert(key1);
    keys.insert(key2);
    keys.insert(key3);
  }
  for (const auto key : keys) {
    ASSERT_NE(nullptr, ht->find(key));
  }
}

TEST(HashTable, Boundary) {
  struct NoopHash : public Hash {
    uint32_t operator()(const void* buf,
                        size_t /* unsed */) const noexcept override {
      return *reinterpret_cast<const uint32_t*>(buf);
    }
    int getMagicId() const noexcept override { return 2; }
  };

  using HTable = detail::HashTable<uint64_t, NoopHash>;

  const detail::BufferAddr dummyAddr{1 /* item offset */, 0 /* byte offset */};

  const uint32_t capacity = 100;

  auto buffer =
      std::make_unique<uint8_t[]>(HTable::computeStorageSize(capacity));
  HTable* ht = new (buffer.get()) HTable(capacity);

  std::set<uint32_t> keys;

  // Insert 0 to 99
  for (uint32_t i = 0; i < 99; ++i) {
    ASSERT_EQ(nullptr, ht->insertOrReplace(i, dummyAddr));
    keys.insert(i);
  }
  ASSERT_THROW(ht->insertOrReplace(99, dummyAddr), std::bad_alloc);
  ASSERT_EQ(99, ht->numEntries());
  ASSERT_EQ(keys.size(), ht->numEntries());
  for (auto key : keys) {
    ASSERT_TRUE(ht->find(key));
  }

  // Remove first 50 elements
  for (uint32_t i = 0; i < 50; ++i) {
    ASSERT_TRUE(ht->remove(i));
    keys.erase(i);
  }
  ASSERT_EQ(49, ht->numEntries());
  ASSERT_EQ(keys.size(), ht->numEntries());
  for (auto key : keys) {
    ASSERT_TRUE(ht->find(key));
  }

  // Now going to insert more keys that collide on the last few slots
  // to cause them to go around the hash table to fill in the beginning
  for (uint32_t i = 1; i < 10; ++i) {
    const uint32_t key = 98 * i * capacity;
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    keys.insert(key);
  }
  ASSERT_EQ(keys.size(), ht->numEntries());
  for (auto key : keys) {
    ASSERT_TRUE(ht->find(key));
  }

  // Insert some more that will collide at 51st entry, so it will wrap around
  // all the way until the newly inserted above.
  for (uint32_t i = 2; i < 10; ++i) {
    const uint32_t key = 51 * i * capacity;
    ASSERT_EQ(nullptr, ht->insertOrReplace(key, dummyAddr));
    keys.insert(key);
  }
  ASSERT_EQ(keys.size(), ht->numEntries());
  for (auto key : keys) {
    ASSERT_TRUE(ht->find(key));
  }

  // Now delete the latter half of the hash table, so we can shift backwards
  for (uint32_t i = 98; i >= 51; --i) {
    ASSERT_TRUE(ht->remove(i));
    keys.erase(i);
  }
  ASSERT_EQ(keys.size(), ht->numEntries());
  for (auto key : keys) {
    ASSERT_TRUE(ht->find(key));
  }
}

TEST(HashTable, Rehash) {
  using namespace facebook::cachelib::detail;
  using HTable = HashTable<uint64_t>;

  const detail::BufferAddr dummyAddr{1 /* item offset */, 0 /* byte offset */};

  // Allocate three hash tables from smaller to larger.
  // Fill up the medium size hash table.
  // Rehashing from medium size to larger size should succeed while rehashing
  // from medium size to smaller size should fail.

  auto buffer1 = std::make_unique<uint8_t[]>(HTable::computeStorageSize(100));
  HTable* ht1 = new (buffer1.get()) HTable(100);

  for (uint32_t i = 0; i < 99; ++i) {
    ASSERT_EQ(nullptr, ht1->insertOrReplace(i, dummyAddr));
  }
  for (uint32_t i = 0; i < 99; ++i) {
    ASSERT_NE(nullptr, ht1->find(i));
  }
  ASSERT_THROW(ht1->insertOrReplace(100, dummyAddr), std::bad_alloc);

  auto buffer2 = std::make_unique<uint8_t[]>(HTable::computeStorageSize(200));
  ASSERT_NO_THROW(new (buffer2.get()) HTable(200, *ht1));
  auto* ht2 = reinterpret_cast<HTable*>(buffer2.get());
  for (uint32_t i = 0; i < 99; ++i) {
    ASSERT_NE(nullptr, ht2->find(i));
  }

  auto buffer3 = std::make_unique<uint8_t[]>(HTable::computeStorageSize(50));
  ASSERT_THROW(new (buffer3.get()) HTable(50, *ht1), std::invalid_argument);
}

template <typename AllocatorT>
class MapTest : public ::testing::Test {
 private:
  struct Value {
    uint32_t len;
    uint8_t data[];
    uint32_t getStorageSize() const { return sizeof(Value) + len; }

    static void release(Value* v) { delete[] reinterpret_cast<uint8_t*>(v); }
    static auto create(uint32_t len) {
      auto v = std::unique_ptr<Value, decltype(&release)>{
          reinterpret_cast<Value*>(new uint8_t[sizeof(Value) + len]), release};
      v->len = len;
      std::memset(&v->data, 0, v->len);
      return v;
    }
  };

 public:
  void testHashTableAllocator() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    // Allocate a hashtable and fill it until over limit. Expand and verify
    // it is no longer over limit.
    auto ht = detail::createHashTable<uint64_t, AllocatorT>(
        *cache, pid, "my_hash_table", 100);
    ASSERT_EQ(100, ht->capacity());
    ASSERT_EQ(0, ht->numEntries());
    ASSERT_FALSE(ht->overLimit());

    const detail::BufferAddr dummyAddr{1 /* item offset */,
                                       0 /* byte offset */};
    for (uint32_t i = 0; i < 95; ++i) {
      ASSERT_EQ(nullptr, ht->insertOrReplace(i, dummyAddr));
    }
    ASSERT_EQ(95, ht->numEntries());
    ASSERT_TRUE(ht->overLimit());

    auto ht2 = detail::expandHashTable<uint64_t, AllocatorT>(*cache, ht);
    ASSERT_EQ(200, ht2->capacity());
    ASSERT_EQ(95, ht2->numEntries());
    ASSERT_FALSE(ht2->overLimit());
  }

  void testBasic() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // Test insertion and find
    ASSERT_TRUE(map.insert(123, 0));
    ASSERT_FALSE(map.insert(123, 0));
    ASSERT_EQ(BasicMap::kReplaced, map.insertOrReplace(123, 0));

    auto* v = map.find(123);
    ASSERT_NE(nullptr, v);

    *v = 123;
    ASSERT_EQ(123, *(map.find(123)));

    // Test move constructor and assignment operators
    auto map2{std::move(map)};
    ASSERT_TRUE(map.isNullWriteHandle());
    ASSERT_TRUE(map2.find(123));

    auto map3 = BasicMap::create(*cache, pid, "my_new_map");
    map3 = std::move(map2);
    ASSERT_TRUE(map2.isNullWriteHandle());
    ASSERT_TRUE(map3.find(123));

    BasicMap map4 = nullptr;
    map4 = std::move(map3);
    ASSERT_TRUE(map3.isNullWriteHandle());
    ASSERT_TRUE(map4.find(123));

    ASSERT_TRUE(map4.erase(123));
    ASSERT_EQ(nullptr, map4.find(123));

    BasicMap map5 = nullptr;
    map4 = std::move(map5);
  }

  void testManyEntriesFixed() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // Fill up the map and expand it beyond 4MB
    // 1000 * 10KB = ~10MB
    const uint32_t numValues = 1000;
    const uint32_t size = 10240;
    for (uint32_t key = 0; key < numValues; ++key) {
      auto v = Value::create(size);
      ASSERT_TRUE(map.insert(key, *v));
    }

    for (uint32_t key = 0; key < numValues; ++key) {
      auto* v = map.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        ASSERT_EQ(0, v->data[i]);
      }
      for (uint32_t i = 0; i < v->len; ++i) {
        v->data[i] = i % std::numeric_limits<uint8_t>::max();
      }
    }

    for (uint32_t key = 0; key < numValues; ++key) {
      auto* v = map.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        ASSERT_EQ(i % std::numeric_limits<uint8_t>::max(), v->data[i]);
      }
    }

    // Delete half of the keys,
    // map should use the same number of bytes before and after
    const uint32_t totalBytes = map.sizeInBytes();

    for (uint32_t key = 0; key < numValues; ++key) {
      if (key % 2 == 0) {
        ASSERT_TRUE(map.erase(key));
      }
    }
    ASSERT_EQ(totalBytes, map.sizeInBytes());

    // Add back the other half of keys, and verify the keys are
    // still what we expect
    for (uint32_t key = 0; key < numValues; ++key) {
      if (key % 2 == 0) {
        auto v = Value::create(size);
        std::memset(&v->data, 0, v->len);
        ASSERT_TRUE(map.insert(key, *v));
      }
    }
    for (uint32_t key = 0; key < numValues; ++key) {
      auto* v = map.find(key);
      ASSERT_NE(nullptr, v);
      if (key % 2 == 0) {
        for (uint32_t i = 0; i < v->len; ++i) {
          ASSERT_EQ(0, v->data[i]);
        }
      } else {
        for (uint32_t i = 0; i < v->len; ++i) {
          ASSERT_EQ(i % std::numeric_limits<uint8_t>::max(), v->data[i]);
        }
      }
    }
  }

  void testManyEntriesFixedUpperBound() {
    // Allocate entries until we're at 2^17 = 131072
    typename AllocatorT::Config config;
    config.configureChainedItems();
    config.setCacheSize(100 * Slab::kSize);
    config.setDefaultAllocSizes(util::generateAllocSizes(2, 1024 * 1024));

    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    const auto pid = cache->addPool("default", numBytes);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // With 8 bytes per entry in hash tagble, we can fit
    // 81920 * 0.9 == 73728 entries before we have to expand by 2x
    // which would be more than 1MB.
    const uint32_t numValues = 73729;
    const uint32_t size = 10;
    for (uint32_t key = 0; key < numValues; ++key) {
      auto v = Value::create(size);
      ASSERT_TRUE(map.insert(key, *v));
    }
    auto v = Value::create(size);
    ASSERT_THROW(map.insert(numValues, *v), cachelib::MapIndexMaxedOut);
  }

  void testManyEntriesVariable() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // Fill up the map and expand it beyond 4MB
    // 1000 * (10KB to 20KB) = 10MB to 20MB
    const uint32_t numValues = 1000;
    for (uint32_t key = 0; key < numValues; ++key) {
      const uint32_t size = 10240 + (folly::Random::rand32() % 10240);
      auto v = Value::create(size);
      std::memset(&v->data, 0, v->len);
      ASSERT_TRUE(map.insert(key, *v));
    }

    for (uint32_t key = 0; key < numValues; ++key) {
      auto* v = map.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        ASSERT_EQ(0, v->data[i]);
      }
      for (uint32_t i = 0; i < v->len; ++i) {
        v->data[i] = i % std::numeric_limits<uint8_t>::max();
      }
    }

    for (uint32_t key = 0; key < numValues; ++key) {
      auto* v = map.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        ASSERT_EQ(i % std::numeric_limits<uint8_t>::max(), v->data[i]);
      }
    }

    // Delete half of the keys
    // On the first new allocation, we should trigger compaction
    for (uint32_t key = 0; key < numValues; ++key) {
      if (key % 2 == 0) {
        ASSERT_TRUE(map.erase(key));
      }
    }

    for (uint32_t key = 0; key < numValues; ++key) {
      if (key % 2 == 0) {
        const uint32_t size = 10240 + (folly::Random::rand32() % 10240);
        auto v = Value::create(size);
        ASSERT_TRUE(map.insert(key, *v));
      }
    }

    for (uint32_t key = 0; key < numValues; ++key) {
      auto* v = map.find(key);
      ASSERT_NE(nullptr, v);
      if (key % 2 == 0) {
        for (uint32_t i = 0; i < v->len; ++i) {
          ASSERT_EQ(0, v->data[i]);
        }
      } else {
        for (uint32_t i = 0; i < v->len; ++i) {
          ASSERT_EQ(i % std::numeric_limits<uint8_t>::max(), v->data[i]);
        }
      }
    }
  }

  void testCompaction() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // Allocate 1000 fixed key/value pair.
    // Delete 500 and trigger compaction.
    // Allocate 500, the sizeInBytes shouldn't change.

    // Fill up the map and expand it beyond 4MB
    // 1000 * (10KB to 20KB) = 10MB to 20MB
    const uint32_t numValues = 1000;
    for (uint32_t key = 0; key < numValues; ++key) {
      const uint32_t size = 10240 + (folly::Random::rand32() % 10240);
      auto v = Value::create(size);
      std::memset(&v->data, 0, v->len);
      ASSERT_TRUE(map.insert(key, *v));
    }
    const auto sizeInBytes = map.sizeInBytes();

    // Delete half of the keys
    for (uint32_t key = 0; key < numValues; ++key) {
      if (key % 2 == 0) {
        ASSERT_TRUE(map.erase(key));
      }
    }

    map.compact();

    // Allocate them again
    for (uint32_t key = 0; key < numValues; ++key) {
      if (key % 2 == 0) {
        const uint32_t size = 10240 + (folly::Random::rand32() % 10240);
        auto v = Value::create(size);
        ASSERT_TRUE(map.insert(key, *v));
      }
    }

    // Size shouldn't have changed
    ASSERT_EQ(sizeInBytes, map.sizeInBytes());
  }

  void testIterator() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // Fill up the map and expand it beyond 4MB
    // 1000 * (10KB to 20KB) = 10MB to 20MB
    const uint32_t numKeys = 1000;
    std::set<uint32_t> keys;
    for (uint32_t key = 0; key < numKeys; ++key) {
      const uint32_t size = 10240 + (std::rand() % 10240);
      auto value = Value::create(size);
      ASSERT_TRUE(map.insert(key, *value));
      keys.insert(key);
    }

    ASSERT_TRUE(map.erase(1));
    ASSERT_TRUE(map.erase(10));
    ASSERT_TRUE(map.erase(100));
    ASSERT_EQ(1000 - 3, map.size());

    ASSERT_EQ(nullptr, map.find(1));
    ASSERT_EQ(nullptr, map.find(10));
    ASSERT_EQ(nullptr, map.find(100));

    ASSERT_EQ(1, keys.erase(1));
    ASSERT_EQ(1, keys.erase(10));
    ASSERT_EQ(1, keys.erase(100));
    ASSERT_EQ(1000 - 3, keys.size());

    // Verify we still have all the keys in the map
    for (auto key : keys) {
      SCOPED_TRACE(folly::sformat("key expected: {}", key));
      ASSERT_NE(nullptr, map.find(key));
    }

    std::for_each(map.begin(), map.end(), [&](auto& kv) {
      const auto keyToPrint = kv.key; // need this to compile with gcc
      SCOPED_TRACE(folly::sformat("key expected: {}", keyToPrint));
      ASSERT_NE(keys.find(kv.key), keys.end());
      keys.erase(kv.key);
    });
    SCOPED_TRACE(folly::sformat("actual number of keys: {}", keys.size()));
    ASSERT_TRUE(keys.empty());
  }

  void testEmptyMapIterator() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    auto v0 = Value::create(1000);
    auto v1 = Value::create(1000);
    auto v2 = Value::create(1000);

    ASSERT_TRUE(map.insert(0, *v0));
    ASSERT_TRUE(map.insert(1, *v1));
    ASSERT_TRUE(map.insert(2, *v2));
    ASSERT_NE(map.begin(), map.end());

    ASSERT_TRUE(map.erase(0));
    ASSERT_TRUE(map.erase(1));
    ASSERT_TRUE(map.erase(2));
    ASSERT_EQ(map.begin(), map.end());

    map.compact();
    ASSERT_EQ(map.begin(), map.end());

    auto itr = map.begin();
    ASSERT_THROW(++itr, std::out_of_range);
  }

  void testStdContainer() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<uint32_t, uint32_t, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");

    // Fill up the map
    const uint32_t numKeys = 10000;
    for (uint32_t key = 0; key < numKeys; ++key) {
      ASSERT_TRUE(map.insert(key, key));
    }
    ASSERT_TRUE(map.erase(1));
    ASSERT_TRUE(map.erase(10));
    ASSERT_TRUE(map.erase(100));
    ASSERT_EQ(10000 - 3, map.size());

    std::vector<typename BasicMap::EntryKeyValue> vec;
    std::copy(std::begin(map), std::end(map),
              std::inserter(vec, std::begin(vec)));
    ASSERT_EQ(vec.size(), map.size());
    for (auto& v : vec) {
      ASSERT_EQ(v.value, *map.find(v.key));
    }
  }

  void testExpandTableFailure() {
    typename AllocatorT::Config config;
    config.configureChainedItems();
    config.setCacheSize(10 * Slab::kSize);
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    cache->addPool("pool", numBytes);

    const auto pid = cache->getPoolId("pool");

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");
    const uint32_t size = 10240;

    // default num entries is 20, we need to go above 90% of that to trigger
    // expandHashTable
    const uint32_t numKey = 19;

    for (uint32_t key = 0; key < numKey; ++key) {
      auto v = Value::create(size);
      for (uint32_t i = 0; i < v->len; ++i) {
        v->data[i] = static_cast<uint8_t>(key);
      }
      ASSERT_TRUE(map.insert(key, *v));
    }
    ASSERT_EQ(numKey, map.size());

    // fill up the cache with other items, and hold their handles to prevent
    // eviction
    std::vector<typename AllocatorT::Item::Handle> handles;
    for (uint32_t i = 0;; i++) {
      auto handle = cache->allocate(pid, std::to_string(i), size);
      if (!handle) {
        break;
      }
      ASSERT_NE(nullptr, handle);
      handles.push_back(std::move(handle));
    }

    // try another insert, expandHashTable gets triggered and fails
    {
      auto v = Value::create(size);
      ASSERT_THROW(map.insert(numKey, *v), std::bad_alloc);
    }

    // the map should still be valid
    for (uint32_t key = 0; key < numKey; ++key) {
      auto v = map.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        EXPECT_EQ(key, v->data[i]);
      }
    }
  }

  // When we need to expand the hash table, we should fork the chain and keep
  // the old handle valid
  void testForkChainAtExpandTable() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map");
    const uint32_t size = 10240;

    // default num entries is 20, we need to go above 90% of that.
    const uint32_t numKey = 19;

    for (uint32_t key = 0; key < numKey; ++key) {
      auto v = Value::create(size);
      for (uint32_t i = 0; i < v->len; ++i) {
        v->data[i] = static_cast<uint8_t>(key);
      }
      ASSERT_TRUE(map.insert(key, *v));
    }
    ASSERT_EQ(numKey, map.size());

    cache->insert(map.viewWriteHandle());
    auto oldHandle = cache->findImpl("my_map", AccessMode::kRead);
    ASSERT_NE(nullptr, oldHandle);
    ASSERT_NE(nullptr, cache->find("my_map"));

    // when we have more than 90% * capacity = 18 entries, and do another
    // insert, expandHashTable gets triggered
    {
      auto v = Value::create(size);
      ASSERT_TRUE(map.insert(numKey, *v));
    }

    // new map is visible
    auto newHandle = cache->findImpl("my_map", AccessMode::kRead);
    ASSERT_NE(nullptr, newHandle);
    auto newMap = BasicMap::fromWriteHandle(*cache, std::move(newHandle));
    ASSERT_FALSE(newMap.isNullWriteHandle());
    ASSERT_NE(nullptr, newMap.find(numKey));

    for (uint32_t key = 0; key < numKey; ++key) {
      ASSERT_TRUE(newMap.erase(key));
    }

    const auto oldMap = BasicMap::fromWriteHandle(*cache, std::move(oldHandle));
    ASSERT_FALSE(oldMap.isNullWriteHandle());
    EXPECT_EQ(numKey, oldMap.size());
    for (uint32_t key = 0; key < numKey; ++key) {
      auto* v = oldMap.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        EXPECT_EQ(key, v->data[i]);
      }
    }
  }

  // When we need to add a new chained item, we should fork the chain and keep
  // the old handle valid
  void testForkChainAtAppend() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, Value, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map", /* numEntries = */ 20,
                                /* numBytes = */ 2500);

    const uint32_t size = 1024;
    const uint32_t numKey = 2; // 2 items to fill up first item

    for (uint32_t key = 0; key < numKey; ++key) {
      auto v = Value::create(size);
      for (uint32_t i = 0; i < v->len; ++i) {
        v->data[i] = static_cast<uint8_t>(key);
      }
      ASSERT_TRUE(map.insert(key, *v));
    }
    ASSERT_EQ(numKey, map.size());

    cache->insert(map.viewWriteHandle());
    auto oldHandle = cache->findImpl("my_map", AccessMode::kRead);
    ASSERT_NE(nullptr, oldHandle);
    ASSERT_NE(nullptr, cache->find("my_map"));

    // add another entry to add a chained item
    {
      auto v = Value::create(size);
      ASSERT_TRUE(map.insert(numKey, *v));
    }

    // new map is visible
    auto newHandle = cache->findImpl("my_map", AccessMode::kRead);
    ASSERT_NE(nullptr, newHandle);
    auto newMap = BasicMap::fromWriteHandle(*cache, std::move(newHandle));
    ASSERT_FALSE(newMap.isNullWriteHandle());
    ASSERT_NE(nullptr, newMap.find(numKey));

    for (uint32_t key = 0; key < numKey + 1; ++key) {
      ASSERT_TRUE(newMap.erase(key));
    }
    ASSERT_EQ(0, newMap.size());

    for (uint32_t key = 10; key < 50; ++key) {
      auto v = Value::create(size);
      ASSERT_TRUE(newMap.insert(key, *v));
    }

    const auto oldMap = BasicMap::fromWriteHandle(*cache, std::move(oldHandle));
    ASSERT_FALSE(oldMap.isNullWriteHandle());
    EXPECT_EQ(numKey, oldMap.size());
    for (uint32_t key = 0; key < numKey; ++key) {
      auto* v = oldMap.find(key);
      ASSERT_NE(nullptr, v);
      for (uint32_t i = 0; i < v->len; ++i) {
        EXPECT_EQ(key, v->data[i]);
      }
    }
  }

  void testStdAlgorithms() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map", /* numEntries = */ 20,
                                /* numBytes = */ 2500);

    map.insert(100, 100);
    map.insert(200, 200);
    map.insert(300, 300);

    auto itr = std::find_if(map.begin(), map.end(),
                            [](auto& p) { return p.key == 200; });
    ASSERT_NE(map.end(), itr);
    ASSERT_EQ(200, itr->value);
  }

  void testTinyMap() {
    auto cache = DataTypeTest::createCache<AllocatorT>();
    const auto pid = cache->getPoolId(DataTypeTest::kDefaultPool);

    using BasicMap = cachelib::Map<int, int, AllocatorT>;
    auto map = BasicMap::create(*cache, pid, "my_map", 2, 256);

    // Test insertion and find
    EXPECT_TRUE(map.insert(12, 100));
    auto* v1 = map.find(12);
    EXPECT_EQ(100, *v1);

    // This should expand hashmap
    EXPECT_TRUE(map.insert(15, 200));
    auto* v2 = map.find(12);
    EXPECT_EQ(100, *v2);
    auto* v3 = map.find(15);
    EXPECT_EQ(200, *v3);
  }
};

TYPED_TEST_CASE(MapTest, AllocatorTypes);
TYPED_TEST(MapTest, HashTableAllocator) { this->testHashTableAllocator(); }
TYPED_TEST(MapTest, Basic) { this->testBasic(); }
TYPED_TEST(MapTest, ManyEntriesFixed) { this->testManyEntriesFixed(); }
TYPED_TEST(MapTest, ManyEntriesFixedUpperBound) {
  this->testManyEntriesFixedUpperBound();
}
TYPED_TEST(MapTest, ManyEntriesVariable) { this->testManyEntriesVariable(); }
TYPED_TEST(MapTest, Compaction) { this->testCompaction(); }
TYPED_TEST(MapTest, Iterator) { this->testIterator(); }
TYPED_TEST(MapTest, EmptyMapIterator) { this->testEmptyMapIterator(); }
TYPED_TEST(MapTest, StdContainer) { this->testStdContainer(); }
TYPED_TEST(MapTest, ExpandTableFailure) { this->testExpandTableFailure(); }
TYPED_TEST(MapTest, ForkChainAtExpandTable) {
  this->testForkChainAtExpandTable();
}
TYPED_TEST(MapTest, ForkChainAtAppend) { this->testForkChainAtAppend(); }
TYPED_TEST(MapTest, StdAlgorithms) { this->testStdAlgorithms(); }
TYPED_TEST(MapTest, TinyMap) { this->testTinyMap(); }
} // namespace tests
} // namespace cachelib
} // namespace facebook
