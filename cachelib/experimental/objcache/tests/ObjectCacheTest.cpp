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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstring-conversion"
#include <folly/futures/Promise.h>
#include <folly/futures/SharedPromise.h>
#pragma GCC diagnostic pop
#include <gtest/gtest.h>

#include <vector>

#include "cachelib/experimental/objcache/ObjectCache.h"
#include "cachelib/navy/serialization/RecordIO.h"
#include "thrift/lib/cpp2/protocol/Cpp2Ops.h"

namespace facebook {
namespace cachelib {
namespace objcache {
namespace test {
namespace {
using LruObjectCache =
    ObjectCache<CacheDescriptor<LruAllocator>,
                MonotonicBufferResource<CacheDescriptor<LruAllocator>>>;

std::unique_ptr<LruObjectCache> createCache(LruObjectCache::Config config) {
  auto objcache = std::make_unique<LruObjectCache>(config);
  objcache->getCacheAlloc().addPool(
      "default", objcache->getCacheAlloc().getCacheMemoryStats().ramCacheSize);
  return objcache;
}
} // namespace
using ObjCacheString = std::
    basic_string<char, std::char_traits<char>, LruObjectCache::Alloc<char>>;
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook

namespace std {
using facebook::cachelib::objcache::test::ObjCacheString;
template <>
struct hash<ObjCacheString> {
  std::size_t operator()(const ObjCacheString& s) const noexcept {
    return std::hash<std::string_view>{}({s.data(), s.size()});
  }
};
} // namespace std

namespace apache {
namespace thrift {
using facebook::cachelib::objcache::test::ObjCacheString;
template <>
class Cpp2Ops<ObjCacheString> {
 public:
  typedef ObjCacheString Type;
  static constexpr protocol::TType thriftType() { return protocol::T_STRING; }
  template <class Protocol>
  static uint32_t write(Protocol* prot, const Type* value) {
    return prot->writeString(*value);
  }
  template <class Protocol>
  static void read(Protocol* prot, Type* value) {
    prot->readString(*value);
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    return prot->serializedSizeString(*value);
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    return prot->serializedSizeString(*value);
  }
};
} // namespace thrift
} // namespace apache

namespace facebook {
namespace cachelib {
namespace objcache {
namespace test {
TEST(ObjectCache, Simple) {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;
  // Create a new vector in cache
  {
    auto vec = objcache->create<Vector>(0 /* poolId */, "my obj");
    ASSERT_TRUE(vec);
    EXPECT_EQ(0, vec->size());
    vec->push_back(123);
    EXPECT_EQ(1, vec->size());
    for (int i = 0; i < 1000; i++) {
      vec->push_back(i);
    }
    EXPECT_EQ(1001, vec->size());

    // Insert into cache
    objcache->insertOrReplace(vec);
  }

  // Look up our vector in cache
  {
    LruObjectCache::ObjectHandle<Vector> vec = objcache->find<Vector>("my obj");
    ASSERT_TRUE(vec);

    EXPECT_EQ(1001, vec->size());

    for (int i = 999; i >= 0; i--) {
      ASSERT_EQ(i, vec->back());
      vec->pop_back();
    }
    EXPECT_EQ(1, vec->size());
    EXPECT_EQ(123, vec->back());
  }

  // Look up again but using a std::shared_ptr
  {
    // Convert to shared_ptr
    // NOTE this incurs a heap allocation for shared_ptr's resource. So some cpu
    // expense here.
    std::shared_ptr<Vector> vec =
        objcache->find<Vector>("my obj").toSharedPtr();
    ASSERT_TRUE(vec);

    EXPECT_EQ(1, vec->size());
    EXPECT_EQ(123, vec->back());
  }

  // Look up something that doesn't exist
  {
    auto tmpVec = objcache->find<Vector>("this key doesn't exist");
    ASSERT_FALSE(tmpVec);
  }
}

TEST(ObjectCache, Copy) {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;

  auto vec = objcache->create<Vector>(0 /* poolId */, "my obj");
  ASSERT_TRUE(vec);
  EXPECT_EQ(0, vec->size());
  vec->push_back(123);
  EXPECT_EQ(1, vec->size());
  for (int i = 0; i < 1000; i++) {
    vec->push_back(i);
  }
  EXPECT_EQ(1001, vec->size());

  // Copy construction, the new vector is still associated with the same
  // allocator.
  Vector vec2 = *vec;
  ASSERT_EQ(vec->get_allocator(), vec2.get_allocator());

  // Copy construction with user-supplied allocator. The original allocator
  // will not be propagated.
  Vector vec3{*vec, decltype(vec->get_allocator()){}};
  ASSERT_NE(vec->get_allocator(), vec3.get_allocator());

  // Copy assignment. The original allocator will not be propagated.
  Vector vec4;
  vec4 = *vec;
  ASSERT_NE(vec->get_allocator(), vec3.get_allocator());
}

TEST(ObjectCache, ObjectHandle) {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;
  {
    auto vec = objcache->create<Vector>(0 /* poolId */, "my obj");
    ASSERT_TRUE(vec);
    EXPECT_EQ(0, vec->size());
    vec->push_back(123);
    EXPECT_EQ(1, vec->size());
    objcache->insertOrReplace(vec);
  }

  {
    LruObjectCache::ObjectHandle<Vector> vec = objcache->find<Vector>("my obj");
    ASSERT_TRUE(vec);
    EXPECT_EQ(1, vec->size());
    EXPECT_EQ(123, vec->back());

    // Convert to shared_ptr from handle
    std::shared_ptr<Vector> vec2 = std::move(vec).toSharedPtr();
    EXPECT_FALSE(vec);
    ASSERT_TRUE(vec2);
    EXPECT_EQ(1, vec2->size());
  }

  {
    LruObjectCache::ObjectHandle<Vector> vec = objcache->find<Vector>("my obj");
    ASSERT_TRUE(vec);
    EXPECT_EQ(1, vec->size());
    EXPECT_EQ(123, vec->back());

    // Convert to object handle from ObjectCache API
    LruObjectCache::ObjectHandle<Vector> vec2 =
        LruObjectCache::toObjectHandle<Vector>(
            std::move(vec).releaseItemHandle());
    EXPECT_FALSE(vec);
    ASSERT_TRUE(vec2);
    EXPECT_EQ(1, vec2->size());
    EXPECT_EQ(123, vec2->back());

    // Convert to shared_ptr from ObjectCache API
    std::shared_ptr<Vector> vec3 = LruObjectCache::toSharedPtr<Vector>(
        std::move(vec2).releaseItemHandle());
    EXPECT_FALSE(vec2);
    ASSERT_TRUE(vec3);
    EXPECT_EQ(1, vec3->size());
    EXPECT_EQ(123, vec3->back());
  }

  {
    auto vec = objcache->create<Vector>(0 /* poolId */, "my obj");
    ASSERT_TRUE(vec);
    EXPECT_EQ(0, vec->size());
    vec->push_back(456);
    EXPECT_EQ(1, vec->size());

    auto oldVec = objcache->insertOrReplace(vec);
    ASSERT_TRUE(oldVec);
    EXPECT_EQ(1, oldVec->size());
    EXPECT_EQ(123, oldVec->back());

    auto oldVec2 = std::move(oldVec).toSharedPtr();
    EXPECT_FALSE(oldVec);
    ASSERT_TRUE(oldVec2);
    EXPECT_EQ(1, oldVec2->size());
    EXPECT_EQ(123, oldVec2->back());
  }
}

TEST(ObjectCache, ObjectHandleInvalid) {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;
  {
    auto it =
        objcache->getCacheAlloc().allocate(0 /* poolId */, "test key", 100);
    ASSERT_TRUE(it);
    objcache->getCacheAlloc().insertOrReplace(it);
  }

  EXPECT_THROW(objcache->find<Vector>("test key"), std::invalid_argument);
  {
    auto it = objcache->getCacheAlloc().findImpl("test key", AccessMode::kRead);
    ASSERT_TRUE(it);
    EXPECT_THROW(LruObjectCache::toObjectHandle<Vector>(std::move(it)),
                 std::invalid_argument);
  }
  {
    auto it = objcache->getCacheAlloc().findImpl("test key", AccessMode::kRead);
    ASSERT_TRUE(it);
    EXPECT_THROW(LruObjectCache::toSharedPtr<Vector>(std::move(it)),
                 std::invalid_argument);
  }
}

TEST(ObjectCache, ObjectWithVTable) {
  struct Parent {
    Parent(LruObjectCache::Alloc<char> allocator) : alloc_{allocator} {}
    virtual ~Parent() = default;
    virtual std::string foo() { return "parent"; }
    LruObjectCache::Alloc<char> alloc_{};
  };

  struct Child : public Parent {
    using Parent::Parent;
    std::string foo() override { return "child"; }
  };

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  {
    auto p = objcache->create<Parent>(0 /* poolId */, "test parent");
    EXPECT_EQ("parent", p->foo());
    objcache->insertOrReplace(p);
    auto c = objcache->create<Child>(0 /* poolId */, "test child");
    EXPECT_EQ("child", c->foo());
    objcache->insertOrReplace(c);
  }

  // Get handles to the exact same type, verify behavior is correct
  {
    auto p = objcache->find<Parent>("test parent");
    EXPECT_EQ("parent", p->foo());
    auto c = objcache->find<Parent>("test child");
    EXPECT_EQ("child", c->foo());
  }

  // Get handles to the different types, verify behavior is correct
  {
    auto p = objcache->find<Child>("test parent");
    EXPECT_EQ("parent", p->foo());
    auto c = objcache->find<Parent>("test child");
    EXPECT_EQ("child", c->foo());
  }
}

template <typename K, typename V>
using ObjCacheF14Map =
    folly::F14FastMap<K,
                      V,
                      std::hash<K>,
                      std::equal_to<K>,
                      LruObjectCache::Alloc<std::pair<const K, V>>>;
TEST(ObjectCache, F14MapSimple) {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  for (int i = 0; i < 100; i++) {
    auto f14 = objcache->create<ObjCacheF14Map<int, int>>(
        0 /* poolId */, folly::sformat("key_{}", i));
    for (int j = 0; j < 100; j++) {
      f14->insert(std::make_pair(j, i));
    }
    objcache->insertOrReplace(f14);
  }
  for (int i = 0; i < 100; i++) {
    auto f14 =
        objcache->find<ObjCacheF14Map<int, int>>(folly::sformat("key_{}", i));
    for (int j = 0; j < 100; j++) {
      EXPECT_EQ(i, (*f14)[j]);
    }
  }
}

TEST(ObjectCache, F14MapWithStrings) {
  using Map = ObjCacheF14Map<int, ObjCacheString>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  ObjCacheString ts = "01234567890123456789";
  for (int i = 0; i < 100; i++) {
    auto f14 =
        objcache->create<Map>(0 /* poolId */, folly::sformat("key_{}", i));
    for (int j = 0; j < 100; j++) {
      f14->insert(std::make_pair(j, ts));
    }
    objcache->insertOrReplace(f14);
  }
  for (int i = 0; i < 100; i++) {
    auto f14 = objcache->find<Map>(folly::sformat("key_{}", i));
    for (int j = 0; j < 100; j++) {
      EXPECT_EQ(ts, (*f14)[j]);
    }
  }
}

TEST(ObjectCache, F14MapWithKeyStrings) {
  using Map = ObjCacheF14Map<ObjCacheString, ObjCacheString>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  ObjCacheString ts = "01234567890123456789";
  for (int i = 0; i < 100; i++) {
    auto f14 =
        objcache->create<Map>(0 /* poolId */, folly::sformat("key_{}", i));
    for (int j = 0; j < 100; j++) {
      f14->insert(std::make_pair(
          ObjCacheString{folly::sformat("key_{}", j).c_str()},
          ObjCacheString{folly::sformat("val_{}_{}", ts, j).c_str()}));
    }
    objcache->insertOrReplace(f14);
  }
  for (int i = 0; i < 100; i++) {
    auto f14 = objcache->find<Map>(folly::sformat("key_{}", i));
    for (int j = 0; j < 100; j++) {
      EXPECT_EQ(ObjCacheString{folly::sformat("val_{}_{}", ts, j).c_str()},
                (*f14)[ObjCacheString{folly::sformat("key_{}", j).c_str()}]);
    }
  }
}

TEST(ObjectCache, NestedContainers) {
  using Nested = ObjCacheF14Map<ObjCacheString,
                                ObjCacheF14Map<ObjCacheString, ObjCacheString>>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  auto objcache = createCache(config);

  ObjCacheString ts = "01234567890123456789";
  for (int i = 0; i < 10; i++) {
    auto f14 =
        objcache->create<Nested>(0 /* poolId */, folly::sformat("key_{}", i));
    for (int j = 0; j < 10; j++) {
      for (int z = 0; z < 10; z++) {
        ObjCacheString key{folly::sformat("key_{}", j).c_str()};
        (*f14)[key].insert(std::make_pair(
            ObjCacheString{folly::sformat("key_{}", z).c_str()},
            ObjCacheString{folly::sformat("val_{}_{}", ts, z).c_str()}));
      }
    }
    objcache->insertOrReplace(f14);
  }
  for (int i = 0; i < 10; i++) {
    auto f14 = objcache->find<Nested>(folly::sformat("key_{}", i));
    for (int j = 0; j < 10; j++) {
      for (int z = 0; z < 10; z++) {
        ObjCacheString key{folly::sformat("key_{}", j).c_str()};
        EXPECT_EQ(
            ObjCacheString{folly::sformat("val_{}_{}", ts, z).c_str()},
            (*f14)[key][ObjCacheString{folly::sformat("key_{}", z).c_str()}]);
      }
    }
  }
}

TEST(ObjCache, Destructor) {
  struct Foo {
    ~Foo() {
      if (destructorCalled) {
        *destructorCalled = true;
      }
    }
    bool* destructorCalled{};
  };
  using VectorOfFoo = std::vector<Foo, LruObjectCache::Alloc<Foo>>;
  using VectorOfHeapString =
      std::vector<std::string, LruObjectCache::Alloc<std::string>>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.setDestructorCallback([](folly::StringPiece key, void* unalignedMem,
                                  const LruAllocator::RemoveCbData&) {
    if (key == "test vec foo") {
      getType<VectorOfFoo,
              MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
          unalignedMem)
          ->~VectorOfFoo();
    } else if (key == "test vec string") {
      getType<VectorOfHeapString,
              MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
          unalignedMem)
          ->~VectorOfHeapString();
    }
  });
  auto objcache = createCache(config);

  {
    bool dc{false};
    {
      auto vec = objcache->create<VectorOfFoo>(0 /* poolId */, "test vec foo");
      vec->push_back({});
      vec->at(0).destructorCalled = &dc;
    }
    ASSERT_TRUE(dc);
  }

  {
    auto vec =
        objcache->create<VectorOfHeapString>(0 /* poolId */, "test vec string");
    for (int i = 0; i < 10; i++) {
      vec->push_back("hello world 0123456789");
    }
    // If destructor callback isn't properly executed, we would trigger
    // ASAN failures since std::string has heap storage
  }
}

TEST(ObjCache, DestructorWithVTable) {
  struct Parent {
    Parent(LruObjectCache::Alloc<char> allocator) : alloc_{allocator} {}
    virtual ~Parent() {
      if (parentDestructorCalled) {
        *parentDestructorCalled = true;
      }
    }
    virtual std::string foo() { return "parent"; }
    bool* parentDestructorCalled{};

    LruObjectCache::Alloc<char> alloc_{};
  };

  struct Child : public Parent {
    using Parent::Parent;
    ~Child() override {
      if (childDestructorCalled) {
        *childDestructorCalled = true;
      }
    }
    std::string foo() override { return "child"; }
    bool* childDestructorCalled{};
  };

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.setDestructorCallback([](folly::StringPiece key, void* unalignedMem,
                                  const LruAllocator::RemoveCbData&) {
    if (key.startsWith("test")) {
      getType<Parent, MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
          unalignedMem)
          ->~Parent();
    }
  });
  auto objcache = createCache(config);

  {
    bool parentDc{false};
    {
      auto p = objcache->create<Parent>(0 /* poolId */, "test parent");
      EXPECT_EQ("parent", p->foo());
      p->parentDestructorCalled = &parentDc;
    }
    ASSERT_TRUE(parentDc);
  }

  {
    bool childDc{false};
    {
      auto c = objcache->create<Child>(0 /* poolId */, "test child");
      EXPECT_EQ("child", c->foo());
      c->childDestructorCalled = &childDc;
    }
    ASSERT_TRUE(childDc);
  }
}

TEST(ObjectCache, Compaction) {
  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;
  std::mutex lock;
  struct TestSyncObj : public LruObjectCache::CompactionSyncObj {
    explicit TestSyncObj(std::mutex& lock) : l{lock} {}
    std::lock_guard<std::mutex> l;
  };

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.enableCompaction(
      [](folly::StringPiece key, void* unalignedMem, const auto& compactor) {
        if (key == "test vec int") {
          compactor.template compact<Vector>(key, unalignedMem);
        }
      },
      [&lock](folly::StringPiece key)
          -> std::unique_ptr<LruObjectCache::CompactionSyncObj> {
        if (key == "test vec int") {
          return std::unique_ptr<LruObjectCache::CompactionSyncObj>{
              new TestSyncObj(lock)};
        }
        return nullptr;
      });
  auto objcache = createCache(config);

  // Create a new vector in cache
  uint32_t bytesOccupied = 0;
  {
    std::lock_guard<std::mutex> l{lock};
    auto vec = objcache->create<Vector>(0 /* poolId */, "test vec int");
    ASSERT_TRUE(vec);
    EXPECT_EQ(0, vec->size());
    vec->push_back(123);
    EXPECT_EQ(1, vec->size());
    for (int i = 0; i < 1000; i++) {
      vec->push_back(i);
    }
    EXPECT_EQ(1001, vec->size());

    bytesOccupied += vec.viewItemHandle()->getSize();
    auto chainedAllocs =
        objcache->getCacheAlloc().viewAsChainedAllocs(vec.viewItemHandle());
    for (const auto& c : chainedAllocs.getChain()) {
      bytesOccupied += c.getSize();
    }

    XLOG(INFO) << vec->get_allocator()
                      .getAllocatorResource()
                      .viewMetadata()
                      ->usedBytes;

    // Insert into cache
    objcache->insertOrReplace(vec);
  }

  // The object will be compacted twice because the first time the vector
  // has reserved 1024 slots (4096 bytes) but we only used 1001 slots. So,
  // the compaction worker will further compact it on a second iteration.
  objcache->triggerCompactionForTesting();
  while (objcache->getStats().compactions < 2) {
  }

  uint32_t compactedBytesOccupied = 0;
  {
    std::lock_guard<std::mutex> l{lock};
    auto vec = objcache->find<Vector>("test vec int");
    EXPECT_EQ(1001, vec->size());
    compactedBytesOccupied += vec.viewItemHandle()->getSize();
    EXPECT_FALSE(vec.viewItemHandle()->hasChainedItem());
  }

  EXPECT_NE(bytesOccupied, compactedBytesOccupied);
  EXPECT_EQ(16482, bytesOccupied);
  EXPECT_EQ(4110, compactedBytesOccupied);
}

TEST(ObjectCache, PersistenceSimple) {
  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  std::string tmpFilePath = std::tmpnam(nullptr);
  config.enablePersistence(
      5 /* persistorRestorerThreadCount */,
      0 /* restorerTimeOutDurationInSec */,
      tmpFilePath /* persistFullPathFile */,
      [](folly::StringPiece key, void* unalignedMem) {
        if (key.startsWith("my obj")) {
          auto* vec =
              getType<Vector,
                      MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
                  unalignedMem);
          return Serializer::serializeToIOBuf(*vec);
        }
        return std::unique_ptr<folly::IOBuf>(nullptr);
      },
      [](PoolId poolId,
         folly::StringPiece key,
         folly::StringPiece payload,
         uint32_t creationTime,
         uint32_t expiryTime,
         LruObjectCache& cache) {
        if (key.toString().starts_with("my obj")) {
          Deserializer deserializer{
              reinterpret_cast<const uint8_t*>(payload.begin()),
              reinterpret_cast<const uint8_t*>(payload.end())};
          auto tmp = deserializer.deserialize<Vector>();

          // TODO: we're compacting here for better space efficiency. However,
          //       this requires 3 copies. Deserialize -> first object ->
          //       compacted object. It's wasteful. Can we do better?
          auto vecTmp = cache.create<Vector>(poolId, key, tmp);
          if (!vecTmp) {
            return LruObjectCache::ItemHandle{};
          }

          std::chrono::seconds ttl = std::chrono::seconds(
              expiryTime > 0 ? expiryTime - creationTime : 0);
          auto vec = cache.createCompact<Vector>(poolId, key, *vecTmp,
                                                 ttl.count(), creationTime);
          if (!vec) {
            return LruObjectCache::ItemHandle{};
          }
          return std::move(vec).releaseItemHandle();
        }
        return LruObjectCache::ItemHandle{};
      },
      2 /* persistorQueueBatchSize */);

  // Create few vector in the cache
  const auto ttlLong = std::chrono::seconds(100);
  const auto ttlShort = std::chrono::seconds(1);
  size_t numOfItemInCache = 2 * 10;
  {
    auto objcache = createCache(config);
    for (size_t j = 0; j < numOfItemInCache; j++) {
      auto vec = objcache->create<Vector>(
          0 /* poolId */, "my obj " + folly::to<std::string>(j));
      ASSERT_TRUE(vec);
      EXPECT_EQ(0, vec->size());
      vec->push_back(123);
      EXPECT_EQ(1, vec->size());
      for (int i = 0; i < 1000; i++) {
        vec->push_back(i);
      }
      EXPECT_EQ(1001, vec->size());

      // Insert into cache
      objcache->insertOrReplace(vec);
      // extend ttl
      if (j % 2 == 0) {
        vec.viewItemHandle()->extendTTL(ttlLong);
        EXPECT_EQ(ttlLong, vec.viewItemHandle()->getConfiguredTTL());
      } else {
        vec.viewItemHandle()->extendTTL(ttlShort);
        EXPECT_EQ(ttlShort, vec.viewItemHandle()->getConfiguredTTL());
      }
      EXPECT_NE(0, vec.viewItemHandle()->getExpiryTime());
    }

    // Persist
    objcache->persist();
  }

  {
    auto objcache = createCache(config);

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(2));
    objcache->recover();

    for (size_t j = 0; j < numOfItemInCache; j++) {
      auto vec = objcache->find<Vector>("my obj " + folly::to<std::string>(j));
      if (j % 2 == 0) {
        ASSERT_TRUE(vec);
        EXPECT_NE(0, vec.viewItemHandle()->getExpiryTime());
        EXPECT_EQ(ttlLong, vec.viewItemHandle()->getConfiguredTTL());
        EXPECT_EQ(1001, vec->size());

        for (int i = 999; i >= 0; i--) {
          ASSERT_EQ(i, vec->back());
          vec->pop_back();
        }
        EXPECT_EQ(1, vec->size());
        EXPECT_EQ(123, vec->back());
      } else {
        // item is expired so we will not recover it
        ASSERT_FALSE(vec);
      }
    }
  }
}

TEST(ObjectCache, PersistenceSimpleWithRecoverTimeOut) {
  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  std::string tmpFilePath = std::tmpnam(nullptr);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.enablePersistence(
      1 /* persistorRestorerThreadCount */,
      1 /* restorerTimeOutDurationInSec */,
      tmpFilePath /* persistFullPathFile */,
      [](folly::StringPiece key, void* unalignedMem) {
        if (key == "my obj one" || key == "my obj two") {
          auto* vec =
              getType<Vector,
                      MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
                  unalignedMem);
          return Serializer::serializeToIOBuf(*vec);
        }
        return std::unique_ptr<folly::IOBuf>(nullptr);
      },
      [](PoolId poolId,
         folly::StringPiece key,
         folly::StringPiece payload,
         uint32_t creationTime,
         uint32_t expiryTime,
         LruObjectCache& cache) {
        // add a sleep to mimic delay in deserialization and recovery of
        // cache
        // content.
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::seconds(10));

        if (key == "my obj one" || key == "my obj two") {
          Deserializer deserializer{
              reinterpret_cast<const uint8_t*>(payload.begin()),
              reinterpret_cast<const uint8_t*>(payload.end())};
          auto tmp = deserializer.deserialize<Vector>();

          // TODO: we're compacting here for better space efficiency.
          // However, this requires 3 copies. Deserialize -> first object ->
          // compacted object. It's wasteful. Can we do better?
          auto vecTmp = cache.create<Vector>(poolId, key, tmp);
          if (!vecTmp) {
            return LruObjectCache::ItemHandle{};
          }

          std::chrono::seconds ttl = std::chrono::seconds(
              expiryTime > 0 ? expiryTime - creationTime : 0);
          auto vec = cache.createCompact<Vector>(poolId, key, *vecTmp,
                                                 ttl.count(), creationTime);
          if (!vec) {
            return LruObjectCache::ItemHandle{};
          }
          return std::move(vec).releaseItemHandle();
        }
        return LruObjectCache::ItemHandle{};
      });

  // Create two vectors in cache
  {
    auto objcache = createCache(config);
    auto vecOne = objcache->create<Vector>(0 /* poolId */, "my obj one");
    ASSERT_TRUE(vecOne);
    EXPECT_EQ(0, vecOne->size());
    vecOne->push_back(123);
    EXPECT_EQ(1, vecOne->size());
    for (int i = 0; i < 1000; i++) {
      vecOne->push_back(i);
    }
    EXPECT_EQ(1001, vecOne->size());

    // Insert into cache
    objcache->insertOrReplace(vecOne);

    auto vecTwo = objcache->create<Vector>(0 /* poolId */, "my obj two");
    ASSERT_TRUE(vecTwo);
    EXPECT_EQ(0, vecTwo->size());
    vecTwo->push_back(123);
    EXPECT_EQ(1, vecTwo->size());
    for (int i = 0; i < 1000; i++) {
      vecTwo->push_back(i);
    }
    EXPECT_EQ(1001, vecTwo->size());

    // Insert into cache
    objcache->insertOrReplace(vecTwo);

    // Persist
    objcache->persist();
  }

  {
    auto objcache = createCache(config);
    objcache->recover();

    // due to time out at least one of the vectors won't be retrieved given
    // recovery happens through one thread.
    auto vecOne = objcache->find<Vector>("my obj one");
    auto vecTwo = objcache->find<Vector>("my obj two");
    ASSERT_FALSE(vecOne && vecTwo);
  }
}

TEST(ObjectCache, PersistenceMultipleTypes) {
  using VecInt = std::vector<int, LruObjectCache::Alloc<int>>;
  using VecStr =
      std::vector<ObjCacheString, LruObjectCache::Alloc<ObjCacheString>>;
  using MapStr = std::map<
      ObjCacheString, ObjCacheString, std::less<ObjCacheString>,
      LruObjectCache::Alloc<std::pair<const ObjCacheString, ObjCacheString>>>;

  std::string tmpFilePath = std::tmpnam(nullptr);
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.enablePersistence(
      5 /* persistorRestorerThreadCount */,
      0 /* restorerTimeOutDurationInSec, no time out */,
      tmpFilePath /* persistFullPathFile */,
      [](folly::StringPiece key, void* unalignedMem) {
        if (key.startsWith("vec_int")) {
          auto* obj =
              getType<VecInt,
                      MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
                  unalignedMem);
          return Serializer::serializeToIOBuf(*obj);
        } else if (key.startsWith("vec_str")) {
          auto* obj =
              getType<VecStr,
                      MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
                  unalignedMem);
          return Serializer::serializeToIOBuf(*obj);
        } else if (key.startsWith("map_str")) {
          auto* obj =
              getType<MapStr,
                      MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
                  unalignedMem);
          return Serializer::serializeToIOBuf(*obj);
        }
        return std::unique_ptr<folly::IOBuf>(nullptr);
      },
      [](PoolId poolId,
         folly::StringPiece key,
         folly::StringPiece payload,
         uint32_t /* unused creationTime */,
         uint32_t /* unused expiryTime */,
         LruObjectCache& cache) {
        Deserializer deserializer{
            reinterpret_cast<const uint8_t*>(payload.begin()),
            reinterpret_cast<const uint8_t*>(payload.end())};
        if (key.startsWith("vec_int")) {
          auto tmp = deserializer.deserialize<VecInt>();
          auto obj = cache.create<VecInt>(poolId, key, tmp);
          if (!obj) {
            return LruObjectCache::ItemHandle{};
          }
          return std::move(obj).releaseItemHandle();
        } else if (key.startsWith("vec_str")) {
          auto tmp = deserializer.deserialize<VecStr>();
          auto obj = cache.create<VecStr>(poolId, key, tmp);
          if (!obj) {
            return LruObjectCache::ItemHandle{};
          }
          return std::move(obj).releaseItemHandle();
        } else if (key.startsWith("map_str")) {
          auto tmp = deserializer.deserialize<MapStr>();
          auto obj = cache.create<MapStr>(poolId, key, tmp);
          if (!obj) {
            return LruObjectCache::ItemHandle{};
          }
          return std::move(obj).releaseItemHandle();
        }
        return LruObjectCache::ItemHandle{};
      },
      2 /* persistorQueueBatchSize */);

  folly::IOBufQueue queue;
  // Create a new vector in cache
  size_t numOfItemInCache = 10;
  {
    auto objcache = createCache(config);
    for (size_t j = 0; j < numOfItemInCache; j++) {
      auto vecInt = objcache->create<VecInt>(
          0 /* poolId */, "vec_int test " + folly::to<std::string>(j));
      vecInt->push_back(123);
      objcache->insertOrReplace(vecInt);

      auto vecStr = objcache->create<VecStr>(
          0 /* poolId */, "vec_str test " + folly::to<std::string>(j));
      vecStr->push_back("hello world 1234567890");
      objcache->insertOrReplace(vecStr);

      auto mapStr = objcache->create<MapStr>(
          0 /* poolId */, "map_str test " + folly::to<std::string>(j));
      (*mapStr)["random key 123"] = ("hello world 1234567890");
      objcache->insertOrReplace(mapStr);
    }

    // Persist
    objcache->persist();
  }

  {
    auto objcache = createCache(config);
    objcache->recover();

    for (size_t j = 0; j < numOfItemInCache; j++) {
      auto vecInt =
          objcache->find<VecInt>("vec_int test " + folly::to<std::string>(j));
      ASSERT_TRUE(vecInt);
      EXPECT_EQ(123, vecInt->back());

      auto vecStr =
          objcache->find<VecStr>("vec_str test " + folly::to<std::string>(j));
      ASSERT_TRUE(vecStr);
      EXPECT_EQ("hello world 1234567890", vecStr->back());

      auto mapStr =
          objcache->find<MapStr>("map_str test " + folly::to<std::string>(j));
      ASSERT_TRUE(mapStr);
      EXPECT_EQ("hello world 1234567890", (*mapStr)["random key 123"]);
    }
  }
}

TEST(ObjectCache, PromiseSimple) {
  struct IntPromiseWrapper {
    explicit IntPromiseWrapper(LruObjectCache::Alloc<char> a) : alloc{a} {}

    // Note the future "core" is on heap memory and not managed by cachelib
    folly::Promise<int> promise;
    LruObjectCache::Alloc<char> alloc;
  };
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.setDestructorCallback([](folly::StringPiece key, void* unalignedMem,
                                  const LruAllocator::RemoveCbData&) {
    if (key == "promise int") {
      getType<IntPromiseWrapper, LruObjectCache::AllocatorResource>(
          unalignedMem)
          ->~IntPromiseWrapper();
    }
  });
  auto objcache = createCache(config);

  auto sfInt = folly::SemiFuture<int>::makeEmpty();
  {
    auto p = objcache->create<IntPromiseWrapper>(0 /* poolId */, "promise int");
    sfInt = p->promise.getSemiFuture();
    objcache->insertOrReplace(p);
  }
  EXPECT_FALSE(sfInt.isReady());

  {
    auto p = objcache->find<IntPromiseWrapper>("promise int");
    p->promise.setValue(123);
  }
  EXPECT_TRUE(sfInt.isReady());
  EXPECT_EQ(123, std::move(sfInt).get());

  objcache->remove("promise int");
}

namespace {
template <typename T>
struct PromiseWrapper {
  using ObjectType = T;

  PromiseWrapper(folly::Promise<T*> p, LruObjectCache::Alloc<char> a)
      : promise{std::move(p)}, alloc{a} {}
  ~PromiseWrapper() {
    if (obj) {
      obj->~ObjectType();
    }
  }

  // Note the future "core" is on heap memory and not managed by cachelib
  folly::Promise<T*> promise;
  ObjectType* obj{};
  LruObjectCache::Alloc<char> alloc;
};
} // namespace
TEST(ObjectCache, PromiseColocateObject) {
  using StrPromiseWrapper = PromiseWrapper<ObjCacheString>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.setDestructorCallback([](folly::StringPiece key, void* unalignedMem,
                                  const LruAllocator::RemoveCbData&) {
    if (key == "promise str") {
      getType<StrPromiseWrapper, LruObjectCache::AllocatorResource>(
          unalignedMem)
          ->~StrPromiseWrapper();
    }
  });
  auto objcache = createCache(config);

  auto sfStr = folly::SemiFuture<ObjCacheString*>::makeEmpty();
  {
    auto [pr, sf] = folly::makePromiseContract<ObjCacheString*>();
    auto p = objcache->create<StrPromiseWrapper>(0 /* poolId */, "promise str",
                                                 std::move(pr));
    sfStr = std::move(sf);
    objcache->insertOrReplace(p);
  }
  EXPECT_FALSE(sfStr.isReady());

  ObjCacheString someString{"01234567890123456789"};
  {
    auto p = objcache->find<StrPromiseWrapper>("promise str");

    // Create a string backed by the same allocator
    ObjCacheString* str =
        LruObjectCache::Alloc<ObjCacheString>(p->alloc).allocate(1);
    new (str) ObjCacheString(someString, p->alloc);
    p->obj = str;

    p->promise.setValue(p->obj);
  }
  EXPECT_TRUE(sfStr.isReady());
  EXPECT_EQ(someString, *std::move(sfStr).get());

  objcache->remove("promise str");
}

namespace {
template <typename T>
struct SharedPromiseWrapper {
  using ObjectType = T;

  SharedPromiseWrapper(folly::SharedPromise<T*> p,
                       LruObjectCache::Alloc<char> a)
      : promise{std::move(p)}, alloc{a} {}
  ~SharedPromiseWrapper() {
    if (obj) {
      obj->~ObjectType();
    }
  }

  // Note the future "core" is on heap memory and not managed by cachelib
  folly::SharedPromise<T*> promise;
  ObjectType* obj{};
  LruObjectCache::Alloc<char> alloc;
};
} // namespace
TEST(ObjectCache, SharedPromiseColocateObject) {
  using StrPromiseWrapper = SharedPromiseWrapper<ObjCacheString>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.setDestructorCallback([](folly::StringPiece key, void* unalignedMem,
                                  const LruAllocator::RemoveCbData&) {
    if (key == "promise str") {
      getType<StrPromiseWrapper, LruObjectCache::AllocatorResource>(
          unalignedMem)
          ->~StrPromiseWrapper();
    }
  });
  auto objcache = createCache(config);
  const auto ttl = std::chrono::seconds(5);

  std::vector<folly::SemiFuture<
      std::pair<ObjCacheString*, std::shared_ptr<StrPromiseWrapper>>>>
      semiFutures;
  {
    auto p = objcache->create<StrPromiseWrapper>(
        0 /* poolId */, "promise str", folly::SharedPromise<ObjCacheString*>{});
    objcache->insertOrReplace(p);

    EXPECT_EQ(0, p.viewItemHandle()->getExpiryTime());
    EXPECT_EQ(0, p.viewItemHandle()->getConfiguredTTL().count());
    // extend ttl
    p.viewItemHandle()->extendTTL(ttl);
    EXPECT_NE(0, p.viewItemHandle()->getExpiryTime());
    EXPECT_EQ(ttl, p.viewItemHandle()->getConfiguredTTL());

    auto sp = std::move(p).toSharedPtr();

    for (int i = 0; i < 3; i++) {
      semiFutures.push_back(sp->promise.getSemiFuture().deferValue(
          [sp](ObjCacheString* str) mutable
          -> folly::SemiFuture<
              std::pair<ObjCacheString*, std::shared_ptr<StrPromiseWrapper>>> {
            return std::make_pair(str, std::move(sp));
          }));
    }
  }

  ObjCacheString someString{"01234567890123456789"};
  auto readString =
      [&](folly::SemiFuture<std::pair<
              ObjCacheString*, std::shared_ptr<StrPromiseWrapper>>> sfStr) {
        sfStr.wait();
        auto pair = std::move(sfStr).get();
        EXPECT_EQ(someString, *pair.first);
      };
  std::vector<std::thread> ts;
  for (auto& sf : semiFutures) {
    ts.push_back(std::thread{readString, std::move(sf)});
  }

  {
    auto p = objcache->find<StrPromiseWrapper>("promise str");

    EXPECT_NE(0, p.viewItemHandle()->getExpiryTime());
    EXPECT_EQ(ttl, p.viewItemHandle()->getConfiguredTTL());

    // Create a string backed by the same allocator
    ObjCacheString* str =
        LruObjectCache::Alloc<ObjCacheString>(p->alloc).allocate(1);
    new (str) ObjCacheString(someString, p->alloc);
    p->obj = str;
    p->promise.setValue(p->obj);
  }

  for (auto& t : ts) {
    t.join();
  }

  objcache->remove("promise str");
}

TEST(ObjectCache, SharedPromiseColocateObject2) {
  using StrPromiseWrapper = SharedPromiseWrapper<ObjCacheString>;

  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(100 * 1024 * 1024);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.setDestructorCallback([](folly::StringPiece key, void* unalignedMem,
                                  const LruAllocator::RemoveCbData&) {
    if (key == "promise str") {
      getType<StrPromiseWrapper, LruObjectCache::AllocatorResource>(
          unalignedMem)
          ->~StrPromiseWrapper();
    }
  });
  auto objcache = createCache(config);

  {
    auto p = objcache->create<StrPromiseWrapper>(
        0 /* poolId */, "promise str", folly::SharedPromise<ObjCacheString*>{});
    objcache->insertOrReplace(p);
  }

  std::vector<folly::SemiFuture<std::shared_ptr<ObjCacheString>>> semiFutures;
  {
    auto p = objcache->find<StrPromiseWrapper>("promise str");
    auto sp = std::move(p).toSharedPtr();

    for (int i = 0; i < 3; i++) {
      semiFutures.push_back(sp->promise.getSemiFuture().deferValue(
          [sp](ObjCacheString* str) mutable
          -> folly::SemiFuture<std::shared_ptr<ObjCacheString>> {
            return std::shared_ptr<ObjCacheString>{
                str, [sp = std::move(sp)](auto /* unused */) {
                  // once sp goes out of
                  // scope, the underlying
                  // item handle will be
                  // destructed and refcount
                  // decremented properly
                }};
          }));
    }
  }

  ObjCacheString someString{"01234567890123456789"};
  auto readString =
      [&](folly::SemiFuture<std::shared_ptr<ObjCacheString>> sfStr) {
        sfStr.wait();
        auto str = std::move(sfStr).get();
        EXPECT_EQ(someString, *str);
      };
  std::vector<std::thread> ts;
  for (auto& sf : semiFutures) {
    ts.push_back(std::thread{readString, std::move(sf)});
  }

  {
    auto p = objcache->find<StrPromiseWrapper>("promise str");

    // Create a string backed by the same allocator
    ObjCacheString* str =
        LruObjectCache::Alloc<ObjCacheString>(p->alloc).allocate(1);
    new (str) ObjCacheString(someString, p->alloc);
    p->obj = str;
    p->promise.setValue(p->obj);
  }

  for (auto& t : ts) {
    t.join();
  }

  objcache->remove("promise str");
}
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook
