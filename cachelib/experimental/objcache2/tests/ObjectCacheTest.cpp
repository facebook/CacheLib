#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/experimental/objcache2/ObjectCache.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {
namespace test {
namespace {
struct Foo {
  int a{};
  int b{};
  int c{};
};
} // namespace

template <typename AllocatorT>
class ObjectCacheTest : public ::testing::Test {
 public:
  void testGetAllocSize() {
    std::vector<uint8_t> maxKeySizes{};
    std::vector<uint32_t> allocSizes{};

    for (uint8_t keySize = 8; keySize < 255; keySize++) {
      maxKeySizes.push_back(keySize);
      allocSizes.push_back(
          ObjectCache<AllocatorT>::template getL1AllocSize<Foo>(keySize));
    }

    for (size_t i = 0; i < maxKeySizes.size(); i++) {
      EXPECT_TRUE(allocSizes[i] >= ObjectCache<AllocatorT>::kL1AllocSizeMin);
      EXPECT_TRUE(maxKeySizes[i] + sizeof(ObjectCacheItem<Foo>) +
                      sizeof(typename AllocatorT::Item) <=
                  allocSizes[i]);
      EXPECT_TRUE(allocSizes[i] % 8 == 0);
    }
  }

  void testSimple() {
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto found1 = objcache->template find<Foo>("Foo");
    EXPECT_EQ(nullptr, found1);

    auto foo = std::make_unique<Foo>();
    foo->a = 1;
    foo->b = 2;
    foo->c = 3;
    auto res = objcache->insertOrReplace("Foo", std::move(foo));
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);
    ASSERT_NE(nullptr, res.second);
    EXPECT_EQ(1, res.second->a);
    EXPECT_EQ(2, res.second->b);
    EXPECT_EQ(3, res.second->c);

    auto found2 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found2);
    EXPECT_EQ(1, found2->a);
    EXPECT_EQ(2, found2->b);
    EXPECT_EQ(3, found2->c);
  }

  void testExpiration() {
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto foo = std::make_unique<Foo>();
    foo->a = 1;
    foo->b = 2;
    foo->c = 3;

    objcache->insertOrReplace("Foo", std::move(foo), 0 /*object size*/,
                              2 /* seconds */);

    auto found1 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found1);
    EXPECT_EQ(1, found1->a);
    EXPECT_EQ(2, found1->b);
    EXPECT_EQ(3, found1->c);

    std::this_thread::sleep_for(std::chrono::seconds{3});
    auto found2 = objcache->template find<Foo>("Foo");
    ASSERT_EQ(nullptr, found2);
  }

  void testReplace() {
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto foo1 = std::make_unique<Foo>();
    foo1->a = 1;
    foo1->b = 2;
    foo1->c = 3;
    std::shared_ptr<Foo> replaced;
    auto res =
        objcache->insertOrReplace("Foo", std::move(foo1), 0, 0, &replaced);
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);
    EXPECT_EQ(nullptr, replaced);

    auto found1 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found1);
    EXPECT_EQ(1, found1->a);
    EXPECT_EQ(2, found1->b);
    EXPECT_EQ(3, found1->c);

    auto foo2 = std::make_unique<Foo>();
    foo2->a = 10;
    foo2->b = 20;
    foo2->c = 30;
    res = objcache->insertOrReplace("Foo", std::move(foo2), 0, 0, &replaced);
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);
    ASSERT_NE(nullptr, replaced);
    EXPECT_EQ(1, replaced->a);
    EXPECT_EQ(2, replaced->b);
    EXPECT_EQ(3, replaced->c);

    auto found2 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found2);
    EXPECT_EQ(10, found2->a);
    EXPECT_EQ(20, found2->b);
    EXPECT_EQ(30, found2->c);
  }

  void testUniqueInsert() {
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto foo1 = std::make_unique<Foo>();
    foo1->a = 1;
    foo1->b = 2;
    foo1->c = 3;
    auto res = objcache->insert("Foo", std::move(foo1));
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);

    auto found1 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found1);
    EXPECT_EQ(1, found1->a);
    EXPECT_EQ(2, found1->b);
    EXPECT_EQ(3, found1->c);

    auto foo2 = std::make_unique<Foo>();
    foo2->a = 10;
    foo2->b = 20;
    foo2->c = 30;
    res = objcache->insert("Foo", std::move(foo2));
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kKeyAlreadyExists,
              res.first);

    auto found2 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found1);
    EXPECT_EQ(1, found2->a);
    EXPECT_EQ(2, found2->b);
    EXPECT_EQ(3, found2->c);

    objcache->remove("Foo");
  }

  void testObjectSizeTrackingBasics() {
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    config.objectSizeTrackingEnabled = true;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);
    EXPECT_EQ(objcache->getTotalObjectSize(), 0);
    auto foo1 = std::make_unique<Foo>();
    foo1->a = 1;
    foo1->b = 2;
    foo1->c = 3;
    auto foo1Size = 64;
    auto foo2 = std::make_unique<Foo>();
    foo2->a = 10;
    foo2->b = 20;
    foo2->c = 30;
    auto foo2Size = 128;

    // will throw without the object size
    ASSERT_THROW(objcache->insertOrReplace("Foo", std::make_unique<Foo>()),
                 std::invalid_argument);

    // insert foo1
    {
      auto res = objcache->insertOrReplace("Foo", std::move(foo1), foo1Size);
      ASSERT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);

      auto found = objcache->template find<Foo>("Foo");
      ASSERT_NE(nullptr, found);
      ASSERT_EQ(1, found->a);
      ASSERT_EQ(2, found->b);
      ASSERT_EQ(3, found->c);
    }
    ASSERT_EQ(objcache->getNumEntries(), 1);
    ASSERT_EQ(objcache->getTotalObjectSize(), foo1Size);

    // replace foo1 with foo2
    {
      auto res = objcache->insertOrReplace("Foo", std::move(foo2), foo2Size);
      ASSERT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);

      auto found = objcache->template find<Foo>("Foo");
      ASSERT_NE(nullptr, found);
      ASSERT_EQ(10, found->a);
      ASSERT_EQ(20, found->b);
      ASSERT_EQ(30, found->c);
    }
    ASSERT_EQ(objcache->getNumEntries(), 1);
    ASSERT_EQ(objcache->getTotalObjectSize(), foo2Size);

    // remove foo2
    objcache->remove("Foo");
    ASSERT_EQ(nullptr, objcache->template find<Foo>("Foo"));
    ASSERT_EQ(objcache->getNumEntries(), 0);
    ASSERT_EQ(objcache->getTotalObjectSize(), 0);
  }

  void testObjectSizeTrackingUniqueInsert() {
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    config.objectSizeTrackingEnabled = true;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    // will throw without the object size
    ASSERT_THROW(objcache->insert("Foo", std::make_unique<Foo>()),
                 std::invalid_argument);

    auto foo1 = std::make_unique<Foo>();
    foo1->a = 1;
    foo1->b = 2;
    foo1->c = 3;
    auto foo1Size = 64;
    auto res = objcache->insert("Foo", std::move(foo1), foo1Size);
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kSuccess, res.first);
    ASSERT_EQ(objcache->getNumEntries(), 1);
    ASSERT_EQ(objcache->getTotalObjectSize(), foo1Size);

    auto found1 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found1);
    EXPECT_EQ(1, found1->a);
    EXPECT_EQ(2, found1->b);
    EXPECT_EQ(3, found1->c);

    auto foo2 = std::make_unique<Foo>();
    foo2->a = 10;
    foo2->b = 20;
    foo2->c = 30;
    auto foo2Size = 128;
    res = objcache->insert("Foo", std::move(foo2), foo2Size);
    EXPECT_EQ(ObjectCache<AllocatorT>::AllocStatus::kKeyAlreadyExists,
              res.first);
    ASSERT_EQ(objcache->getNumEntries(), 1);
    ASSERT_EQ(objcache->getTotalObjectSize(), foo1Size);

    auto found2 = objcache->template find<Foo>("Foo");
    ASSERT_NE(nullptr, found2);
    EXPECT_EQ(1, found2->a);
    EXPECT_EQ(2, found2->b);
    EXPECT_EQ(3, found2->c);
  }

  void testMultithreadReplace() {
    // Sanity test to see if insertOrReplace across multiple
    // threads are safe.
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto runReplaceOps = [&] {
      for (int i = 0; i < 2000; i++) {
        // Rotate through 5 different keys
        auto key = folly::sformat("key_{}", i % 5);
        auto foo2 = std::make_unique<Foo>();
        objcache->insertOrReplace(key, std::move(foo2));
      }
    };

    std::vector<std::thread> ts;
    for (int i = 0; i < 10; i++) {
      ts.push_back(std::thread{runReplaceOps});
    }
    for (int i = 0; i < 10; i++) {
      ts[i].join();
    }
  }

  void testMultithreadEviction() {
    // Sanity test to see if evictions across multiple
    // threads are safe.
    ObjectCacheConfig config;
    config.l1EntriesLimit = 1000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto runInsertOps = [&](int id) {
      for (int i = 0; i < 2000; i++) {
        auto key = folly::sformat("key_{}_{}", id, i);
        auto foo2 = std::make_unique<Foo>();
        objcache->insertOrReplace(key, std::move(foo2));
      }
    };

    std::vector<std::thread> ts;
    for (int i = 0; i < 10; i++) {
      ts.push_back(std::thread{runInsertOps, i});
    }
    for (int i = 0; i < 10; i++) {
      ts[i].join();
    }
  }

  void testMultithreadFindAndReplace() {
    // Sanity test to see if find and insertions at the same time
    // across mutliple threads are safe.
    ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto runReplaceOps = [&] {
      for (int i = 0; i < 2000; i++) {
        // Rotate through 5 different keys
        auto key = folly::sformat("key_{}", i % 5);
        auto foo2 = std::make_unique<Foo>();
        objcache->insertOrReplace(key, std::move(foo2));
      }
    };

    auto runFindOps = [&] {
      for (int i = 0; i < 2000; i++) {
        // Rotate through 5 different keys
        auto key = folly::sformat("key_{}", i % 5);
        auto res = objcache->template find<Foo>(key);
      }
    };

    std::vector<std::thread> rs;
    std::vector<std::thread> fs;
    for (int i = 0; i < 10; i++) {
      rs.push_back(std::thread{runReplaceOps});
      fs.push_back(std::thread{runFindOps});
    }
    for (int i = 0; i < 10; i++) {
      rs[i].join();
      fs[i].join();
    }
  }

  void testMultithreadFindAndEviction() {
    // Sanity test to see if find and evictions across multiple
    // threads are safe.
    ObjectCacheConfig config;
    config.l1EntriesLimit = 1000;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto runInsertOps = [&](int id) {
      for (int i = 0; i < 2000; i++) {
        auto key = folly::sformat("key_{}_{}", id, i);
        auto foo2 = std::make_unique<Foo>();
        objcache->insertOrReplace(key, std::move(foo2));
      }
    };

    auto runFindOps = [&](int id) {
      for (int i = 0; i < 2000; i++) {
        auto key = folly::sformat("key_{}_{}", id, i);
        auto res = objcache->template find<Foo>(key);
      }
    };

    std::vector<std::thread> rs;
    std::vector<std::thread> fs;
    for (int i = 0; i < 10; i++) {
      rs.push_back(std::thread{runInsertOps, i});
      fs.push_back(std::thread{runFindOps, i});
    }
    for (int i = 0; i < 10; i++) {
      rs[i].join();
      fs[i].join();
    }
  }

  void testMultithreadFindAndReplaceWith10Shards() {
    // Sanity test to see if find and evictions across multiple
    // threads are safe.
    ObjectCacheConfig config;
    config.l1EntriesLimit = 100'000;
    config.l1NumShards = 10;
    auto objcache = ObjectCache<AllocatorT>::template create<Foo>(config);

    auto runReplaceOps = [&] {
      for (int i = 0; i < 2000; i++) {
        // Rotate through 5 different keys
        auto key = folly::sformat("key_{}", i % 5);
        auto foo2 = std::make_unique<Foo>();
        objcache->insertOrReplace(key, std::move(foo2));
      }
    };

    auto runFindOps = [&] {
      for (int i = 0; i < 2000; i++) {
        // Rotate through 5 different keys
        auto key = folly::sformat("key_{}", i % 5);
        auto res = objcache->template find<Foo>(key);
      }
    };

    std::vector<std::thread> rs;
    std::vector<std::thread> fs;
    for (int i = 0; i < 10; i++) {
      rs.push_back(std::thread{runReplaceOps});
      fs.push_back(std::thread{runFindOps});
    }
    for (int i = 0; i < 10; i++) {
      rs[i].join();
      fs[i].join();
    }
  }
};

using AllocatorTypes = ::testing::Types<LruAllocator,
                                        Lru2QAllocator,
                                        TinyLFUAllocator,
                                        LruAllocatorSpinBuckets>;
TYPED_TEST_CASE(ObjectCacheTest, AllocatorTypes);
TYPED_TEST(ObjectCacheTest, GetAllocSize) { this->testGetAllocSize(); }
TYPED_TEST(ObjectCacheTest, Simple) { this->testSimple(); }
TYPED_TEST(ObjectCacheTest, Expiration) { this->testExpiration(); }
TYPED_TEST(ObjectCacheTest, Replace) { this->testReplace(); }
TYPED_TEST(ObjectCacheTest, UniqueInsert) { this->testUniqueInsert(); }
TYPED_TEST(ObjectCacheTest, ObjectSizeTrackingBasics) {
  this->testObjectSizeTrackingBasics();
}
TYPED_TEST(ObjectCacheTest, ObjectSizeTrackingUniqueInsert) {
  this->testObjectSizeTrackingUniqueInsert();
}
TYPED_TEST(ObjectCacheTest, MultithreadReplace) {
  this->testMultithreadReplace();
}
TYPED_TEST(ObjectCacheTest, MultithreadEviction) {
  this->testMultithreadEviction();
}
TYPED_TEST(ObjectCacheTest, MultithreadFindAndReplace) {
  this->testMultithreadFindAndReplace();
}
TYPED_TEST(ObjectCacheTest, MultithreadFindAndEviction) {
  this->testMultithreadFindAndEviction();
}
TYPED_TEST(ObjectCacheTest, MultithreadFindAndReplaceWith10Shards) {
  this->testMultithreadFindAndReplaceWith10Shards();
}

TEST(ObjectCacheTest, LruEviction) {
  ObjectCacheConfig config;
  config.l1EntriesLimit = 1024;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

  for (int i = 0; i < 1025; i++) {
    auto foo = std::make_unique<Foo>();
    foo->a = i;
    auto key = folly::sformat("key_{}", i);
    objcache->insertOrReplace(key, std::move(foo));
    auto found = objcache->find<Foo>(key);
    ASSERT_NE(nullptr, found);
    EXPECT_EQ(i, found->a);
  }
  auto found = objcache->find<Foo>("key_0");
  EXPECT_EQ(nullptr, found);
}

TEST(ObjectCacheTest, LruEvictionWithSizeTracking) {
  ObjectCacheConfig config;
  config.l1EntriesLimit = 1024;
  config.objectSizeTrackingEnabled = true;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);
  int totalObjectSize = 0;

  // After the loop, the first item will be evicted and the total object
  // size will be the sum of remaining items.
  for (size_t i = 0; i < config.l1EntriesLimit + 1; i++) {
    auto foo = std::make_unique<Foo>();
    foo->a = i;
    auto key = folly::sformat("key_{}", i);
    auto objectSize = 8 * (i + 1);
    objcache->insertOrReplace(key, std::move(foo), objectSize);
    totalObjectSize += objectSize;
    auto found = objcache->find<Foo>(key);
    ASSERT_NE(nullptr, found);
    EXPECT_EQ(i, found->a);
    if (i < config.l1EntriesLimit) {
      ASSERT_EQ(objcache->getNumEntries(), i + 1);
      EXPECT_EQ(totalObjectSize, objcache->getTotalObjectSize());
    } else {
      EXPECT_EQ(nullptr, objcache->find<Foo>("key_0"));
      ASSERT_EQ(objcache->getNumEntries(), i);
      EXPECT_EQ(totalObjectSize - 8, objcache->getTotalObjectSize());
    }
  }
}
} // namespace test
} // namespace objcache2
} // namespace cachelib
} // namespace facebook
