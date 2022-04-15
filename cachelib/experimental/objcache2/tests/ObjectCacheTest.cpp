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

TEST(ObjectCache, Simple) {
  ObjectCacheConfig config;
  config.l1EntriesLimit = 10'000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

  auto found1 = objcache->find<Foo>("Foo");
  EXPECT_EQ(nullptr, found1);

  auto foo = std::make_unique<Foo>();
  foo->a = 1;
  foo->b = 2;
  foo->c = 3;
  auto res = objcache->insertOrReplace("Foo", std::move(foo));
  EXPECT_TRUE(res.first);
  ASSERT_NE(nullptr, res.second);
  EXPECT_EQ(1, res.second->a);
  EXPECT_EQ(2, res.second->b);
  EXPECT_EQ(3, res.second->c);

  auto found2 = objcache->find<Foo>("Foo");
  ASSERT_NE(nullptr, found2);
  EXPECT_EQ(1, found2->a);
  EXPECT_EQ(2, found2->b);
  EXPECT_EQ(3, found2->c);
}

TEST(ObjectCache, Expiration) {
  ObjectCacheConfig config;
  config.l1EntriesLimit = 10'000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

  auto foo = std::make_unique<Foo>();
  foo->a = 1;
  foo->b = 2;
  foo->c = 3;

  objcache->insertOrReplace("Foo", std::move(foo), 2 /* seconds */);

  auto found1 = objcache->find<Foo>("Foo");
  ASSERT_NE(nullptr, found1);
  EXPECT_EQ(1, found1->a);
  EXPECT_EQ(2, found1->b);
  EXPECT_EQ(3, found1->c);

  std::this_thread::sleep_for(std::chrono::seconds{3});
  auto found2 = objcache->find<Foo>("Foo");
  ASSERT_EQ(nullptr, found2);
}

TEST(ObjectCache, Replace) {
  ObjectCacheConfig config;
  config.l1EntriesLimit = 10'000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

  auto foo1 = std::make_unique<Foo>();
  foo1->a = 1;
  foo1->b = 2;
  foo1->c = 3;
  std::shared_ptr<Foo> replaced;
  auto res = objcache->insertOrReplace("Foo", std::move(foo1), 0, &replaced);
  EXPECT_TRUE(res.first);
  EXPECT_EQ(nullptr, replaced);

  auto found1 = objcache->find<Foo>("Foo");
  ASSERT_NE(nullptr, found1);
  EXPECT_EQ(1, found1->a);
  EXPECT_EQ(2, found1->b);
  EXPECT_EQ(3, found1->c);

  auto foo2 = std::make_unique<Foo>();
  foo2->a = 10;
  foo2->b = 20;
  foo2->c = 30;
  res = objcache->insertOrReplace("Foo", std::move(foo2), 0, &replaced);
  EXPECT_TRUE(res.first);
  ASSERT_NE(nullptr, replaced);
  EXPECT_EQ(1, replaced->a);
  EXPECT_EQ(2, replaced->b);
  EXPECT_EQ(3, replaced->c);

  auto found2 = objcache->find<Foo>("Foo");
  ASSERT_NE(nullptr, found1);
  EXPECT_EQ(10, found2->a);
  EXPECT_EQ(20, found2->b);
  EXPECT_EQ(30, found2->c);
}

TEST(ObjectCache, Eviction) {
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

TEST(ObjectCache, Multithread_Replace) {
  // Sanity test to see if insertOrReplace across multiple
  // threads are safe.
  ObjectCacheConfig config;
  config.l1EntriesLimit = 10'000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

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

TEST(ObjectCache, Multithread_Eviction) {
  // Sanity test to see if evictions across multiple
  // threads are safe.
  ObjectCacheConfig config;
  config.l1EntriesLimit = 1000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

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

TEST(ObjectCache, Multithread_FindAndReplace) {
  // Sanity test to see if find and insertions at the same time
  // across mutliple threads are safe.
  ObjectCacheConfig config;
  config.l1EntriesLimit = 10'000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

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
      auto res = objcache->find<Foo>(key);
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

TEST(ObjectCache, Multithread_FindAndEviction) {
  // Sanity test to see if find and evictions across multiple
  // threads are safe.
  ObjectCacheConfig config;
  config.l1EntriesLimit = 1000;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

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
      auto res = objcache->find<Foo>(key);
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

TEST(ObjectCache, Multithread_FindAndReplaceWith10Shards) {
  // Sanity test to see if find and evictions across multiple
  // threads are safe.
  ObjectCacheConfig config;
  config.l1EntriesLimit = 100'000;
  config.l1NumShards = 10;
  auto objcache = ObjectCache<LruAllocator>::create<Foo>(config);

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
      auto res = objcache->find<Foo>(key);
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
} // namespace test
} // namespace objcache2
} // namespace cachelib
} // namespace facebook
