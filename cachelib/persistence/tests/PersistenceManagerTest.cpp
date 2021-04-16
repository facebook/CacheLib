// Copyright 2004-present Facebook. All Rights Reserved.

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/NvmTestUtils.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/compact_cache/CCacheCreator.h"
#include "cachelib/persistence/PersistenceManager.h"

namespace facebook {
namespace cachelib {

using namespace persistence;

using Cache = cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
using CacheConfig = typename Cache::Config;

namespace tests {

class PersistenceManagerTest : public ::testing::Test {
 public:
  PersistenceManagerTest()
      : buffer_(folly::IOBuf::create(kCapacity)),
        cacheDir_("/tmp/persistence_test" +
                  folly::to<std::string>(folly::Random::rand32())) {
    util::makeDir(cacheDir_);
    config_
        .setCacheSize(kCacheSize) // 100MB
        .setCacheName("test")
        .enableCachePersistence(cacheDir_)
        .usePosixForShm()
        .setAccessConfig(
            {25 /* bucket power */, 10 /* lock power */}) // assuming caching 20
                                                          // million items
        .setDefaultAllocSizes(std::set<uint32_t>{64, 128, 1024, 2048, 4096})
        // Disable slab rebalancing
        .enablePoolRebalancing(nullptr, std::chrono::seconds{0})
        .validate(); // will throw if bad config

    for (uint32_t i = 0; i < kNumKeys; ++i) {
      std::string k = folly::sformat("key_{}", i);
      std::string v;
      v.resize(folly::Random::rand32(2048));
      folly::Random::secureRandom(v.data(), v.length());
      keys_.emplace_back(k, v);
    }
  }

  ~PersistenceManagerTest() { util::removePath(cacheDir_); }

 protected:
  const uint32_t kNumKeys = 1024 * 1024;    // 1 million
  const size_t kCacheSize = 100 * kNumKeys; // 100MB
  const size_t kCapacity = 4 * kCacheSize;  // 400MB

  std::unique_ptr<folly::IOBuf> buffer_;
  std::string cacheDir_;
  CacheConfig config_;
  std::vector<std::pair<std::string, std::string>> keys_;
};

} // namespace tests
} // namespace cachelib
} // namespace facebook
