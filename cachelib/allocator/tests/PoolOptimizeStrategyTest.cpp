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

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/MarginalHitsOptimizeStrategy.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/compact_cache/CCacheCreator.h"

namespace facebook {
namespace cachelib {

namespace tests {
template <typename AllocatorT>
class PoolOptimizeStrategyTest : public testing::Test {
 public:
  PoolId getCompactCacheId(AllocatorT& cache, std::string name) {
    return cache.compactCacheManager_->getAllocator(name).getPoolId();
  }
};

using PoolOptimizeStrategy2QTest = PoolOptimizeStrategyTest<Lru2QAllocator>;

struct CACHELIB_PACKED_ATTR LargeInt {
  int value;
  // padding to make the struct large enough s.t. only 1 bucket in 1 slab
  char unused[240 * 1024 - 4];
  /* implicit */ LargeInt(int v = 0) : value(v) {}
  bool operator==(const LargeInt& other) const { return value == other.value; }
  bool operator!=(const LargeInt& other) const { return value != other.value; }
  bool isEmpty() const { return value == 0; }
};

TEST_F(PoolOptimizeStrategy2QTest, MarginalHitsRegularPoolOptimize) {
  using MMConfig = Lru2QAllocator::MMConfig;
  const auto itemSize = 10240;
  const auto numOps = 10;
  Lru2QAllocator::Config config;

  config.setCacheSize(20 * Slab::kSize);
  config.enableTailHitsTracking();
  auto cache = std::make_unique<Lru2QAllocator>(config);
  const auto allocSize = Slab::kSize;
  const std::set<uint32_t> allocSizes{static_cast<uint32_t>(allocSize)};

  MMConfig mmConfig;
  // get rid of hot and warm queue
  mmConfig.hotSizePercent = 0;
  mmConfig.coldSizePercent = 100;

  // always promote
  mmConfig.lruRefreshTime = 0;
  auto p0 =
      cache->addPool("Pool0", cache->getCacheMemoryStats().ramCacheSize / 2,
                     allocSizes, mmConfig);
  auto p1 =
      cache->addPool("Pool1", cache->getCacheMemoryStats().ramCacheSize / 2,
                     allocSizes, mmConfig);
  ASSERT_NE(Slab::kInvalidPoolId, p0);
  ASSERT_NE(Slab::kInvalidPoolId, p1);

  // populate pools
  auto populate = [&](auto pid, auto prefix, auto& i) {
    for (i = 0; !cache->getPoolStats(pid).numEvictions(); i++) {
      auto handle = util::allocateAccessible(
          *cache, pid, prefix + std::to_string(i), itemSize);
      ASSERT_NE(nullptr, handle);
    }
  };
  uint32_t num0, num1;
  populate(p0, "key0-", num0);
  populate(p1, "key1-", num1);

  MarginalHitsOptimizeStrategy::Config strategyConfig{};
  auto strategy =
      std::make_shared<MarginalHitsOptimizeStrategy>(strategyConfig);

  // initialize pool states
  {
    auto init = strategy->pickVictimAndReceiverRegularPools(*cache);
    EXPECT_EQ(init.victimPoolId, Slab::kInvalidPoolId);
    EXPECT_EQ(init.receiverPoolId, Slab::kInvalidPoolId);
  }

  // access pool 0 at tail
  for (uint32_t i = 1; i < numOps && i < num0; i++) {
    ASSERT_NE(nullptr, cache->find("key0-" + std::to_string(i)));
  }

  // according to previous stats, move from pool 1 to pool 0
  {
    auto ctx = strategy->pickVictimAndReceiverRegularPools(*cache);
    EXPECT_EQ(p0, ctx.receiverPoolId);
    EXPECT_EQ(p1, ctx.victimPoolId);
  }

  // access pool 1 at tail
  for (uint32_t i = 1; i < numOps && i < num1; i++) {
    ASSERT_NE(nullptr, cache->find("key1-" + std::to_string(i)));
  }

  // according to previous stats, move from pool 0 to pool 1
  {
    auto ctx = strategy->pickVictimAndReceiverRegularPools(*cache);
    EXPECT_EQ(p1, ctx.receiverPoolId);
    EXPECT_EQ(p0, ctx.victimPoolId);
  }
}

TEST_F(PoolOptimizeStrategy2QTest, MarginalHitsCompactCacheOptimize) {
  const auto numItemsInBucket = 8;
  const auto numOps = 10;
  const auto poolSize = Slab::kSize;
  Lru2QAllocator::Config config;
  config.setCacheSize(10 * Slab::kSize);
  config.enableCompactCache();
  auto cache = std::make_unique<Lru2QAllocator>(config);

  using CCacheT =
      typename CCacheCreator<CCacheAllocator, LargeInt, LargeInt>::type;
  typename CCacheT::Value dummyValue(0xAA);
  auto& ccache0 = *cache->template addCompactCache<CCacheT>("C0", poolSize);
  cache->setPoolOptimizerFor(cache->getPoolId("C0"), true);
  auto& ccache1 = *cache->template addCompactCache<CCacheT>("C1", poolSize);
  cache->setPoolOptimizerFor(cache->getPoolId("C1"), true);
  auto& ccache2 = *cache->template addCompactCache<CCacheT>("C2", poolSize);
  auto p0 = this->getCompactCacheId(*cache, "C0");
  auto p1 = this->getCompactCacheId(*cache, "C1");

  ASSERT_GE(ccache0.getSize(), Slab::kSize);
  ASSERT_GE(ccache1.getSize(), Slab::kSize);
  ASSERT_GE(ccache2.getSize(), Slab::kSize);

  for (uint32_t i = 1; i <= numItemsInBucket; i++) {
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache0.set(i, &dummyValue));
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache1.set(i, &dummyValue));
    ASSERT_EQ(CCacheReturn::NOTFOUND, ccache2.set(i, &dummyValue));
  }

  MarginalHitsOptimizeStrategy::Config strategyConfig(
      /* moving average param */ 0.3,
      /* pool min size slab */ 0,
      /* pool max free memory slab */ 1);
  auto strategy =
      std::make_shared<MarginalHitsOptimizeStrategy>(strategyConfig);

  // initialize pool states
  {
    auto init = strategy->pickVictimAndReceiverCompactCaches(*cache);
    EXPECT_EQ(init.victimPoolId, Slab::kInvalidPoolId);
    EXPECT_EQ(init.receiverPoolId, Slab::kInvalidPoolId);
  }

  // access ccache 0 & ccache 2 at tail
  for (uint32_t i = 1; i <= numOps; i++) {
    ASSERT_EQ(CCacheReturn::FOUND,
              ccache0.get(1, /* value */ nullptr, /* size */ nullptr,
                          /* promotion */ false));
    ASSERT_EQ(CCacheReturn::FOUND,
              ccache2.get(1, /* value */ nullptr, /* size */ nullptr,
                          /* promotion */ false));
  }

  // according to previous stats, move from ccache 1 to ccache 0
  // (ccache 2 does not participate auto resizing)
  {
    auto ctx = strategy->pickVictimAndReceiverCompactCaches(*cache);
    EXPECT_EQ(p0, ctx.receiverPoolId);
    EXPECT_EQ(p1, ctx.victimPoolId);
  }

  // access ccache 1 & ccache 2 at tail
  for (uint32_t i = 1; i <= numOps; i++) {
    ASSERT_EQ(CCacheReturn::FOUND,
              ccache1.get(1, /* value */ nullptr, /* size */ nullptr,
                          /* promotion */ false));
    ASSERT_EQ(CCacheReturn::FOUND,
              ccache2.get(1, /* value */ nullptr, /* size */ nullptr,
                          /* promotion */ false));
  }

  // according to previous stats, move from ccache 0 to ccache 1
  // (ccache 2 does not participate auto resizing)
  {
    auto ctx = strategy->pickVictimAndReceiverCompactCaches(*cache);
    EXPECT_EQ(p1, ctx.receiverPoolId);
    EXPECT_EQ(p0, ctx.victimPoolId);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
