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

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/FreeMemStrategy.h"
#include "cachelib/allocator/HitsPerSlabStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/MarginalHitsStrategy.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/tests/AllocatorTestUtils.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
TEST(RebalanceStrategy, Basic) {
  PoolId pid = 1;
  RebalanceStrategy r;
  ASSERT_FALSE(r.poolStatePresent(pid));
  PoolStats stats{};
  r.initPoolState(pid, stats);
  ASSERT_TRUE(r.poolStatePresent(pid));

  std::set<ClassId> victims = {MemoryAllocator::kMaxClassId};
  r.filterVictimsByHoldOff(1, PoolStats{}, victims);
}

namespace tests {
template <typename AllocatorT>
class RebalanceStrategyTest : public testing::Test {
 public:
  enum Strategy { LruTailAge, HitsPerSlab, FreeMem, MarginalHits };

  void initAllocatorConfigForStrategy(typename AllocatorT::Config& config,
                                      Strategy s) {
    LruTailAgeStrategy::Config lruConfig;
    HitsPerSlabStrategy::Config hpsConfig;
    FreeMemStrategy::Config fmConfig;
    MarginalHitsStrategy::Config mhConfig;

    switch (s) {
    case LruTailAge:
      lruConfig.tailAgeDifferenceRatio = 0.0;
      lruConfig.minTailAgeDifference = 0;
      lruConfig.minSlabs = 0;
      config.enablePoolRebalancing(
          std::make_shared<LruTailAgeStrategy>(lruConfig),
          std::chrono::seconds{1});
      break;
    case HitsPerSlab:
      hpsConfig.minSlabs = 0;
      config.enablePoolRebalancing(
          std::make_shared<HitsPerSlabStrategy>(hpsConfig),
          std::chrono::seconds{1});
      break;
    case FreeMem:
      fmConfig.minSlabs = 0;
      config.enablePoolRebalancing(std::make_shared<FreeMemStrategy>(fmConfig),
                                   std::chrono::seconds{1});
      break;
    case MarginalHits:
      config.enableTailHitsTracking();
      config.enablePoolRebalancing(
          std::make_shared<MarginalHitsStrategy>(mhConfig),
          std::chrono::seconds{1});
      break;
    }
  }

  void doWork(typename AllocatorT::Config& config,
              bool shouldAllocate,
              uint32_t expectedFreeSlabs = 0) {
    auto cache = std::make_unique<AllocatorT>(config);
    const std::set<uint32_t> allocSizes{16 * 1024, 128 * 1024};
    const auto pid = cache->addPool(
        "default", cache->getCacheMemoryStats().ramCacheSize, allocSizes);

    std::vector<typename AllocatorT::WriteHandle> handles;
    int handleCount = 0;
    for (;; ++handleCount) {
      auto handle = util::allocateAccessible(
          *cache, pid, folly::sformat("key_{}", handleCount), 50000);
      if (!handle) {
        break;
      }
      handles.push_back(std::move(handle));
    }
    // If free slabs are expected, free up half of the handles
    if (expectedFreeSlabs > 0) {
      for (int j = 0; j < handleCount; j += 2) {
        cache->remove(handles[j]);
      }
    }
    handles.clear();

    const auto timeout = 10;
    const auto startTime = util::getCurrentTimeSec();
    bool canAllocateSmallSize = false;
    while (true) {
      auto handle = util::allocateAccessible(*cache, pid, "small size", 1);
      if (handle) {
        canAllocateSmallSize = true;
        break;
      }

      const auto currentTime = util::getCurrentTimeSec();
      if (currentTime - startTime > timeout) {
        break;
      }

      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(15));
    ASSERT_EQ(shouldAllocate, canAllocateSmallSize);
    if (expectedFreeSlabs > 0) {
      auto slabReleaseStats = cache->getSlabReleaseStats();
      auto initCount = slabReleaseStats.numSlabReleaseForRebalanceAttempts;
      do {
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        slabReleaseStats = cache->getSlabReleaseStats();
        if (slabReleaseStats.numSlabReleaseForRebalanceAttempts >
            initCount + expectedFreeSlabs + 1) {
          break;
        }
      } while (true);
    }
    auto stats = cache->getPool(pid).getStats();
    ASSERT_EQ(stats.freeSlabs, expectedFreeSlabs);
  }

  void runPoolRebalancerStatsTest() {
    typename AllocatorT::Config config;
    config.setCacheSize(10 * Slab::kSize);

    /* Rebalance from victim to receiver class */
    const ClassId victim = static_cast<ClassId>(1);
    const ClassId receiver = static_cast<ClassId>(0);
    config.enablePoolRebalancing(
        std::make_shared<AlwaysPickOneRebalanceStrategy>(victim, receiver),
        std::chrono::seconds{1});
    auto cache = std::make_unique<AllocatorT>(config);

    /* Two slab classes, 10000 bytes and 100000 bytes */
    const std::set<uint32_t> allocSizes{10000, 100000};

    /* Make a pool with a size of ~4mb per slab */
    const auto pid = cache->addPool(
        "default", cache->getCacheMemoryStats().ramCacheSize, allocSizes);

    /* Initially we should no free or allocated slabs */
    ASSERT_EQ(cache->getPoolStats(pid).mpStats.freeSlabs, 0);
    ASSERT_EQ(cache->getPoolStats(pid).mpStats.allocatedSlabs(), 0);

    /* Allocate until rebalance */
    uint i = 0;
    while (!cache->getAllSlabReleaseEvents(pid).rebalancerEvents.size()) {
      util::allocateAccessible(*cache, pid, folly::sformat("key_{}", i++),
                               50000);
    }
    /* At least one rebalance occurred */
    ASSERT_NE(i, 0);
    const auto& rebalancer_events =
        cache->getAllSlabReleaseEvents(pid).rebalancerEvents;
    ASSERT_TRUE(rebalancer_events.size() > 0);
    for (const auto& event : rebalancer_events) {
      /* Ensure all rebalances happen from the origin and destination class
       * we expected */
      ASSERT_FALSE(event.to == event.from);
      ASSERT_EQ(event.from, victim);
      ASSERT_EQ(event.to, receiver);
      ASSERT_EQ(event.pid, pid);
      ASSERT_TRUE(event.sequenceNum >= 0);
    }
  }

  void testDeltaAllocFailures() {
    // 1. Create a pool with two allocation classes
    // 2. Allocate until one is full
    // 3. Allocate from the other to trigger alloc failures
    // 4. Eventually PoolRebalancer should move a slab to the other class
    // 5. Do this for all strategies
    typename AllocatorT::Config config;
    config.setCacheSize(10 * Slab::kSize);

    initAllocatorConfigForStrategy(config, LruTailAge);
    doWork(config, true);
    initAllocatorConfigForStrategy(config, HitsPerSlab);
    doWork(config, true);
    initAllocatorConfigForStrategy(config, FreeMem);
    doWork(config, true);
  }

  void testFreeAllocSlabReleases() {
    typename AllocatorT::Config config;

    config.setCacheSize(51 * Slab::kSize);
    config.poolRebalancerFreeAllocThreshold = 20;

    initAllocatorConfigForStrategy(config, LruTailAge);
    doWork(config, true, 8);
  }

  void testDeltaAllocFailuresWithOneSlabs() {
    // 1. Create a pool with two allocation classes
    // 2. Allocate until one is full
    // 3. Allocate from the other to trigger alloc failures
    // 4. rebalancing should not move any slabs since there is just one slab
    // overall
    // 5. Do this for all strategies
    typename AllocatorT::Config config;

    config.setCacheSize(2 * Slab::kSize);
    initAllocatorConfigForStrategy(config, LruTailAge);
    doWork(config, false);
    initAllocatorConfigForStrategy(config, HitsPerSlab);
    doWork(config, false);
    initAllocatorConfigForStrategy(config, FreeMem);
    doWork(config, false);
  }

  /**
   * Helper function to run a single test scenario of rebalancing with the
   * optional weight function. The function will assert and fail the test if
   * expectingRebalance or smallAcVictim is wrong.
   *
   * Parameters
   *
   * - nBigFinds execute these many finds (hits) on big AC
   * - nSmallFinds execute these many finds (hits) on small AC
   * - expectingRebalance true if rebalancing should move slabs between ACs
   *        false if no slab movement is expected
   * - smallAcVictim true if rebalancing should take a slab from smaller AC
   *        false if rebalancing should take a slab from the large AC
   * - weightFactor 0 for disabling the use of weight function
   *        > 0 for a simple weight function based on input factor
   */
  void testHitsPerSlabWithWeights() {
    auto testFn = [](unsigned nBigFinds, unsigned nSmallFinds,
                     bool expectingRebalance, bool smallAcVictim,
                     uint32_t weightFactor) {
      typename AllocatorT::Config allocatorConfig;
      constexpr auto kCacheSlabs = 10;
      allocatorConfig.setCacheSize((kCacheSlabs + 1) * Slab::kSize);

      const ClassId smallAC = static_cast<ClassId>(0);
      const ClassId largeAC = static_cast<ClassId>(1);
      HitsPerSlabStrategy::Config weightedHitsConfig;

      const auto kFactor = weightFactor != 0 ? weightFactor : 1000;
      weightedHitsConfig.minDiff = 1;
      weightedHitsConfig.diffRatio = 0;

      if (weightFactor) {
        weightedHitsConfig.getWeight =
            [kFactor](const PoolId,
                      const ClassId classId,
                      const cachelib::PoolStats& pStats) -> double {
          auto allocSize = pStats.mpStats.acStats.at(classId).allocSize;
          return (allocSize + kFactor - 1) / kFactor;
        };

        // Asserts for testing the simple weight function
        cachelib::PoolStats pStats;
        pStats.mpStats.acStats[smallAC].allocSize = 1;
        ASSERT_TRUE(weightedHitsConfig.getWeight(0, smallAC, pStats) == 1);
        pStats.mpStats.acStats[smallAC].allocSize = kFactor;
        ASSERT_TRUE(weightedHitsConfig.getWeight(0, smallAC, pStats) == 1);
        pStats.mpStats.acStats[smallAC].allocSize = kFactor + 1;
        ASSERT_TRUE(weightedHitsConfig.getWeight(0, smallAC, pStats) == 2);
        pStats.mpStats.acStats[smallAC].allocSize = kFactor * 4;
        ASSERT_TRUE(weightedHitsConfig.getWeight(0, smallAC, pStats) == 4);
      }

      auto rebalancer =
          std::make_shared<HitsPerSlabStrategy>(weightedHitsConfig);

      allocatorConfig.enablePoolRebalancing(
          std::make_shared<HitsPerSlabStrategy>(weightedHitsConfig),
          std::chrono::seconds{1000});

      const auto kBigItemSz = kFactor * 9;
      const auto kSmallItemSz = kFactor / 2;

      auto cache = std::make_unique<AllocatorT>(allocatorConfig);
      const std::set<uint32_t> allocSizes{kSmallItemSz + 128, kBigItemSz + 128};
      const auto pid = cache->addPool(
          "default", cache->getCacheMemoryStats().ramCacheSize, allocSizes);

      /* Fill half the slabs with big items */
      const auto kTargetSlabs = kCacheSlabs / 2;
      std::vector<typename AllocatorT::WriteHandle> handlesBigItems;
      for (unsigned i = 0;
           cache->getPoolStats(pid).numSlabsForClass(largeAC) < kTargetSlabs;
           ++i) {
        auto handle = util::allocateAccessible(
            *cache, pid, folly::sformat("key_{}", i), kBigItemSz);
        if (!handle) {
          break;
        }
        handlesBigItems.push_back(std::move(handle));
      }

      /* Fill the 2nd half with small items */
      std::vector<typename AllocatorT::WriteHandle> handlesSmallItems;
      for (unsigned i = 0;
           cache->getPoolStats(pid).numSlabsForClass(smallAC) < kTargetSlabs;
           ++i) {
        auto handle = util::allocateAccessible(
            *cache, pid, folly::sformat("keySmall_{}", i), kSmallItemSz);
        if (!handle) {
          break;
        }
        handlesSmallItems.push_back(std::move(handle));
      }

      const auto filledBig = handlesBigItems.size();
      const auto filledSmall = handlesSmallItems.size();
      handlesBigItems.clear();
      handlesSmallItems.clear();

      // Run the rebalancer once to init pool stats
      auto ctx = rebalancer->pickVictimAndReceiver((CacheBase&)*cache, pid);

      unsigned id = 0;
      // Allocate a few more item from each AC to mark the AC with evictions
      // AC with no eviction will not receive a slab in rebalancing
      size_t nEvicts = cache->getPoolStats(pid).numEvictions();
      while (nEvicts == cache->getPoolStats(pid).numEvictions()) {
        auto handle = util::allocateAccessible(
            *cache, pid, folly::sformat("keyE_{}", ++id), kBigItemSz);
      }

      nEvicts = cache->getPoolStats(pid).numEvictions();
      while (nEvicts == cache->getPoolStats(pid).numEvictions()) {
        auto handle = util::allocateAccessible(
            *cache, pid, folly::sformat("keySmallE_{}", ++id), 1);
      }

      // Generate nBigFinds, nSmallFinds hits on the ACs
      for (unsigned i = 0, f = 0; f < nBigFinds; i = (i + 1) % filledBig) {
        auto handle = cache->find(folly::sformat("key_{}", i));
        if (handle) {
          f++;
        }
      }
      for (unsigned i = 0, f = 0; f < nSmallFinds; i = (i + 1) % filledSmall) {
        auto handle = cache->find(folly::sformat("keySmall_{}", i));
        if (handle) {
          f++;
        }
      }

      ctx = rebalancer->pickVictimAndReceiver((CacheBase&)*cache, pid);

      if (expectingRebalance) {
        ASSERT_TRUE(ctx.victimClassId != ctx.receiverClassId);
        ASSERT_TRUE(ctx.victimClassId != Slab::kInvalidClassId);
        ASSERT_TRUE(ctx.receiverClassId != Slab::kInvalidClassId);

        if (smallAcVictim) {
          ASSERT_EQ(ctx.victimClassId, smallAC);
          ASSERT_EQ(ctx.receiverClassId, largeAC);
        } else {
          ASSERT_EQ(ctx.victimClassId, largeAC);
          ASSERT_EQ(ctx.receiverClassId, smallAC);
        }
      } else {
        ASSERT_TRUE(ctx.victimClassId == Slab::kInvalidClassId);
        ASSERT_TRUE(ctx.receiverClassId == Slab::kInvalidClassId);
      }
    };

    ////////////////////////////////////////////////
    // Test HitsPerSlabStrategy without weights
    ////////////////////////////////////////////////

    // Equal hits no expected rebalance
    testFn(100, 100, false /* no rebalance */, true, 0);

    // Small hits rebalnce from large
    testFn(0, 1000, true /* rebalance */, false /* from big */, 0);

    // Large hits rebalance from small
    testFn(1000, 0, true /* rebalance */, true /* from small */, 0);

    ////////////////////////////////////////////////
    // Test HitsPerSlabStrategy with weights
    ////////////////////////////////////////////////

    // large hits rebalance from small
    testFn(6000, 0, true /* rebalnce */, true /* from small */, 1500 * 10);

    // Equal hits rebalance from small (large weigh more)
    testFn(1000, 1000, true /* rebalance */, true /* from small */, 1000);

    // Small hits rebalnce from large
    testFn(0, 1000, true /* rebalance */, false /* from large */, 1500 * 10);

    // 1:5 large:small hits with 1:10 weight in favor of large
    // small:
    //  hits = 159
    //  hits / projected slabs (n - 1): 159 / (5 - 1) = 39
    // large
    //  hits 20 * weight 10 = 200
    //  hits / curr slab:  200 / 5 = 40
    // large 40 hits/slab > small 39 hits/slab -> rebalnce from small to large
    testFn(20, 159, true /* rebalance */, true /* from small */, 1000);

    // With 1 more hit based on above calculation, projected h/slab of viction
    // will be 40 as well therefore no rebalancing.
    testFn(20, 160, false /* rebalance */, true /* from small */, 1000);
  }

  void testLruTailAgeWithWeights() {
    // 1. Create a pool with two allocation classes, one class with small weight
    // and another one with greater weight.
    // 2. Allocate until one is full
    // 3. Allocate from the other to trigger alloc failures
    // 4. validate that rebalance happened: allocation class with smaller weight
    // should get a slab from the other allocation class with greater weight
    typename AllocatorT::Config allocatorConfig;
    allocatorConfig.setCacheSize(10 * Slab::kSize);

    /* Weight for allocation class 0 is 0.2 */
    const ClassId receiver = static_cast<ClassId>(0);
    const ClassId victim = static_cast<ClassId>(1);

    LruTailAgeStrategy::Config weightedlruConfig;
    weightedlruConfig.getWeight = [](const PoolId,
                                     const ClassId classId,
                                     const cachelib::PoolStats&) -> double {
      return ((classId == 0) ? 0.4 : 1.0);
    };
    allocatorConfig.enablePoolRebalancing(
        std::make_shared<LruTailAgeStrategy>(weightedlruConfig),
        std::chrono::seconds{1});

    auto cache = std::make_unique<AllocatorT>(allocatorConfig);
    const std::set<uint32_t> allocSizes{10000, 100000};

    const auto pid = cache->addPool(
        "default", cache->getCacheMemoryStats().ramCacheSize, allocSizes);

    /* Attempt to fill bigger allocation class */
    std::vector<typename AllocatorT::WriteHandle> handlesBigItems;
    for (int handleCount = 0;; ++handleCount) {
      auto handle = util::allocateAccessible(
          *cache, pid, folly::sformat("key_{}", handleCount), 50000);
      if (!handle) {
        break;
      }
      handlesBigItems.push_back(std::move(handle));
    }

    /* Attempt to fill smaller allocation class */
    std::vector<typename AllocatorT::WriteHandle> handlesSmallItems;
    for (int handleCount2 = 0;; ++handleCount2) {
      auto handle2 = util::allocateAccessible(
          *cache, pid, folly::sformat("keySmall_{}", handleCount2), 1);
      if (!handle2) {
        break;
      }
      handlesSmallItems.push_back(std::move(handle2));
    }

    handlesBigItems.clear();
    handlesSmallItems.clear();

    /* Let rebalancer run for a couple seconds in the background */
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    const auto& rebalancerEvents =
        cache->getAllSlabReleaseEvents(pid).rebalancerEvents;

    ASSERT_TRUE(rebalancerEvents.size() > 0);

    for (const auto& event : rebalancerEvents) {
      /* Ensure all rebalances happen from the origin and destination class
       * we expected */
      ASSERT_NE(event.to, event.from);
      ASSERT_EQ(event.from, victim);
      ASSERT_EQ(event.to, receiver);
      ASSERT_EQ(event.pid, pid);
      ASSERT_TRUE(event.sequenceNum >= 0);
    }
  }
};

TYPED_TEST_CASE(RebalanceStrategyTest, AllocatorTypes);
TYPED_TEST(RebalanceStrategyTest, DeltaAllocFailures) {
  this->testDeltaAllocFailures();
}

TYPED_TEST(RebalanceStrategyTest, DeltaAllocFailuresNoSlabs) {
  this->testDeltaAllocFailuresWithOneSlabs();
}

TYPED_TEST(RebalanceStrategyTest, FreeAllocsPoolRebalancer) {
  this->testFreeAllocSlabReleases();
}

TYPED_TEST_CASE(RebalanceStrategyTest, AllocatorTypes);
TYPED_TEST(RebalanceStrategyTest, testPoolRebalancerStats) {
  this->runPoolRebalancerStatsTest();
}

TYPED_TEST(RebalanceStrategyTest, WeightedHitsPerSlabRebalancer) {
  this->testHitsPerSlabWithWeights();
}

TYPED_TEST(RebalanceStrategyTest, WeightedLruTailAgeRebalancer) {
  this->testLruTailAgeWithWeights();
}

using RebalanceStrategy2QTest = RebalanceStrategyTest<Lru2QAllocator>;

TEST_F(RebalanceStrategy2QTest, MarginalHitsSlabRebalance) {
  using MMConfig = Lru2QAllocator::MMConfig;
  const auto smallItemSize = Slab::kSize / 3;
  const auto largeItemSize = Slab::kSize * 2 / 3;
  const auto smallAllocSize = Slab::kSize / 2;
  const auto largeAllocSize = Slab::kSize;
  const auto numOps = 10;
  Lru2QAllocator::Config config;
  MarginalHitsStrategy::Config strategyConfig{};
  auto strategy = std::make_shared<MarginalHitsStrategy>(strategyConfig);

  // disable background pool resizer & slab rebalancer
  config.setCacheSize(20 * Slab::kSize);
  config.enableTailHitsTracking();
  auto cache = std::make_unique<Lru2QAllocator>(config);
  MMConfig mmConfig;
  const std::set<uint32_t> allocSizes{static_cast<uint32_t>(smallAllocSize),
                                      static_cast<uint32_t>(largeAllocSize)};

  // get rid of hot and warm queue
  mmConfig.hotSizePercent = 0;
  mmConfig.coldSizePercent = 100;

  // always promote
  mmConfig.lruRefreshTime = 0;

  auto pid = cache->addPool("Pool", cache->getCacheMemoryStats().ramCacheSize,
                            allocSizes, mmConfig);
  ASSERT_NE(Slab::kInvalidPoolId, pid);
  ClassId cid0{Slab::kInvalidClassId}, cid1{Slab::kInvalidClassId};
  {
    auto cacheStats = cache->getPoolStats(pid).cacheStats;
    for (auto&& it : cacheStats) {
      if (it.second.allocSize == smallAllocSize) {
        cid0 = it.first;
      }
      if (it.second.allocSize == largeAllocSize) {
        cid1 = it.first;
      }
    }
  }
  ASSERT_NE(Slab::kInvalidClassId, cid0);
  ASSERT_NE(Slab::kInvalidClassId, cid1);

  // populate classes
  uint32_t num;
  for (num = 0; !cache->getPoolStats(pid).numEvictions(); num++) {
    auto handle = util::allocateAccessible(
        *cache, pid, "large-" + std::to_string(num), largeItemSize);
    ASSERT_NE(nullptr, handle);
    handle = util::allocateAccessible(
        *cache, pid, "small-" + std::to_string(num), smallItemSize);
    ASSERT_NE(nullptr, handle);
  }
  ASSERT_GE(num, 5);

  // initialize states
  {
    auto init = strategy->pickVictimAndReceiver(*cache, pid);
    EXPECT_EQ(init.victimClassId, Slab::kInvalidClassId);
    EXPECT_EQ(init.receiverClassId, Slab::kInvalidClassId);
  }

  // access class 0 at tail
  for (uint32_t i = 1; i < numOps && i < num; i++) {
    ASSERT_NE(nullptr, cache->find("small-" + std::to_string(i)));
  }

  // according to previous stats, move from class 1 to class 0
  {
    auto ctx = strategy->pickVictimAndReceiver(*cache, pid);
    EXPECT_EQ(cid0, ctx.receiverClassId);
    EXPECT_EQ(cid1, ctx.victimClassId);
  }

  // access class 1 at tail
  for (uint32_t i = 1; i < numOps && i < num; i++) {
    ASSERT_NE(nullptr, cache->find("large-" + std::to_string(i)));
  }

  // according to previous stats, move from class 0 to class 1
  {
    auto ctx = strategy->pickVictimAndReceiver(*cache, pid);
    EXPECT_EQ(cid1, ctx.receiverClassId);
    EXPECT_EQ(cid0, ctx.victimClassId);
  }
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
