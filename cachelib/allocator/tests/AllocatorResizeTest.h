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

#pragma once

#include <folly/Random.h>

#include <algorithm>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/PoolResizeStrategy.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/TestUtils.h"
#include "cachelib/compact_cache/CCacheCreator.h"

namespace {
const std::string kShmInfoName = "cachelib_serialization";
const size_t kShmInfoSize = 10 * 1024 * 1024; // 10 MB
const uint32_t kMemoryMonitorInterval = 5;
const uint32_t kWaitForMemMonitorTime = kMemoryMonitorInterval + 2;
} // namespace

namespace facebook {
namespace cachelib {
namespace tests {

// helpful worker we can start and stop to run some allocations on the side.
class AsyncWorker : public PeriodicWorker {
 public:
  explicit AsyncWorker(std::function<void()> f) : f_(std::move(f)) {}
  ~AsyncWorker() override {
    try {
      stop();
    } catch (const std::exception&) {
    }
  }

 private:
  void work() final { f_(); }
  std::function<void()> f_;
};

// fill up the memory and test that making further allocations causes
// evictions from the cache.
template <typename AllocatorT>
class AllocatorResizeTest : public AllocatorTest<AllocatorT> {
 public:
  void testShrinkWithFreeMem() {
    // create an allocator worth 100 slabs.
    typename AllocatorT::Config config;
    const uint32_t poolResizeSlabsPerIter = 3;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);

    const unsigned int numPools = 3;
    const unsigned int numSizes = 5;
    // number of iterations for pool resizing that we intend to test
    const unsigned int expectedIters = 3;

    config.setCacheSize((numPools * numSizes * expectedIters + 1) *
                        Slab::kSize);
    AllocatorT alloc(config);

    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

    // use power 2 allocation sizes. this is important to ensure that the pool's
    // limit and allocated size match up when the pool is exhausted.
    const auto acSizes = tests::getRandomPow2AllocSizes(numSizes);

    ASSERT_EQ(0, numBytes % Slab::kSize);
    ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
    auto poolId1 = alloc.addPool("default", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId1);

    // try to allocate as much as possible in the first pool, shrink the pool
    // and ensure that the resizing does not get kicked in until we fill up all
    // of the cache.
    const unsigned int nSizes = 10;
    const unsigned int keyLen = 100;
    const auto sizes1 =
        this->getValidAllocSizes(alloc, poolId1, nSizes, keyLen);

    // at this point, the cache should be full. we dont own any references to
    // the item and we should be able to allocate by recycling.
    this->fillUpPoolUntilEvictions(alloc, poolId1, sizes1, keyLen);
    this->ensureAllocsOnlyFromEvictions(alloc, poolId1, sizes1, keyLen,
                                        numBytes * 5);

    const size_t shrinkSize =
        poolResizeSlabsPerIter * Slab::kSize * expectedIters;

    ASSERT_TRUE(alloc.shrinkPool(poolId1, shrinkSize));
    ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes - shrinkSize);

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // with lots of memory available for other pools, the resize should not
    // really happen.
    ASSERT_NE(alloc.getPool(poolId1).getCurrentAllocSize(),
              alloc.getPool(poolId1).getPoolSize());

    auto poolId2 = alloc.addPool("two", numBytes, acSizes);
    const auto sizes2 =
        this->getValidAllocSizes(alloc, poolId2, nSizes, keyLen);

    this->fillUpPoolUntilEvictions(alloc, poolId2, sizes2, keyLen);
    ASSERT_NE(Slab::kInvalidPoolId, poolId2);

    auto poolId3 = alloc.addPool("three", numBytes, acSizes);
    const auto sizes3 =
        this->getValidAllocSizes(alloc, poolId3, nSizes, keyLen);

    this->fillUpPoolUntilEvictions(alloc, poolId3, sizes3, keyLen);
    ASSERT_NE(Slab::kInvalidPoolId, poolId3);

    // should be able to create a new pool for the amount we shrunk the original
    // pool by. But until we start allocating with this pool, the resizing
    // should not kick in.
    auto poolId4 = alloc.addPool("four", shrinkSize, acSizes);
    const auto sizes4 =
        this->getValidAllocSizes(alloc, poolId2, nSizes, keyLen);

    ASSERT_NE(alloc.getPool(poolId1).getCurrentAllocSize(),
              alloc.getPool(poolId1).getPoolSize());

    // fill up the fourth pool.
    auto periodicWorkerAlloc = [&]() {
      this->fillUpPoolUntilEvictions(alloc, poolId4, sizes4, keyLen);
    };

    AsyncWorker w(periodicWorkerAlloc);
    w.start(std::chrono::seconds(1));

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds{expectedIters + 1});

    if (alloc.getPool(poolId4).getCurrentAllocSize() < shrinkSize) {
      // there could be some starvation either for pool resizer or worker,
      // so allow more time for resizing
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::seconds{5});
    }

    w.stop();

    // now the pool should have been resized
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(),
              numBytes - shrinkSize)
        << numBytes << " " << alloc.getPool(poolId1).getPoolSize();

    ASSERT_EQ(shrinkSize, alloc.getPool(poolId4).getCurrentAllocSize());
  }

  void testPoolResizerWithSlabReleaseTimeouts() {
    // Test that pool resizer correctly handles slab release timeouts
    typename AllocatorT::Config config;
    const uint32_t poolResizeSlabsPerIter = 3;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);
    config.slabRebalanceTimeout = std::chrono::milliseconds(2);

    const unsigned int numSlabs = 25;
    config.setCacheSize(numSlabs * Slab::kSize);
    AllocatorT alloc(config);

    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    const std::set<uint32_t> acSizes = {512 * 1024};
    auto poolId = alloc.addPool("test_pool", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId);
    const uint32_t itemSize = 450 * 1024;

    // pool resizer will only be triggered when all slabs are allocated, so we
    // allocate items until allocation fails. Allocation will fail because we
    // cannot evict because we hold handles to all items
    std::vector<typename AllocatorT::WriteHandle> handles;
    size_t i = 0;
    while (true) {
      std::string key = folly::sformat("key_{}", i++);
      auto handle = util::allocateAccessible(alloc, poolId, key, itemSize);
      if (!handle) {
        break;
      }
      // Hold handle so slab release will timeout
      handles.push_back(std::move(handle));
    }

    // Shrink the pool to trigger resizing (makes pool over limit)
    auto initialAbortedReleases =
        alloc.getGlobalCacheStats().numAbortedSlabReleases;
    const size_t shrinkSize = 9 * Slab::kSize;
    ASSERT_TRUE(alloc.shrinkPool(poolId, shrinkSize));

    // Wait for resizing attempts and slab release to time out
    ASSERT_EVENTUALLY_TRUE([&] {
      return alloc.getGlobalCacheStats().numAbortedSlabReleases >= 20;
    });

    EXPECT_GT(alloc.getSlabReleaseStats().numSlabReleaseForResizeAttempts, 0);
    EXPECT_EQ(alloc.getSlabReleaseStats().numSlabReleaseForResize, 0);

    size_t finalAbortedReleases =
        alloc.getGlobalCacheStats().numAbortedSlabReleases;
    EXPECT_GT(finalAbortedReleases, initialAbortedReleases)
        << "Expected some slab releases to be aborted due to timeouts";

    // releasing handles so slab release do not timeout anymore
    handles.clear();

    // After releasing handles, the pool should eventually resize
    ASSERT_EVENTUALLY_TRUE([&] {
      return alloc.getPool(poolId).getPoolSize() == numBytes - shrinkSize;
    });
    EXPECT_EQ(alloc.getPool(poolId).getPoolSize(), numBytes - shrinkSize);
  }

  void testGrowWithFreeMem() {
    // create an allocator worth 100 slabs.
    typename AllocatorT::Config config;
    const uint32_t poolResizeSlabsPerIter = 2;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);

    const unsigned int numPools = 3;
    const unsigned int numSizes = 5;
    const unsigned int expectedIters = 2;

    config.setCacheSize((numPools * numSizes * expectedIters + 1) *
                        Slab::kSize);
    AllocatorT alloc(config);

    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

    // use power 2 allocation sizes. this is important to ensure that the pool's
    // limit and allocated size match up when the pool is exhausted.
    const auto acSizes = tests::getRandomPow2AllocSizes(numSizes);

    ASSERT_EQ(0, numBytes % Slab::kSize);
    ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
    auto poolId1 = alloc.addPool("default", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId1);

    // try to allocate as much as possible in the first pool, shrink the pool
    // and ensure that the resizing does not get kicked in until we fill up all
    // of the cache.
    const unsigned int nSizes = 10;
    const unsigned int keyLen = 100;
    const auto sizes1 =
        this->getValidAllocSizes(alloc, poolId1, nSizes, keyLen);

    const auto poolId2 = alloc.addPool("two", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId2);
    const auto sizes2 =
        this->getValidAllocSizes(alloc, poolId2, nSizes, keyLen);

    // fill up the other two pools.
    this->fillUpPoolUntilEvictions(alloc, poolId2, sizes2, keyLen);

    // this pool will be empty until we complete the resize.
    const auto poolId3 = alloc.addPool("three", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId3);
    const auto sizes3 =
        this->getValidAllocSizes(alloc, poolId3, nSizes, keyLen);

    // at this point, the cache should be full. we dont own any references to
    // the item and we should be able to allocate by recycling.
    this->fillUpPoolUntilEvictions(alloc, poolId1, sizes1, keyLen);
    this->ensureAllocsOnlyFromEvictions(alloc, poolId1, sizes1, keyLen,
                                        numBytes * 5);

    const size_t growSize =
        poolResizeSlabsPerIter * Slab::kSize * expectedIters;

    ASSERT_TRUE(alloc.shrinkPool(poolId2, growSize));
    ASSERT_TRUE(alloc.growPool(poolId1, growSize));
    ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes + growSize);
    ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes - growSize);

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // with lots of memory available for other pools, the resize should not
    // really happen.
    ASSERT_EQ(numBytes, alloc.getPool(poolId2).getCurrentAllocSize());

    // since pool3 is empty, we should be able to allocate
    this->fillUpPoolUntilEvictions(alloc, poolId1, sizes1, keyLen);
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(),
              numBytes + growSize);

    // fill up the unused pool and the pool which we grew.
    auto periodicWorkerAlloc = [&]() {
      this->fillUpPoolUntilEvictions(alloc, poolId3, sizes3, keyLen);
    };

    AsyncWorker w(periodicWorkerAlloc);
    w.start(std::chrono::seconds(1));

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds{expectedIters + 1});

    w.stop();

    // now the pool should have been resized
    ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(),
              numBytes - growSize);
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(),
              numBytes + growSize);
  }

  // get a pool to have some free slabs outside of all allocation class and
  // try to resize the pool to release those slabs
  void testResizingWithFreeSlabs() {
    typename AllocatorT::Config config;
    config.setCacheSize(2 * Slab::kSize);
    const uint32_t poolResizeSlabsPerIter = 1;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);

    AllocatorT alloc(config);
    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    auto poolId = alloc.addPool("foobar", numBytes);

    // fill up the pool with items of the same size
    const int keyLen = 100;
    const int valLen = 1000;
    const std::vector<uint32_t> sizes = {keyLen + valLen + 100};

    for (int i = 0; i < 10; i++) {
      util::allocateAccessible(alloc, poolId, folly::sformat("foo{}", i),
                               valLen);
    }

    ASSERT_EQ(alloc.getPool(poolId).getStats().allocatedSlabs(), 1);
    auto classId = alloc.getAllocInfo(&*alloc.begin()).classId;

    alloc.releaseSlab(poolId, classId, SlabReleaseMode::kRebalance);
    ASSERT_EQ(alloc.getPool(poolId).getStats().freeSlabs, 1);

    // shrink pool to 0 size
    alloc.shrinkPool(poolId, numBytes);
    ASSERT_EQ(alloc.getPool(poolId).getPoolSize(), 0);

    unsigned int expectedIters = 1;
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds{expectedIters + 1});

    ASSERT_EQ(alloc.getPool(poolId).getStats().allocatedSlabs(), 0);
    ASSERT_EQ(alloc.getPool(poolId).getCurrentAllocSize(), 0);
  }

  void testBasicResize() {
    // create an allocator worth 100 slabs.
    typename AllocatorT::Config config;
    const uint32_t poolResizeSlabsPerIter = 3;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);

    const unsigned int numPools = 2;
    const unsigned int numSizes = 5;
    // number of iterations for pool resizing that we intend to test
    const unsigned int expectedIters = 3;
    config.setCacheSize((numPools * numSizes * expectedIters + 1) *
                        Slab::kSize);

    AllocatorT alloc(config);

    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

    // use power 2 allocation sizes. this is important to ensure that the pool's
    // limit and allocated size match up when the pool is exhausted.
    const auto acSizes = tests::getRandomPow2AllocSizes(numSizes);

    ASSERT_EQ(0, numBytes % Slab::kSize);
    ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
    auto poolId1 = alloc.addPool("default", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId1);

    // try to allocate as much as possible in the first pool, shrink the pool
    // and ensure that the resizing does not get kicked in until we fill up all
    // of the cache.
    const unsigned int nSizes = 10;
    const unsigned int keyLen = 100;
    const auto sizes1 =
        this->getValidAllocSizes(alloc, poolId1, nSizes, keyLen);

    // at this point, the cache should be full. we dont own any references to
    // the item and we should be able to allocate by recycling.
    this->fillUpPoolUntilEvictions(alloc, poolId1, sizes1, keyLen);
    this->ensureAllocsOnlyFromEvictions(alloc, poolId1, sizes1, keyLen,
                                        numBytes * 5);

    const size_t delta = poolResizeSlabsPerIter * Slab::kSize * expectedIters;

    auto poolId2 = alloc.addPool("two", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId2);

    const auto sizes2 =
        this->getValidAllocSizes(alloc, poolId2, nSizes, keyLen);
    this->fillUpPoolUntilEvictions(alloc, poolId2, sizes2, keyLen);

    ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(),
              alloc.getPool(poolId1).getCurrentAllocSize());
    ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(),
              alloc.getPool(poolId2).getCurrentAllocSize());

    ASSERT_TRUE(alloc.resizePools(poolId1, poolId2, delta));

    // allocate from pool2.
    auto periodicWorkerAlloc = [&]() {
      this->fillUpPoolUntilEvictions(alloc, poolId2, sizes2, keyLen);
    };

    AsyncWorker w(periodicWorkerAlloc);
    w.start(std::chrono::seconds(1));

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds{expectedIters + 1});

    w.stop();

    // now the pool should have been resized
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), numBytes - delta);
    ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), numBytes + delta);
  }

  // do some resizing and do a shutdown and ensure that the resizing continues
  // after the shutdown is done.
  void testBasicResizeWithSharedMem() {
    // create an allocator worth 100 slabs.
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    const uint32_t poolResizeSlabsPerIter = 3;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);

    const unsigned int numPools = 3;
    const unsigned int numSizes = 5;
    // number of iterations for pool resizing that we intend to test
    const unsigned int expectedIters = 3;
    const unsigned int nSizes = 10;
    const unsigned int keyLen = 100;

    // use power 2 allocation sizes. this is important to ensure that the pool's
    // limit and allocated size match up when the pool is exhausted.
    const auto acSizes = tests::getRandomPow2AllocSizes(numSizes);

    const size_t delta = poolResizeSlabsPerIter * Slab::kSize * expectedIters;

    size_t numBytes;

    PoolId poolId1, poolId2, poolId3;
    std::vector<uint32_t> sizes1, sizes2, sizes3;

    {
      config.setCacheSize((numPools * numSizes * expectedIters + 1) *
                          Slab::kSize);
      AllocatorT alloc(AllocatorT::SharedMemNew, config);

      numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
      poolId1 = alloc.addPool("default", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId1);

      // try to allocate as much as possible in the first pool, shrink the pool
      // and ensure that the resizing does not get kicked in until we fill up
      // all
      // of the cache.
      sizes1 = this->getValidAllocSizes(alloc, poolId1, nSizes, keyLen);

      // at this point, the cache should be full. we dont own any references to
      // the item and we should be able to allocate by recycling.
      this->fillUpPoolUntilEvictions(alloc, poolId1, sizes1, keyLen);
      this->ensureAllocsOnlyFromEvictions(alloc, poolId1, sizes1, keyLen,
                                          numBytes * 5);

      poolId2 = alloc.addPool("two", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId2);

      sizes2 = this->getValidAllocSizes(alloc, poolId2, nSizes, keyLen);
      this->fillUpPoolUntilEvictions(alloc, poolId2, sizes2, keyLen);

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(),
                alloc.getPool(poolId1).getCurrentAllocSize());
      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(),
                alloc.getPool(poolId2).getCurrentAllocSize());

      ASSERT_TRUE(alloc.resizePools(poolId1, poolId2, delta));

      poolId3 = alloc.addPool("three", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId3);

      ASSERT_NE(alloc.getPool(poolId1).getCurrentAllocSize(), numBytes - delta);
      ASSERT_NE(alloc.getPool(poolId2).getCurrentAllocSize(), numBytes + delta);

      sizes3 = this->getValidAllocSizes(alloc, poolId3, nSizes, keyLen);
      // shutdown the allocator.
      ASSERT_EQ(AllocatorT::ShutDownStatus::kSuccess, alloc.shutDown());
    }

    AllocatorT newAlloc(AllocatorT::SharedMemAttach, config);

    // allocate from pool2 and pool3
    auto periodicWorkerAlloc = [&]() {
      this->fillUpPoolUntilEvictions(newAlloc, poolId2, sizes2, keyLen);
      this->fillUpPoolUntilEvictions(newAlloc, poolId3, sizes3, keyLen);
    };

    AsyncWorker w(periodicWorkerAlloc);
    w.start(std::chrono::seconds(1));

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds{expectedIters + 5});

    w.stop();

    // now the pool should have been resized
    ASSERT_EQ(newAlloc.getPool(poolId1).getCurrentAllocSize(),
              numBytes - delta);
    ASSERT_EQ(newAlloc.getPool(poolId2).getCurrentAllocSize(),
              numBytes + delta);
  }

  // testResizeMemMonitor Description:
  //    Create two pools of 50 slabs each
  //    Fill up 10 slabs in each pool
  //    Advise Away 4 slabs - 2 slabs in each pool
  //    Verify inUse and advisedAway slabs
  //    Shrink pool1 to 25 slabs and grow pool2 to 75 slabs
  //    Fill up all the slabs in each pool
  //    Verify inUse and advisedAway slabs
  //    Wait for memory monitor to run and verify inUse and advisedAway slabs
  //
  void testResizeMemMonitor() {
    // create an allocator worth 100 slabs.
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    const uint32_t poolResizeSlabsPerIter = 20;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(kMemoryMonitorInterval);

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    const unsigned int numPools = 2;
    const unsigned int keyLen = 25;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    size_t numBytes;

    PoolId poolId1, poolId2;
    {
      config.setCacheSize((numPools * 50 + 1) * Slab::kSize);
      AllocatorT alloc(AllocatorT::SharedMemNew, config);
      numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
      poolId1 = alloc.addPool("pool1", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId1);
      poolId2 = alloc.addPool("pool2", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId2);
      auto allocSizes = alloc.getPool(poolId1).getAllocSizes();
      std::vector<uint32_t> sizes = {450 * 1024, 900 * 1024};
      // Allocate to use up 10 slabs in pool
      for (int i = 0; i < 5; i++) {
        this->fillUpOneSlab(alloc, poolId1, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId1, allocSizes[1], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[1], keyLen);
      }
      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 10 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 10 * Slab::kSize);

      auto& memMonitor = *alloc.memMonitor_;
      memMonitor.updateNumSlabsToAdvise(4);
      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 8 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 8 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 2);

      auto delta = 25 * Slab::kSize;
      ASSERT_TRUE(alloc.resizePools(poolId1, poolId2, delta));
      this->fillUpPoolUntilEvictions(alloc, poolId1, sizes, keyLen);
      this->fillUpPoolUntilEvictions(alloc, poolId2, sizes, keyLen);

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes / 2);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 23 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), 3 * numBytes / 2);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 73 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 2);

      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes / 2);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 23 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), 3 * numBytes / 2);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 73 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 2);
    }
  }

  // testMemMonitorNoResize Description:
  //    Create two pools of 50 slabs each
  //    Fill up 30 slabs in each pool
  //    Advise away 10 slabs in each pool
  //    Wait for memory monitor to run
  //    Verify inUse and advisedSlabs in each pool
  //    Fill up all the slabs in each pool
  //    Verify inUse and advisedSlabs in each pool again because they should
  //    change because usage changed.
  //
  void testMemMonitorNoResize() {
    // create an allocator worth 100 slabs.
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(kMemoryMonitorInterval);

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    const unsigned int numPools = 2;
    const unsigned int keyLen = 25;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    size_t numBytes;
    config.setCacheSize((numPools * 40 + 1) * Slab::kSize);

    PoolId poolId1, poolId2;
    {
      AllocatorT alloc(AllocatorT::SharedMemNew, config);
      numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
      poolId1 = alloc.addPool("pool1", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId1);
      poolId2 = alloc.addPool("pool2", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId2);
      auto allocSizes = alloc.getPool(poolId1).getAllocSizes();
      std::vector<uint32_t> sizes = {450 * 1024, 900 * 1024};
      // Allocate to use up 24 slabs in each pool
      for (int i = 0; i < 12; i++) {
        this->fillUpOneSlab(alloc, poolId1, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId1, allocSizes[1], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[1], keyLen);
      }
      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 24 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 24 * Slab::kSize);

      auto& memMonitor = *alloc.memMonitor_;
      memMonitor.updateNumSlabsToAdvise(24);
      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 12 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 12);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 12 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 12);

      for (int i = 0; i < 6; i++) {
        this->fillUpOneSlab(alloc, poolId2, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[1], keyLen);
      }

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 12 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 12);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 24 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 12);

      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 12 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 12);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 24 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 12);
      // shutdown and make sure that the advised slabs in
      // each pool remain same after restore
      alloc.shutDown();
    }

    {
      // without the fix for T46009966, these values were zero, even though
      // individual pool level num advised slabs were saved and restored
      // because these values were recalculated to be zero because
      // MemoryPoolManager::numSlabsToAdvise_ was not getting save/restored
      //
      AllocatorT alloc(AllocatorT::SharedMemAttach, config);
      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 12 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 12);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 24 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 12);
    }
  }

  // testMemMonitorCompactCache Description:
  //    Create three pools of 5, 5 and 10 slabs each
  //    Fill up the pools completely
  //    Advise away 2, 2 and 4 slabs from the pools respectively
  //    Wait for memory monitor to run
  //    Verify inUse and adviseAway slabs in each pool
  //    Shrink pool 2 to 0 size and create a new compact cache pool of same size
  //    wait for memory monitor to run again
  //    Verify inUse and adviseAway slabs because it would have changed because
  //    of shrinking of pool 2 and compact cache slabs should not be advised.
  void testMemMonitorCompactCache() {
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    config.enableCompactCache();
    const uint32_t poolResizeSlabsPerIter = 20;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(kMemoryMonitorInterval);

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    // const unsigned int numPools = 3;
    const unsigned int keyLen = 25;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    size_t numBytes;

    PoolId poolId1, poolId2, poolId3;
    {
      config.setCacheSize((4 * 5 + 1) * Slab::kSize);
      AllocatorT alloc(AllocatorT::SharedMemNew, config);
      numBytes = alloc.getCacheMemoryStats().ramCacheSize / 4;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * 4, alloc.getCacheMemoryStats().ramCacheSize);
      poolId1 = alloc.addPool("pool1", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId1);
      poolId2 = alloc.addPool("pool2", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId2);
      poolId3 = alloc.addPool("pool3", 2 * numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId3);
      std::vector<uint32_t> sizes = {450 * 1024, 900 * 1024};
      this->fillUpPoolUntilEvictions(alloc, poolId1, sizes, keyLen);
      this->fillUpPoolUntilEvictions(alloc, poolId2, sizes, keyLen);
      this->fillUpPoolUntilEvictions(alloc, poolId3, sizes, keyLen);

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 5 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 5 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), 2 * numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 10 * Slab::kSize);

      auto& memMonitor = *alloc.memMonitor_;
      memMonitor.updateNumSlabsToAdvise(8);
      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 3 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 3 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), 2 * numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 6 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getNumSlabsAdvised(), 4);

      alloc.shrinkPool(poolId2, numBytes);
      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      struct Key {
        int id;
        // need these two functions for key comparison
        bool operator==(const Key& other) const { return id == other.id; }
        bool isEmpty() const { return id == 0; }
        Key(int i) : id(i) {}
      } __attribute__((packed));
      using IntValueCCache =
          typename CCacheCreator<CCacheAllocator, Key, int>::type;

      const auto& cc =
          alloc.template addCompactCache<IntValueCCache>("ccpool", numBytes);
      int p = 1;
      ASSERT_EQ(CCacheReturn::NOTFOUND, cc->set(Key(p), &p));

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 3 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), 0);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 0 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), 2 * numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 6 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getNumSlabsAdvised(), 4);
      alloc.shutDown();
    }
    {
      AllocatorT alloc(AllocatorT::SharedMemAttach, config);
      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 3 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), 0);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 0 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 2);

      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), 2 * numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 6 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getNumSlabsAdvised(), 4);
    }
  }

  // testMemMonitorEmptySlabs Description:
  //    Create 3 pools of 50 slabs each
  //    Fill up pools 1 and 2 with 20 slabs each and pool 3 40 slabs
  //    Advise away 3, 3 and 6 slabs from each of the pools
  //    verify inUse and adviseAway slabs
  //    Fill up all the pools
  //    verify that inUse and adviseAway changes to equal values
  //
  //
  void testMemMonitorEmptySlabs() {
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    const uint32_t poolResizeSlabsPerIter = 20;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(kMemoryMonitorInterval);

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    const unsigned int numPools = 3;
    const unsigned int keyLen = 25;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    size_t numBytes;

    PoolId poolId1, poolId2, poolId3;
    {
      config.setCacheSize((numPools * 50 + 1) * Slab::kSize);
      AllocatorT alloc(AllocatorT::SharedMemNew, config);
      numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
      poolId1 = alloc.addPool("pool1", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId1);
      poolId2 = alloc.addPool("pool2", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId2);
      poolId3 = alloc.addPool("pool3", numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId3);
      auto allocSizes = alloc.getPool(poolId1).getAllocSizes();
      std::vector<uint32_t> sizes = {450 * 1024, 900 * 1024};
      // Allocate to use up 10 slabs in pool
      for (int i = 0; i < 10; i++) {
        this->fillUpOneSlab(alloc, poolId1, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId1, allocSizes[1], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId2, allocSizes[1], keyLen);
      }
      for (int i = 0; i < 20; i++) {
        this->fillUpOneSlab(alloc, poolId3, allocSizes[0], keyLen);
        this->fillUpOneSlab(alloc, poolId3, allocSizes[1], keyLen);
      }
      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 20 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 20 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 40 * Slab::kSize);

      auto& memMonitor = *alloc.memMonitor_;
      memMonitor.updateNumSlabsToAdvise(12);
      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 17 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 3);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 17 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 3);

      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 34 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getNumSlabsAdvised(), 6);

      // fill up and wait for memory monitor to run few times.
      for (int i = 0; i < 3; i++) {
        this->fillUpPoolUntilEvictions(alloc, poolId1, sizes, keyLen);
        this->fillUpPoolUntilEvictions(alloc, poolId2, sizes, keyLen);
        this->fillUpPoolUntilEvictions(alloc, poolId3, sizes, keyLen);

        std::this_thread::sleep_for(
            std::chrono::seconds{kWaitForMemMonitorTime});
      }

      ASSERT_EQ(alloc.getPool(poolId1).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 47 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 3);

      ASSERT_EQ(alloc.getPool(poolId2).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(), 47 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 3);

      ASSERT_EQ(alloc.getPool(poolId3).getPoolSize(), numBytes);
      ASSERT_EQ(alloc.getPool(poolId3).getCurrentAllocSize(), 44 * Slab::kSize);
      ASSERT_EQ(alloc.getPool(poolId3).getNumSlabsAdvised(), 6);
    }
  }

  // Verify that per-pool advised slab counts always reflect reality.
  // After advising slabs across pools of different sizes and waiting for
  // a no-op iteration (numSlabsToAdvise == totalSlabsAdvised), each pool's
  // getNumSlabsAdvised() must match the actual number of madvised slabs
  // and must not exceed the pool's total slab count.
  //
  // Regression test for a bug where calcNumSlabsToAdviseReclaim would
  // overwrite per-pool curSlabsAdvised_ counters with computed proportional
  // targets when numSlabsToAdvise == totalSlabsAdvised, even though no
  // actual advise or reclaim operations were performed. With asymmetric
  // pool sizes and >50% of slabs advised, the computed target for a small
  // pool can exceed its total slab count, causing getPoolUsableSize() to
  // underflow and overLimit() to return a false positive.
  void testMemMonitorAdvisedCountsMatchReality() {
    typename AllocatorT::Config config;
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(kMemoryMonitorInterval);

    // Disable slab rebalancing and pool resizing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    const unsigned int keyLen = 25;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    const size_t pool1Slabs = 4;
    const size_t pool2Slabs = 50;
    config.setCacheSize((pool1Slabs + pool2Slabs + 1) * Slab::kSize);

    AllocatorT alloc(config);
    auto poolId1 = alloc.addPool("pool1", pool1Slabs * Slab::kSize, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId1);
    auto poolId2 = alloc.addPool("pool2", pool2Slabs * Slab::kSize, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId2);
    auto allocSizes = alloc.getPool(poolId1).getAllocSizes();

    // Fill pool1 completely (4 slabs: 2 per alloc class)
    for (size_t i = 0; i < pool1Slabs / 2; i++) {
      this->fillUpOneSlab(alloc, poolId1, allocSizes[0], keyLen);
      this->fillUpOneSlab(alloc, poolId1, allocSizes[1], keyLen);
    }
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(),
              pool1Slabs * Slab::kSize);

    // Fill pool2 completely (50 slabs: 25 per alloc class)
    for (size_t i = 0; i < pool2Slabs / 2; i++) {
      this->fillUpOneSlab(alloc, poolId2, allocSizes[0], keyLen);
      this->fillUpOneSlab(alloc, poolId2, allocSizes[1], keyLen);
    }
    ASSERT_EQ(alloc.getPool(poolId2).getCurrentAllocSize(),
              pool2Slabs * Slab::kSize);

    auto& memMonitor = *alloc.memMonitor_;

    // Step 1: Advise 10 slabs. Proportional targets:
    //   pool1: 4*10/54 = 0, +1 correction = 1
    //   pool2: 50*10/54 = 9
    // After: pool1(3 alloc, 1 advised), pool2(41 alloc, 9 advised)
    memMonitor.updateNumSlabsToAdvise(10);
    ASSERT_EVENTUALLY_TRUE([&] {
      return alloc.getPool(poolId1).getNumSlabsAdvised() +
                 alloc.getPool(poolId2).getNumSlabsAdvised() ==
             10;
    });

    ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 1);
    ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 9);
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 3 * Slab::kSize);

    // Step 2: Advise 44 total (+34 delta). Used: pool1=3, pool2=41, total=44.
    //   Targets: pool1 = 3*44/44 = 3, pool2 = 41*44/44 = 41.
    //   Advise 2 more from pool1 (1→3), 32 more from pool2 (9→41).
    //   After: pool1(1 alloc, 3 advised), pool2(9 alloc, 41 advised)
    memMonitor.updateNumSlabsToAdvise(34);
    ASSERT_EVENTUALLY_TRUE([&] {
      return alloc.getPool(poolId1).getNumSlabsAdvised() +
                 alloc.getPool(poolId2).getNumSlabsAdvised() ==
             44;
    });

    ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 3);
    ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 41);
    ASSERT_EQ(alloc.getPool(poolId1).getCurrentAllocSize(), 1 * Slab::kSize);

    // Step 3: Let at least one more memory monitor iteration run.
    // numSlabsToAdvise (44) == totalSlabsAdvised (44), so no advise or
    // reclaim should occur. Advised counts must remain unchanged.
    // With asymmetric pool sizes and >50% of slabs advised, proportional
    // target computation can produce values exceeding a pool's slab count
    // (e.g. pool1 target = 1*44/10 = 4, +1 correction = 5 > 4 slabs).
    // The counters must never be overwritten with such computed targets.
    auto runCountBefore = memMonitor.getRunCount();
    ASSERT_EVENTUALLY_TRUE(
        [&] { return memMonitor.getRunCount() >= runCountBefore + 2; });

    // Advised counts must not exceed the pool's total slab count
    ASSERT_LE(alloc.getPool(poolId1).getNumSlabsAdvised(), pool1Slabs);
    ASSERT_LE(alloc.getPool(poolId2).getNumSlabsAdvised(), pool2Slabs);

    // Advised counts must reflect reality (actual madvised slabs)
    ASSERT_EQ(alloc.getPool(poolId1).getNumSlabsAdvised(), 3);
    ASSERT_EQ(alloc.getPool(poolId2).getNumSlabsAdvised(), 41);

    // Pool1 must not be over limit
    ASSERT_FALSE(alloc.getPool(poolId1).overLimit());
    ASSERT_FALSE(alloc.getPool(poolId2).overLimit());
  }

  // testMemoryMonitorPerIterationAdviseReclaim
  // Create 5 pools of 50 slabs each
  // fillup all pools
  // since lower limit is set to 1GB and upper limit set to 2GB, and
  // advise reclaim percent per iter value of 2, per iteration, 5 slabs
  // will be advised or reclaimed. And since maxadvise percent is 20,
  // after 11 iterations, advising away should stop.
  //
  // Then reclaim the advised away slabs.
  //
  void testMemoryMonitorPerIterationAdviseReclaim() {
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(1);
    config.memMonitorConfig.lowerLimitGB = 1;
    config.memMonitorConfig.upperLimitGB = 2;
    config.memMonitorConfig.maxAdvisePercentPerIter = 2;
    config.memMonitorConfig.maxReclaimPercentPerIter = 2;
    config.memMonitorConfig.maxAdvisePercent = 20;

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    const unsigned int numPools = 5;
    const unsigned int keyLen = 25;
    const unsigned int slabsPerPool = 50;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    size_t numBytes;

    std::vector<PoolId> poolIds;

    {
      config.setCacheSize((numPools * slabsPerPool + 1) * Slab::kSize);
      AllocatorT alloc(AllocatorT::SharedMemNew, config);
      numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
      for (int i = 0; i < 5; i++) {
        std::string s = "pool" + std::to_string(i);
        auto poolId = alloc.addPool(folly::StringPiece(s.data(), s.size()),
                                    numBytes, acSizes);
        ASSERT_NE(Slab::kInvalidPoolId, poolId);
        poolIds.push_back(poolId);
      }
      auto allocSizes = alloc.getPool(poolIds[0]).getAllocSizes();
      std::vector<uint32_t> sizes = {450 * 1024, 900 * 1024};

      for (int i = 0; i < 5; i++) {
        this->fillUpPoolUntilEvictions(alloc, poolIds[i], sizes, keyLen);
      }

      for (int i = 0; i < 5; i++) {
        ASSERT_EQ(alloc.getPool(poolIds[i]).getPoolSize(), numBytes);
        ASSERT_EQ(alloc.getPool(poolIds[i]).getCurrentAllocSize(),
                  slabsPerPool * Slab::kSize);
      }

      // advise away to the max limit
      uint64_t perIterAdvSize = 5 * Slab::kSize;
      auto bytesToSlabs = [](size_t bytes) { return bytes / Slab::kSize; };
      const uint32_t slabsToAdvisePerIter =
          (bytesToSlabs((config.memMonitorConfig.upperLimitGB -
                         config.memMonitorConfig.lowerLimitGB)
                        << 30) *
           config.memMonitorConfig.maxAdvisePercentPerIter) /
          100;
      const uint32_t numItersToMaxAdviseAway =
          numPools * slabsPerPool * config.memMonitorConfig.maxAdvisePercent /
          (100 * slabsToAdvisePerIter);

      unsigned int i;
      /* iterate for numItersToMaxAdviseAway times */
      for (i = 1; i <= numItersToMaxAdviseAway; i++) {
        alloc.memMonitor_->adviseAwaySlabs();
        std::this_thread::sleep_for(std::chrono::seconds{2});
        ASSERT_EQ(alloc.allocator_->getAdvisedMemorySize(), i * perIterAdvSize);
      }
      i--;
      // This should fail
      alloc.memMonitor_->adviseAwaySlabs();
      std::this_thread::sleep_for(std::chrono::seconds{2});
      auto totalAdvisedAwayMemory = alloc.allocator_->getAdvisedMemorySize();
      ASSERT_EQ(totalAdvisedAwayMemory, i * perIterAdvSize);

      // Try to reclaim back
      for (i = 1; i <= numItersToMaxAdviseAway; i++) {
        alloc.memMonitor_->reclaimSlabs();
        std::this_thread::sleep_for(std::chrono::seconds{2});
        ASSERT_EQ(alloc.allocator_->getAdvisedMemorySize(),
                  totalAdvisedAwayMemory - i * perIterAdvSize);
      }
      totalAdvisedAwayMemory = alloc.allocator_->getAdvisedMemorySize();
      ASSERT_EQ(totalAdvisedAwayMemory, 0);
    }
  }

  // testMemoryAdviseWithSaveRestore
  // Crate 5 pools of 20 slabs each
  // fillup all pools
  // Advise away half of slabs from each pool
  // Save and Restore and check to make sure advised away slabs are restored
  //      correctly.
  //
  // Corrupt the pool level curSlabsAdvised
  //
  // Save and Restore and check to make sure advised away slabs are restored
  //      correctly even when the pool level advised away stat was corrupted.
  //
  void testMemoryAdviseWithSaveRestore() {
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(1);
    config.memMonitorConfig.lowerLimitGB = 1;
    config.memMonitorConfig.upperLimitGB = 2;
    config.memMonitorConfig.maxAdvisePercentPerIter = 2;
    config.memMonitorConfig.maxReclaimPercentPerIter = 2;
    config.memMonitorConfig.maxAdvisePercent = 20;

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    const int numPools = 5;
    const unsigned int keyLen = 25;
    const unsigned int slabsPerPool = 20;
    const std::set<uint32_t> acSizes = {512 * 1024, 1024 * 1024};
    size_t numBytes;

    std::vector<PoolId> poolIds;

    {
      config.setCacheSize((numPools * slabsPerPool + 1) * Slab::kSize);
      AllocatorT alloc(AllocatorT::SharedMemNew, config);
      numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

      ASSERT_EQ(0, numBytes % Slab::kSize);
      ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
      for (int i = 0; i < numPools; i++) {
        std::string s = "pool" + std::to_string(i);
        auto poolId = alloc.addPool(folly::StringPiece(s.data(), s.size()),
                                    numBytes, acSizes);
        ASSERT_NE(Slab::kInvalidPoolId, poolId);
        poolIds.push_back(poolId);
      }
      auto allocSizes = alloc.getPool(poolIds[0]).getAllocSizes();
      std::vector<uint32_t> sizes = {450 * 1024, 900 * 1024};

      for (int i = 0; i < numPools; i++) {
        this->fillUpPoolUntilEvictions(alloc, poolIds[i], sizes, keyLen);
      }

      for (int i = 0; i < numPools; i++) {
        ASSERT_EQ(alloc.getPool(poolIds[i]).getPoolSize(), numBytes);
        ASSERT_EQ(alloc.getPool(poolIds[i]).getCurrentAllocSize(),
                  slabsPerPool * Slab::kSize);
      }
      auto& memMonitor = *alloc.memMonitor_;
      memMonitor.updateNumSlabsToAdvise(numPools * slabsPerPool / 2);
      std::this_thread::sleep_for(std::chrono::seconds{kWaitForMemMonitorTime});

      for (int i = 0; i < numPools; i++) {
        ASSERT_EQ(alloc.getPool(poolIds[i]).getPoolSize(), numBytes);
        ASSERT_EQ(alloc.getPool(poolIds[i]).getCurrentAllocSize(),
                  (slabsPerPool / 2) * Slab::kSize);
        ASSERT_EQ(alloc.getPool(poolIds[i]).getNumSlabsAdvised(),
                  slabsPerPool / 2);
      }
      alloc.shutDown();
    }
    {
      AllocatorT alloc(AllocatorT::SharedMemAttach, config);
      for (int i = 0; i < numPools; i++) {
        ASSERT_EQ(alloc.getPool(poolIds[i]).getPoolSize(), numBytes);
        ASSERT_EQ(alloc.getPool(poolIds[i]).getCurrentAllocSize(),
                  (slabsPerPool / 2) * Slab::kSize);
        ASSERT_EQ(alloc.getPool(poolIds[i]).getNumSlabsAdvised(),
                  slabsPerPool / 2);
      }
    }
  }

  void testShrinkGrowthAdviseRaceCondition() {
    typename AllocatorT::Config config;
    config.enableCachePersistence(this->cacheDir_);
    const uint32_t poolResizeSlabsPerIter = 3;
    config.enablePoolResizing(std::make_shared<RebalanceStrategy>(),
                              std::chrono::seconds{1}, poolResizeSlabsPerIter);

    const unsigned int numPools = 10;
    const unsigned int numSizes = 5;
    const unsigned int numResizeThreads = 10;
    // number of iterations for pool resizing that we intend to test
    const unsigned int expectedIters = 3;

    config.setCacheSize((numPools * numSizes * expectedIters + 1) *
                        Slab::kSize);
    AllocatorT alloc(AllocatorT::SharedMemNew, config);

    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize / numPools;

    const std::set<uint32_t> acSizes{16384, 32768, 65536, 131072, 262144};

    ASSERT_EQ(0, numBytes % Slab::kSize);
    ASSERT_EQ(numBytes * numPools, alloc.getCacheMemoryStats().ramCacheSize);
    std::vector<PoolId> poolIds;
    const unsigned int keyLen = 10;
    for (unsigned int i = 0; i < numPools; i++) {
      std::string poolName = "mypool" + std::to_string(i + 1);
      auto poolId = alloc.addPool(poolName, numBytes, acSizes);
      ASSERT_NE(Slab::kInvalidPoolId, poolId);
      poolIds.push_back(poolId);
      for (auto& sz : acSizes) {
        size_t allocBytes = 0;
        for (size_t k = 0; k < expectedIters * Slab::kSize / sz; k++) {
          const auto key = this->getRandomNewKey(alloc, keyLen);
          auto handle = util::allocateAccessible(alloc, poolId, key, sz - 45);
          if (!handle.get()) {
            break;
          }
          allocBytes += handle->getSize();
        }
      }

      for (auto& sz : acSizes) {
        for (size_t k = 0; k < expectedIters * Slab::kSize / sz; k++) {
          const auto key = this->getRandomNewKey(alloc, keyLen);
          size_t allocBytes = 0;
          auto handle = util::allocateAccessible(alloc, poolId, key, sz - 45);
          allocBytes += handle->getSize();
        }
      }
      auto ps = alloc.getPoolStats(poolId);
    }

    auto thread_func = [&alloc, &poolIds](unsigned int index) {
      auto poolId = poolIds[index];
      int nIter = 0;
      const int maxIter = 10000;
      while (nIter++ < maxIter) {
        const size_t shrinkSize = Slab::kSize;

        alloc.shrinkPool(poolId, shrinkSize);
        alloc.growPool(poolId, shrinkSize);
      }
    };
    auto slab_rel_thread_func = [&alloc]() {
      PoolId poolId = 0;
      int nIter = 0;
      const int maxIter = 10000;
      while (nIter++ < maxIter) {
        auto stats = alloc.getPool(poolId).getStats();
        auto strategy = std::make_shared<PoolResizeStrategy>();
        auto slabsInUseByPool =
            alloc.getPool(poolId).getCurrentUsedSize() / Slab::kSize;
        if (slabsInUseByPool > 0) {
          for (unsigned int classId = 0; classId < numSizes; classId++) {
            try {
              alloc.releaseSlab(poolId, static_cast<ClassId>(classId),
                                SlabReleaseMode::kAdvise);
            } catch (const std::invalid_argument&) {
            }
          }
        }
        if (++poolId == numPools) {
          poolId = 0;
        }
      }
    };
    std::vector<std::thread> threads;
    for (unsigned int i = 0; i < numResizeThreads; i++) {
      threads.push_back(std::thread{thread_func, i});
    }
    threads.push_back(std::thread{slab_rel_thread_func});
    for (unsigned int i = 0; i <= numResizeThreads; i++) {
      threads[i].join();
    }
  }

  struct MemMonitorTestParams {
    unsigned int totalSlabs;
    unsigned int slabsToFill;
    unsigned int maxAdvisePercent;
    unsigned int expectedAdvisedAfterFirstIteration;
    unsigned int expectedAdvisedAfterSecondIteration;
  };

  void setupMemMonitorTest(typename AllocatorT::Config& config) {
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    config.memMonitorInterval = std::chrono::seconds(kMemoryMonitorInterval);
    config.memMonitorConfig.maxAdvisePercentPerIter = 4;
    config.memMonitorConfig.upperLimitGB = 2;
    config.memMonitorConfig.lowerLimitGB = 1;
    // With these settings, memory monitor will advise away 4% * (2GB-1GB) =
    // 40MB = 10 slabs per iteration
  }

  void runMemMonitorTest(const MemMonitorTestParams& params) {
    typename AllocatorT::Config config;
    setupMemMonitorTest(config);
    config.memMonitorConfig.maxAdvisePercent = params.maxAdvisePercent;
    config.setCacheSize(params.totalSlabs * Slab::kSize);

    AllocatorT alloc(config);

    const auto numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    const std::set<uint32_t> acSizes = {512 * 1024};
    auto poolId = alloc.addPool("test_pool", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId);

    const auto allocSizes = alloc.getPool(poolId).getAllocSizes();
    const size_t itemsPerSlab = Slab::kSize / allocSizes[0];
    const size_t numItems = itemsPerSlab * params.slabsToFill;
    const uint32_t itemSize = 450 * 1024;

    std::vector<typename AllocatorT::WriteHandle> handles;
    for (size_t i = 0; i < numItems; ++i) {
      auto handle = util::allocateAccessible(
          alloc, poolId, folly::sformat("key_{}", i), itemSize);
      ASSERT_NE(nullptr, handle);
      handles.push_back(std::move(handle));
    }
    handles.clear();

    ASSERT_EQ(alloc.getPoolStats(poolId).mpStats.allocatedSlabs(),
              params.slabsToFill);
    ASSERT_EQ(alloc.getPool(poolId).getNumSlabsAdvised(), 0);

    alloc.memMonitor_->adviseAwaySlabs();
    alloc.memMonitor_->checkPoolsAndAdviseReclaim();
    ASSERT_EQ(alloc.getPool(poolId).getNumSlabsAdvised(),
              params.expectedAdvisedAfterFirstIteration);

    alloc.memMonitor_->adviseAwaySlabs();
    alloc.memMonitor_->checkPoolsAndAdviseReclaim();
    ASSERT_EQ(alloc.getPool(poolId).getNumSlabsAdvised(),
              params.expectedAdvisedAfterSecondIteration);
  }

  void testMemMonitorAdvisesAwayOverLimit() {
    // Test advising with maxAdvisePercent=51%
    // Cache: 23 total slabs (1 for metadata, 22 usable)
    // First iteration: advises 10 slabs (10/22 = 45%)
    // Second iteration: advises 1 more slab (11/22 = 50% < 51%)
    runMemMonitorTest({
        .totalSlabs = 23,
        .slabsToFill = 22,
        .maxAdvisePercent = 51,
        .expectedAdvisedAfterFirstIteration = 10,
        .expectedAdvisedAfterSecondIteration = 11,
    });
  }

  void testMemMonitorAdvisesAwayOverLimit2() {
    // Test advising exactly at maxAdvisePercent boundary (50%)
    // Cache: 21 total slabs (1 for metadata, 20 usable)
    // First iteration: advises 10 slabs (10/20 = 50%)
    // Second iteration: cannot advise more (would exceed 50% limit)
    runMemMonitorTest({
        .totalSlabs = 21,
        .slabsToFill = 20,
        .maxAdvisePercent = 50,
        .expectedAdvisedAfterFirstIteration = 10,
        .expectedAdvisedAfterSecondIteration = 10,
    });
  }

  void testMemMonitorAdvisesAwayOverLimit3() {
    // Test advising with maxAdvisePercent=51%
    // Cache: 21 total slabs (1 for metadata, 20 usable)
    // First iteration: advises 10 slabs (10/20 = 50%)
    // Second iteration: cannot advise more because one more slab would exceed
    // 51%
    runMemMonitorTest({
        .totalSlabs = 21,
        .slabsToFill = 20,
        .maxAdvisePercent = 51,
        .expectedAdvisedAfterFirstIteration = 10,
        .expectedAdvisedAfterSecondIteration = 10,
    });
  }

  void runMemMonitorAdvisesAwaySlabsTest(int numIterations,
                                         int sleepSeconds,
                                         int expectedAdvisedAfterThread,
                                         int expectedAdvisedAfterOneMore) {
    typename AllocatorT::Config config;
    config.memMonitorConfig.mode = MemoryMonitor::TestMode;
    // Setting interval to 999 because we want to call memory monitor manually
    config.memMonitorInterval = std::chrono::seconds(999);
    config.memMonitorConfig.maxAdvisePercentPerIter = 2;
    config.memMonitorConfig.maxReclaimPercentPerIter = 10;
    config.memMonitorConfig.maxAdvisePercent = 50;
    config.memMonitorConfig.upperLimitGB = 2;
    config.memMonitorConfig.lowerLimitGB = 1;
    // With these settings, we advise away (2-1) * 0.02 = 20MB which is equal to
    // 5 slabs per iteration

    // Slab release timeout triggered this bug
    config.slabRebalanceTimeout = std::chrono::milliseconds(50);

    // Disable pool rebalancing to focus on memory monitor behavior
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    // Create cache with 20 slabs, 1 is used for metadata
    const unsigned int numSlabs = 21;
    config.setCacheSize(numSlabs * Slab::kSize);

    AllocatorT alloc(config);

    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    const std::set<uint32_t> acSizes = {512 * 1024};
    auto poolId = alloc.addPool("test_pool", numBytes, acSizes);
    ASSERT_NE(Slab::kInvalidPoolId, poolId);

    // Fill up 20 slabs worth of allocations
    const uint32_t itemSize = 450 * 1024;
    const auto allocSizes = alloc.getPool(poolId).getAllocSizes();
    const size_t itemsPerSlab = Slab::kSize / allocSizes[0];
    const size_t numItems = itemsPerSlab * 20;

    std::vector<typename AllocatorT::WriteHandle> handles;
    // Allocate 20 slabs
    for (size_t i = 0; i < numItems; ++i) {
      std::string key = folly::sformat("key_{}", i);
      auto handle = util::allocateAccessible(alloc, poolId, key, itemSize);
      ASSERT_NE(nullptr, handle);
      // hold handle, so that cachelib cannot release slab and slab release will
      // timeout in memory monitor
      handles.push_back(std::move(handle));
    }

    auto poolStats = alloc.getPoolStats(poolId);
    size_t initialAllocatedSlabs = poolStats.mpStats.allocatedSlabs();
    ASSERT_EQ(initialAllocatedSlabs, 20);

    size_t advisedSlabs = alloc.getPool(poolId).getNumSlabsAdvised();
    ASSERT_EQ(advisedSlabs, 0);

    std::thread monitorThread([&alloc, &poolId, numIterations]() {
      for (int i = 0; i < numIterations; i++) {
        alloc.memMonitor_->adviseAwaySlabs();
        alloc.memMonitor_->checkPoolsAndAdviseReclaim();
        auto advisedSlabs = alloc.getPool(poolId).getNumSlabsAdvised();
        auto expectedAdvisedSlabs = std::min(10, (i + 1) * 5);
        ASSERT_EQ(expectedAdvisedSlabs, advisedSlabs);
      }
    });

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));

    handles.clear();
    monitorThread.join();

    advisedSlabs = alloc.getPool(poolId).getNumSlabsAdvised();
    ASSERT_EQ(advisedSlabs, expectedAdvisedAfterThread);

    alloc.memMonitor_->adviseAwaySlabs();
    alloc.memMonitor_->checkPoolsAndAdviseReclaim();
    advisedSlabs = alloc.getPool(poolId).getNumSlabsAdvised();
    ASSERT_EQ(advisedSlabs, expectedAdvisedAfterOneMore);

    // Verify that slab releases were aborted due to timeouts
    ASSERT_GT(alloc.getGlobalCacheStats().numAbortedSlabReleases, 0);
  }

  // Test to reproduce a bug where memory monitor advises away all slabs when
  // slab releases timeout repeatedly
  void testMemMonitorAdvisesAwayAllCacheBug() {
    int numIterations = 4;
    int sleepSeconds = 5;
    int expectedAdvisedAfterThread = 10;
    int expectedAdvisedAfterOneMore = 10;
    runMemMonitorAdvisesAwaySlabsTest(numIterations, sleepSeconds,
                                      expectedAdvisedAfterThread,
                                      expectedAdvisedAfterOneMore);
  }

  // Test to ensure that if slab release is timed out, memory monitor will retry
  // and still advise away slabs it intended to
  void testMemMonitorAdvisesTimeout() {
    int numIterations = 1;
    int sleepSeconds = 2;
    int expectedAdvisedAfterThread = 5;
    int expectedAdvisedAfterOneMore = 10;
    runMemMonitorAdvisesAwaySlabsTest(numIterations, sleepSeconds,
                                      expectedAdvisedAfterThread,
                                      expectedAdvisedAfterOneMore);
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
