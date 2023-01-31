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
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/TestBase.h"
#include "cachelib/common/TestUtils.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace tests {

template <typename AllocatorT>
class AllocatorHitStatsTest : public SlabAllocatorTestBase {
 public:
  void testFragmentationStats() {
    typename AllocatorT::Config config;
    config.setCacheSize(100 * Slab::kSize);
    // Enable chained item to test chained item's fragmentation.
    config.configureChainedItems();
    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    AllocatorT alloc(config);
    const std::set<uint32_t>& allocSizes = {1024,      4 * 1024,   16 * 1024,
                                            64 * 1024, 256 * 1024, 1024 * 1024};
    const auto poolId = alloc.addPool(
        "1", alloc.getCacheMemoryStats().ramCacheSize, allocSizes);

    const int numItems = 10000;

    const int maxRounds = 8;
    const int intervalSec = 5;
    // Allocate items with TTL.
    for (unsigned int i = 0; i < numItems; ++i) {
      const auto allocSize = folly::Random::rand32(100, 1024 * 1024 - 1000);
      auto keyName = folly::to<std::string>(i);
      const auto ttl = folly::Random::rand32(1, maxRounds) * intervalSec;
      auto handle =
          util::allocateAccessible(alloc, poolId, keyName, allocSize, ttl);
    }

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Start to test fragmentation size
    for (unsigned int i = 0; i < maxRounds + 1; ++i) {
      /* sleep override */
      std::this_thread::sleep_for(
          std::chrono::milliseconds(intervalSec * 1000));

      for (unsigned int item = 0; item < numItems; ++item) {
        // find and remove the expired items.
        alloc.find(folly::to<std::string>(item));
      }

      uint64_t totFragMemory = getTotFragMemory(alloc);

      ASSERT_EQ(alloc.getPoolStats(poolId).totalFragmentation(), totFragMemory);
      // make sure all the items are clear.
      if (i == maxRounds) {
        ASSERT_EQ(alloc.getPoolStats(poolId).totalFragmentation(), 0);
      }
    }

    // Allocate new items to test memory fragmentation after release slab.
    for (unsigned int i = 0; i < numItems; ++i) {
      const auto keyLen = folly::Random::rand32(10, 100);
      const auto allocSize = folly::Random::rand32(100, 1024 * 1024 - 1000);
      auto str = cachelib::test_util::getRandomAsciiStr(keyLen);
      auto handle = util::allocateAccessible(alloc, poolId, str, allocSize);
    }

    // Get the original total fragmentation before releasing slabs.
    uint64_t totFrag = getTotFragMemory(alloc);
    ASSERT_EQ(alloc.getPoolStats(poolId).totalFragmentation(), totFrag);

    // Current fragmentation size after releasing a slab.
    uint64_t curFrag = 0;
    // Release all the slabs one by one,
    // and then test the total fragmentation after releasing.
    const auto cids = alloc.getPoolStats(poolId).mpStats.classIds;
    for (const auto& cid : cids) {
      while (alloc.getPoolStats(poolId).cacheStats.at(cid).fragmentationSize >
             0) {
        alloc.releaseSlab(poolId, cid, SlabReleaseMode::kRebalance);
        curFrag = getTotFragMemory(alloc);
        ASSERT_EQ(alloc.getPoolStats(poolId).totalFragmentation(), curFrag);
        ASSERT_GT(totFrag, curFrag);
        totFrag = curFrag;
      }
    }
    // Make sure there are no items left in the end.
    ASSERT_EQ(alloc.getPoolStats(poolId).totalFragmentation(), 0);

    // Test chained item's fragmentation
    uint64_t totChainFrag = 0;
    const int numParents = 10;

    // allocate chained items' parents.
    for (unsigned int i = 0; i < numParents; ++i) {
      auto parent = util::allocateAccessible(
          alloc, poolId, "parent" + folly::to<std::string>(i),
          folly::Random::rand32(100, 2 * 1024));
      ASSERT_NE(nullptr, parent);
      totChainFrag += util::getFragmentation(alloc, *parent);
    }

    // allocate chained items for random parents.
    for (unsigned int i = 0; i < numItems; ++i) {
      const auto parentName =
          "parent" +
          folly::to<std::string>(folly::Random::rand32(0, numParents));
      auto parent = alloc.findToWrite(parentName);
      ASSERT_NE(nullptr, parent);
      auto childItem = alloc.allocateChainedItem(
          parent, folly::Random::rand32(100, 2 * 1024));
      ASSERT_NE(nullptr, childItem);
      totChainFrag += util::getFragmentation(alloc, *childItem);
      alloc.addChainedItem(parent, std::move(childItem));

      // check the fragmentation size of chained items.
      ASSERT_EQ(totChainFrag, alloc.getPoolStats(poolId).totalFragmentation());
    }

    // Remove all the chained items' parent directly,
    // which also removes all the chained items.
    for (unsigned int i = 0; i < numParents; ++i) {
      auto parent = alloc.find("parent" + folly::to<std::string>(i));
      alloc.remove(parent);
    }
    // to make sure clean the chained items clearly.
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(5));
    // So the total fragmentation size of pool stats should be 0.
    ASSERT_EQ(0, alloc.getPoolStats(poolId).totalFragmentation());
  }

  void testPoolName() {
    std::unordered_map<PoolId, std::string> poolsNames;
    typename AllocatorT::Config config;
    const size_t cacheSize = 100 * Slab::kSize;
    config.setCacheSize(cacheSize);
    AllocatorT alloc(config);

    const int numPools = folly::Random::rand32(MemoryPoolManager::kMaxPools / 2,
                                               MemoryPoolManager::kMaxPools);

    // create pools with unique pool name
    const auto poolSize = cacheSize / (numPools * 2);
    for (int i = 0; i < numPools; ++i) {
      const auto poolNameLen = folly::Random::rand32(10, 100);
      auto poolName = cachelib::test_util::getRandomAsciiStr(poolNameLen);
      const auto pid = alloc.addPool(poolName, poolSize);
      poolsNames.insert({pid, poolName});
    }

    ASSERT_GT(poolsNames.size(), 0);

    // check the pool name by its pool ID
    const int numGetPoolNameTry = 10000;
    for (unsigned int i = 0; i < numGetPoolNameTry; ++i) {
      const auto pid = static_cast<PoolId>(folly::Random::rand32(0, numPools));
      ASSERT_EQ(poolsNames[pid], alloc.getPoolStats(pid).poolName);
    }
  }

  void testCacheStats() {
    // the eviction call back to keep track of the evicted keys.
    std::set<std::string> evictedKeys;
    std::mutex evictCbMutex;
    auto evictCb = [&evictedKeys, &evictCbMutex](
                       const typename AllocatorT::RemoveCbData& data) {
      if (data.context == RemoveContext::kEviction) {
        std::lock_guard<std::mutex> l(evictCbMutex);
        const auto k = data.item.getKey();
        evictedKeys.insert({k.data(), k.size()});
      }
    };
    typename AllocatorT::Config config;
    config.setRemoveCallback(evictCb);
    config.setCacheSize(100 * Slab::kSize);

    // Disable slab rebalancing
    config.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    // create an allocator worth 100 slabs.
    AllocatorT alloc(config);
    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    const auto poolSize = numBytes / 2;
    auto poolId = alloc.addPool("one", poolSize);
    auto poolId2 = alloc.addPool("two", poolSize);

    const unsigned int nThreads = 10;    // number of threads
    const unsigned int numHits = 100000; // hits per thread
    const unsigned int numMiss = 10000;  // miss per thread
    const unsigned int numValidAllocsPerThread = 10000;
    const unsigned int numInValidAllocsPerThread = 10000;
    const unsigned int numInValidSizePerThread = 10;
    std::vector<std::thread> threads; // threads
    // number of threads that have completed doing hits/misses.
    std::atomic<unsigned int> nCompleted{0};
    std::atomic<unsigned int> nFailed{0};
    std::atomic<unsigned int> nInvalid{0};
    std::atomic<unsigned int> nAllocSuccess{0};
    // true if the last thread is done with hits/misses
    bool completed = false;
    // true if the main thread is done checking the stats
    bool doneChecking = false;
    // condition variable for completion of all hits/misses
    std::condition_variable cv;
    // condition variable for completion of main thread.
    std::condition_variable cv2;
    // mutex protecting all the condition variables.
    std::mutex m;

    // try to allocate as much as possible and ensure that even when we run more
    // than twice the size of the cache, we are able to allocate by recycling
    // the memory. To do this, we first ensure we have a minimum number of
    // allocations across a set of sizes.
    std::vector<std::string> keys;
    const unsigned int nKeys = 1000;
    while (keys.size() != nKeys) {
      const auto keyLen = folly::Random::rand32(10, 100);
      const auto allocSize = folly::Random::rand32(100, 1024 * 1024 - 1000);
      auto str = cachelib::test_util::getRandomAsciiStr(keyLen);
      auto handle = util::allocateAccessible(alloc, poolId, str, allocSize);
      if (handle) {
        keys.push_back(str);
      }
    }

    if (!evictedKeys.empty()) {
      for (const auto& key : evictedKeys) {
        auto it = std::find(keys.begin(), keys.end(), key);
        if (it != keys.end()) {
          keys.erase(it);
        }
      }
    }

    ASSERT_GT(keys.size(), 0);

    // check the memory fragmentation.
    uint64_t totFragMemory = getTotFragMemory(alloc);

    // only allocated in poolId so poolId2 should be 0.
    ASSERT_EQ(alloc.getPoolStats(poolId2).totalFragmentation(), 0);
    ASSERT_EQ(alloc.getPoolStats(poolId).totalFragmentation(), totFragMemory);
    ASSERT_GT(totFragMemory, 0);

    for (unsigned int j = 0; j < nThreads; j++) {
      threads.push_back(std::thread([&]() {
        for (unsigned int i = 0; i < numHits; i++) {
          const unsigned int index = folly::Random::rand32(0, keys.size());
          auto handle = alloc.find(keys[index]);
        }

        for (unsigned int i = 0; i < numMiss; i++) {
          const auto keyLen = folly::Random::rand32(101, 200);
          auto str = cachelib::test_util::getRandomAsciiStr(keyLen);
          auto handle = alloc.find(str);
        }

        // use poolId2 to do some allocs. some of these will fail when we run
        // out of slabs.
        for (unsigned int i = 0; i < numValidAllocsPerThread; i++) {
          const auto keyLen = folly::Random::rand32(10, 100);
          const auto allocSize = folly::Random::rand32(100, 1024 * 1024 - 1000);
          auto str = cachelib::test_util::getRandomAsciiStr(keyLen);
          auto handle =
              util::allocateAccessible(alloc, poolId2, str, allocSize);
          if (handle) {
            ++nAllocSuccess;
          } else {
            ++nFailed;
          }
        }

        for (unsigned int i = 0; i < numInValidAllocsPerThread; i++) {
          // key length is invalid
          const auto keyLen = folly::Random::rand32(257, 500);

          // use valid alloc size. invalid alloc size is not measured per alloc
          // size
          const auto allocSize = folly::Random::rand32(100, 1024 * 1024 - 1000);
          auto str = cachelib::test_util::getRandomAsciiStr(keyLen);
          try {
            auto handle =
                util::allocateAccessible(alloc, poolId2, str, allocSize);
            if (!handle) {
              ++nFailed;
            } else {
              ++nAllocSuccess;
            }
          } catch (const std::exception& e) {
            ++nInvalid;
          }
        }

        // invalid alloc size
        const auto keyLen = folly::Random::rand32(57, 100);
        for (unsigned int i = 0; i < numInValidSizePerThread; i++) {
          const auto allocSize =
              folly::Random::rand32(10 * 1024 * 1024, 12 * 1024 * 1024);
          auto str = cachelib::test_util::getRandomAsciiStr(keyLen);
          try {
            auto handle =
                util::allocateAccessible(alloc, poolId2, str, allocSize);
            if (!handle) {
              ++nFailed;
            } else {
              ++nAllocSuccess;
            }
          } catch (const std::exception& e) {
            ++nInvalid;
          }
        }

        // last thread to complete  notifies the main thread.
        if (++nCompleted == nThreads) {
          {
            std::unique_lock<std::mutex> l(m);
            completed = true;
            // notify that we are done bumping up the stats.
          }
          cv.notify_one();
        }

        // wait for the other thread to check for all the stats. we need to
        // stay alive until the other thread executes the asserts to ensure
        // that the thread local stats are not destroyed.
        std::unique_lock<std::mutex> l(m);
        cv2.wait(l, [&] { return doneChecking; });
      }));
    }

    // wait for the threads to bump up the stats.
    {
      std::unique_lock<std::mutex> l(m);
      cv.wait(l, [&] { return completed; });
    }

    auto poolStats = alloc.getPoolStats(poolId);
    // alloc stats are for pool2.
    auto poolStats2 = alloc.getPoolStats(poolId2);

    const auto globalCacheStats = alloc.getGlobalCacheStats();

    // we have read the stats. now kill off all the threads. we ll verify the
    // stats later.
    {
      std::unique_lock<std::mutex> l(m);
      doneChecking = true;
      cv2.notify_all();
    }

    for (auto& thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }

    // verify all the stats.
    ASSERT_EQ(nThreads * numHits, poolStats.numPoolGetHits);
    ASSERT_EQ(nThreads * (numHits + numMiss), globalCacheStats.numCacheGets);
    ASSERT_EQ(nThreads * numMiss, globalCacheStats.numCacheGetMiss);

    uint64_t totalHits = 0;
    for (const auto& classId : poolStats.getClassIds()) {
      totalHits += poolStats.cacheStats[classId].numHits;
    }

    ASSERT_EQ(totalHits, poolStats.numPoolGetHits);

    ASSERT_EQ(nThreads * (numValidAllocsPerThread + numInValidAllocsPerThread),
              poolStats2.numAllocAttempts());
    ASSERT_EQ(nFailed, poolStats2.numAllocFailures());
    ASSERT_EQ(nInvalid, globalCacheStats.invalidAllocs);

    // verify the memory fragmentation size.
    totFragMemory = getTotFragMemory(alloc);

    ASSERT_EQ(poolStats.totalFragmentation() + poolStats2.totalFragmentation(),
              totFragMemory);

    ASSERT_GT(totFragMemory, 0);
  }

 private:
  // To get the total memory fragmentation size from the allocator.
  uint64_t getTotFragMemory(AllocatorT& alloc) const {
    uint64_t res = 0;
    // Get all the items accessible and accumulate their fragmentation.
    for (const auto& item : alloc) {
      if (item.isAccessible()) {
        res += alloc.getUsableSize(item) - item.getSize();
      }
    }
    return res;
  }
};

} // namespace tests
} // namespace cachelib
} // namespace facebook
