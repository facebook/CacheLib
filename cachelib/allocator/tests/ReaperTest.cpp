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

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook::cachelib::tests {
namespace {
using Cache = LruAllocator;

Cache::Config reaperTestConfig() {
  Cache::Config config;
  config.setCacheSize(32 * 1024 * 1024).setCacheName("reaper-test");
  config.enableItemReaperInBackground(std::chrono::milliseconds(0));
  config.enablePoolRebalancing(std::make_shared<RebalanceStrategy>(),
                               std::chrono::milliseconds(0));
  return config;
}

template <typename Predicate>
bool waitFor(Predicate predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

size_t visitSlots(Cache& cache) {
  size_t visits = 0;
  ReaperAPIWrapper<Cache>::traverseAndExpireItems(cache, [&](void*, AllocInfo) {
    ++visits;
    return true;
  });
  return visits;
}

} // namespace

TEST(ReaperTest, EmptyCacheSkipsAllocatedSlots) {
  Cache cache(reaperTestConfig());
  const auto pool =
      cache.addPool("pool", cache.getCacheMemoryStats().ramCacheSize);
  EXPECT_EQ(visitSlots(cache), 0);
  auto parent = cache.allocate(pool, "parent", 128);
  ASSERT_TRUE(parent);
  auto child = cache.allocateChainedItem(parent, 128);
  ASSERT_TRUE(child);
  cache.addChainedItem(parent, std::move(child));
  // Allocated but unpublished items and their children cannot be reaped.
  EXPECT_EQ(cache.getAccessContainerNumKeys(), 0);
  EXPECT_EQ(visitSlots(cache), 0);
  cache.insertOrReplace(parent);
  EXPECT_GT(visitSlots(cache), 0);
  cache.remove("parent");
  // The outstanding parent handle keeps its allocations alive after unlink.
  EXPECT_EQ(cache.getAccessContainerNumKeys(), 0);
  EXPECT_EQ(visitSlots(cache), 0);
  EXPECT_EQ(parent->getKey(), "parent");
  auto live = cache.allocate(pool, "live", 128);
  ASSERT_TRUE(live);
  cache.insertOrReplace(live);
  EXPECT_GT(visitSlots(cache), 0);
}

TEST(ReaperTest, ReactivatesAcrossEmptyTransitions) {
  Cache cache(reaperTestConfig());
  const auto pool =
      cache.addPool("pool", cache.getCacheMemoryStats().ramCacheSize);
  Reaper<Cache> reaper(cache, {});
  ASSERT_TRUE(reaper.start(std::chrono::milliseconds(10), "reaper-test"));
  ASSERT_TRUE(waitFor([&] { return reaper.getStats().numTraversals > 0; }));
  EXPECT_EQ(reaper.getStats().numVisitedItems, 0);
  for (size_t i = 0; i < 20; ++i) {
    auto expired =
        cache.allocate(pool, "expired", 128, 1, util::getCurrentTimeSec() - 10);
    ASSERT_TRUE(expired);
    auto child = cache.allocateChainedItem(expired, 128);
    ASSERT_TRUE(child);
    cache.addChainedItem(expired, std::move(child));
    cache.insertOrReplace(expired);
    auto pinnedPasses = reaper.getStats().numTraversals;
    ASSERT_TRUE(waitFor(
        [&] { return reaper.getStats().numTraversals > pinnedPasses + 1; }));
    // Reaping must preserve an expired parent while another handle pins it.
    EXPECT_EQ(cache.getAccessContainerNumKeys(), 1);
    EXPECT_EQ(expired->getKey(), "expired");
    expired.reset();
    ASSERT_TRUE(
        waitFor([&] { return cache.getAccessContainerNumKeys() == 0; }));
    EXPECT_FALSE(cache.find("expired"));
    auto live = cache.allocate(pool, "live", 128);
    ASSERT_TRUE(live);
    cache.insertOrReplace(live);
    auto passes = reaper.getStats().numTraversals;
    ASSERT_TRUE(
        waitFor([&] { return reaper.getStats().numTraversals > passes + 1; }));
    EXPECT_TRUE(cache.find("live"));
    EXPECT_EQ(cache.getAccessContainerNumKeys(), 1);
    cache.remove("live");
  }
  reaper.stop(std::chrono::seconds(0));
  EXPECT_EQ(reaper.getStats().numReapedItems, 20);
  EXPECT_EQ(reaper.getStats().numVisitErrs, 0);
  EXPECT_EQ(cache.getAccessContainerNumKeys(), 0);
}

TEST(ReaperTest, NonemptyCacheStillSkipsSlabDuringConcurrentTraversal) {
  Cache cache(reaperTestConfig());
  const auto pool =
      cache.addPool("pool", cache.getCacheMemoryStats().ramCacheSize);
  auto item = cache.allocate(pool, "live", 128);
  ASSERT_TRUE(item);
  cache.insertOrReplace(item);

  folly::Baton<> entered;
  folly::Baton<> release;
  std::thread scanner([&] {
    bool first = true;
    ReaperAPIWrapper<Cache>::traverseAndExpireItems(cache,
                                                    [&](void*, AllocInfo) {
                                                      if (first) {
                                                        first = false;
                                                        entered.post();
                                                        release.wait();
                                                      }
                                                      return true;
                                                    });
  });

  const bool enteredSlab = entered.try_wait_for(std::chrono::seconds(5));
  const auto skippedBefore = cache.getGlobalCacheStats().numReaperSkippedSlabs;
  const auto concurrentVisits = enteredSlab ? visitSlots(cache) : 0;
  const auto skippedAfter = cache.getGlobalCacheStats().numReaperSkippedSlabs;
  release.post();
  scanner.join();

  ASSERT_TRUE(enteredSlab);
  EXPECT_EQ(concurrentVisits, 0);
  EXPECT_GT(skippedAfter, skippedBefore);
  EXPECT_TRUE(cache.find("live"));
}

} // namespace facebook::cachelib::tests
