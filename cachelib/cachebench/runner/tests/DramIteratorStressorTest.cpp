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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/cache/CacheStats.h"
#include "cachelib/cachebench/runner/CacheStressor.h"
#include "cachelib/cachebench/util/Sleep.h"

namespace facebook::cachelib::cachebench {
namespace {

// Repeatedly returns set("alpha") so the iterator worker has a deterministic
// non-empty cache to visit during the stressor run.
class RepeatingSetGenerator : public GeneratorBase {
 public:
  RepeatingSetGenerator()
      : request_{keys_.front(), sizes_.begin(), sizes_.end(), OpType::kSet} {}

  const Request& getReq(uint8_t,
                        std::mt19937_64&,
                        std::optional<uint64_t>) override {
    return request_;
  }

  const std::vector<std::string>& getAllKeys() const override { return keys_; }

 private:
  std::vector<std::string> keys_{"alpha"};
  std::vector<size_t> sizes_{64};
  Request request_;
};

CacheConfig makeCacheConfig() {
  CacheConfig config;
  config.allocator = "LRU";
  config.cacheSizeMB = 16;
  config.allocSizes = {256};
  config.htBucketPower = 10;
  config.htLockPower = 5;
  config.chainedItemHtBucketPower = 10;
  config.chainedItemHtLockPower = 5;
  return config;
}

StressorConfig makeStressorConfig(DramIteratorMode dramIteratorMode) {
  StressorConfig config;
  config.numOps = 20;
  config.numThreads = 1;
  config.numKeys = 1;
  config.opDelayBatch = 1;
  config.opDelayNs = 1000 * 1000;
  config.dramIteratorMode = std::move(dramIteratorMode);
  config.dramIteratorIntervalMs = 1;
  return config;
}

void insertItem(Cache<LruAllocator>& cache, folly::StringPiece key) {
  auto item = cache.allocate(PoolId{0}, key, 64);
  ASSERT_NE(nullptr, item);
  cache.insertOrReplace(item);
}

bool waitForDramIteratorSweep(CacheStressor<LruAllocator>& stressor) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  while (std::chrono::steady_clock::now() < deadline) {
    auto stats = stressor.getCacheStats();
    const auto* cacheStats = stats->asPtr<const Stats>();
    if (cacheStats != nullptr &&
        cacheStats->dramIteratorStats.stats.sweeps > 0 &&
        cacheStats->dramIteratorStats.stats.totalItems > 0) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

void expectPeriodicWorkerRecordsDramIterator(DramIteratorMode mode,
                                             const std::string& modeName) {
  calibrateSleep();
  auto config = makeStressorConfig(mode);
  config.dramIteratorSleepMs = 1;
  CacheStressor<LruAllocator> stressor{
      makeCacheConfig(), std::move(config),
      std::make_unique<RepeatingSetGenerator>()};

  stressor.start();
  EXPECT_TRUE(waitForDramIteratorSweep(stressor));
  stressor.finish();

  auto stats = stressor.getCacheStats();
  const auto* cacheStats = stats->asPtr<const Stats>();
  ASSERT_NE(nullptr, cacheStats);
  EXPECT_EQ(modeName, cacheStats->dramIteratorStats.mode);
  EXPECT_GT(cacheStats->dramIteratorStats.stats.sweeps, 0);
  EXPECT_GT(cacheStats->dramIteratorStats.stats.totalItems, 0);
  EXPECT_EQ(0, cacheStats->dramIteratorStats.stats.sweepExceptions);
}

TEST(DramIteratorStressorTest, IteratorsMatchOnStableCache) {
  Cache<LruAllocator> cache{makeCacheConfig()};
  insertItem(cache, "alpha");
  insertItem(cache, "bravo");
  insertItem(cache, "charlie");

  const auto regular = cache.runRegularDramIteratorSweep();
  const auto lockGroup = cache.runLockGroupDramIteratorSweep();

  EXPECT_EQ(3, regular.items);
  EXPECT_EQ(regular.items, lockGroup.items);
  EXPECT_EQ(regular.keyBytes, lockGroup.keyBytes);
  EXPECT_EQ(regular.valueBytes, lockGroup.valueBytes);
}

TEST(DramIteratorStressorTest, PeriodicWorkerRecordsRegularIterator) {
  expectPeriodicWorkerRecordsDramIterator(DramIteratorMode::kRegular,
                                          "regular");
}

TEST(DramIteratorStressorTest, PeriodicWorkerRecordsLockGroupIterator) {
  expectPeriodicWorkerRecordsDramIterator(DramIteratorMode::kLockGroup,
                                          "lock_group");
}

TEST(DramIteratorStressorTest, DisabledModeDoesNotRunIterator) {
  CacheStressor<LruAllocator> stressor{
      makeCacheConfig(), makeStressorConfig(DramIteratorMode::kDisabled),
      std::make_unique<RepeatingSetGenerator>()};

  stressor.start();
  stressor.finish();

  const auto stats = stressor.getCacheStats();
  const auto* cacheStats = stats->asPtr<const Stats>();
  ASSERT_NE(nullptr, cacheStats);
  EXPECT_TRUE(cacheStats->dramIteratorStats.mode.empty());
  EXPECT_EQ(0, cacheStats->dramIteratorStats.stats.sweeps);
  EXPECT_EQ(0, cacheStats->dramIteratorStats.stats.sweepExceptions);
}

TEST(DramIteratorStressorTest, SchedulerDelaysFirstSweepByConfiguredInterval) {
  auto config = makeStressorConfig(DramIteratorMode::kRegular);
  config.dramIteratorIntervalMs = 60 * 1000;
  CacheStressor<LruAllocator> stressor{
      makeCacheConfig(), std::move(config),
      std::make_unique<RepeatingSetGenerator>()};

  stressor.start();
  stressor.finish();

  const auto stats = stressor.getCacheStats();
  const auto* cacheStats = stats->asPtr<const Stats>();
  ASSERT_NE(nullptr, cacheStats);
  EXPECT_EQ(0, cacheStats->dramIteratorStats.stats.sweeps);
}

} // namespace
} // namespace facebook::cachelib::cachebench
