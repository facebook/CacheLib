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

#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/datatype/Map.h"
#include "cachelib/datatype/RangeMap.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace detail {
struct TestValue {
  static std::unique_ptr<TestValue> create(uint32_t val, uint32_t size) {
    return std::unique_ptr<TestValue>{new (size) TestValue(val, size)};
  }

  static void* operator new(size_t /* unused */, size_t actualSize) {
    void* ptr = malloc(actualSize);
    XDCHECK(ptr != nullptr);
    return ptr;
  }

  static void operator delete(void* ptr) { free(ptr); }

  TestValue(uint32_t val, uint32_t size) : size_{size}, val_{val} {}

  uint32_t getStorageSize() const { return size_; }

  // Actual size of the test value.
  uint32_t size_;

  // Value we will be using to ensure correctness in the test.
  uint32_t val_;
};
} // namespace detail

// This test case is designed to accumulate a huge amount of outstanding
// refcounts on a single item in order to test if cache can run fine when
// we run into refcount overflow problems.
class HighRefcountStressor : public Stressor {
 public:
  // @param cacheConfig configuration for the cache.
  // @param numOps   number of ops per thread.
  HighRefcountStressor(const CacheConfig& cacheConfig, uint64_t numOps);

  Stats getCacheStats() const override { return cache_->getStats(); }

  // Only number of operations are reported
  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats stats;
    stats.ops = ops_;
    return stats;
  }

  uint64_t getTestDurationNs() const override {
    return std::chrono::nanoseconds{endTime_ - startTime_}.count();
  }

  void start() override;

  void finish() override { testThread_.join(); }

 private:
  void testLoop();

  static const uint32_t kNumThreads = 10'000;

  const uint64_t numOpsPerThread_{};

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  std::atomic<uint64_t> ops_{0};

  std::unique_ptr<Cache<LruAllocator>> cache_;

  std::thread testThread_;
};

// This test case populates a cachelib::Map and tries to expand it to a very
// large size and then remove some entries at random and try to compact. It
// will read the entries of the map by iterating through them and try to
// verify they do exist by looking them up via the map's hash table.
class CachelibMapStressor : public Stressor {
 public:
  CachelibMapStressor(const CacheConfig& cacheConfig, uint64_t numOps);

  Stats getCacheStats() const override { return cache_->getStats(); }

  // Only number of operations are reported
  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats stats;
    stats.ops = ops_;
    return stats;
  }

  uint64_t getTestDurationNs() const override {
    return std::chrono::nanoseconds{endTime_ - startTime_}.count();
  }

  void start() override;

  void finish() override { testThread_.join(); }

 private:
  using CacheType = Cache<LruAllocator>;
  using TestMap = cachelib::Map<uint32_t, detail::TestValue, CacheType>;

  // Add entries to the map
  static void populate(TestMap& map);

  // Randomly delete some elements
  static void pokeHoles(TestMap& map);

  // Read entries from the map by iterating and verify they can be looked up
  static void readEntries(TestMap& map);

  void testLoop();

  void lockExclusive(CacheType::Key key) { getLock(key).lock(); }
  void unlockExclusive(CacheType::Key key) { getLock(key).unlock(); }

  folly::SharedMutex& getLock(CacheType::Key key) {
    auto bucket = MurmurHash2{}(key.data(), key.size()) % locks_.size();
    return locks_[bucket];
  }

  // Test configurations.
  static const uint32_t kMapSizeUpperbound = 10'000;
  static const uint32_t kMapDeletionRate = 5;
  static const uint32_t kMapShrinkSize = 100;
  static const uint32_t kMapEntryValueSizeMin = 1000;
  static const uint32_t kMapEntryValueSizeMax = 2000;
  static const uint32_t kMapInsertionBatchMin = 1000;
  static const uint32_t kMapInsertionBatchMax = 2000;

  const uint64_t numOpsPerThread_{};

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  std::atomic<uint64_t> ops_{0};

  std::array<std::string, 100> keys_;

  std::unique_ptr<CacheType> cache_;
  std::array<folly::SharedMutex, 100> locks_;

  std::thread testThread_;
};

// This test case populates a cachelib::Map and tries to expand it to a very
// large size and then remove some entries at random and try to compact. It
// will read the entries of the map by iterating through them and try to
// verify they do exist by looking them up via the map's index.
class CachelibRangeMapStressor : public Stressor {
 public:
  CachelibRangeMapStressor(const CacheConfig& cacheConfig, uint64_t numOps);

  Stats getCacheStats() const override { return cache_->getStats(); }

  // Only number of operations are reported
  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats stats;
    stats.ops = ops_;
    return stats;
  }

  uint64_t getTestDurationNs() const override {
    return std::chrono::nanoseconds{endTime_ - startTime_}.count();
  }

  void start() override;

  void finish() override { testThread_.join(); }

 private:
  using CacheType = Cache<LruAllocator>;
  using TestMap = cachelib::RangeMap<uint32_t, detail::TestValue, CacheType>;

  // Add entries to the map
  static void populate(TestMap& map);

  // Randomly delete some elements
  static void pokeHoles(TestMap& map);

  // Read entries from the map by iterating and verify they can be looked up
  static void readEntries(TestMap& map);

  void testLoop();

  void lockExclusive(CacheType::Key key) { getLock(key).lock(); }
  void unlockExclusive(CacheType::Key key) { getLock(key).unlock(); }

  folly::SharedMutex& getLock(CacheType::Key key) {
    auto bucket = MurmurHash2{}(key.data(), key.size()) % locks_.size();
    return locks_[bucket];
  }

  // Test configurations.
  static const uint32_t kMapSizeUpperbound = 10'000;
  static const uint32_t kMapDeletionRate = 5;
  static const uint32_t kMapShrinkSize = 100;
  static const uint32_t kMapEntryValueSizeMin = 1000;
  static const uint32_t kMapEntryValueSizeMax = 2000;
  static const uint32_t kMapInsertionBatchMin = 1000;
  static const uint32_t kMapInsertionBatchMax = 2000;

  const uint64_t numOpsPerThread_{};

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  std::atomic<uint64_t> ops_{0};

  std::array<std::string, 100> keys_;

  std::unique_ptr<CacheType> cache_;
  std::array<folly::SharedMutex, 100> locks_;

  std::thread testThread_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
