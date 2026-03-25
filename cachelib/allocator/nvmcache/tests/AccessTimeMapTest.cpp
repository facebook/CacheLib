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

#include <thread>
#include <vector>

#include "cachelib/allocator/nvmcache/AccessTimeMap.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace tests {

TEST(AccessTimeMapTest, BasicSetAndGet) {
  AccessTimeMap m(4);

  // get on missing key returns nullopt
  ASSERT_EQ(m.get(42), std::nullopt);

  // set then get returns value
  m.set(42, 100);
  ASSERT_EQ(m.get(42), 100);

  // overwrite returns new value
  m.set(42, 200);
  ASSERT_EQ(m.get(42), 200);
}

TEST(AccessTimeMapTest, GetAndRemove) {
  AccessTimeMap m(4);

  // getAndRemove on never-set key returns nullopt
  ASSERT_EQ(m.getAndRemove(99), std::nullopt);

  m.set(99, 300);
  ASSERT_EQ(m.getAndRemove(99), 300);

  // second getAndRemove on same key returns nullopt
  ASSERT_EQ(m.getAndRemove(99), std::nullopt);
}

TEST(AccessTimeMapTest, Remove) {
  AccessTimeMap m(4);

  m.set(10, 500);
  m.remove(10);
  ASSERT_EQ(m.get(10), std::nullopt);

  // remove on missing key doesn't crash or change size
  auto sizeBefore = m.size();
  m.remove(999);
  ASSERT_EQ(m.size(), sizeBefore);
}

TEST(AccessTimeMapTest, SizeTracking) {
  AccessTimeMap m(4);

  ASSERT_EQ(m.size(), 0);

  m.set(1, 10);
  ASSERT_EQ(m.size(), 1);

  m.set(2, 20);
  ASSERT_EQ(m.size(), 2);

  // overwrite existing key — size unchanged
  m.set(1, 11);
  ASSERT_EQ(m.size(), 2);

  m.remove(1);
  ASSERT_EQ(m.size(), 1);

  m.getAndRemove(2);
  ASSERT_EQ(m.size(), 0);
}

TEST(AccessTimeMapTest, ZeroShardsThrows) {
  ASSERT_THROW(AccessTimeMap(0), std::invalid_argument);
  // Also verify with a non-zero maxSize (the division-by-zero scenario).
  ASSERT_THROW(AccessTimeMap(0, 100), std::invalid_argument);
}

TEST(AccessTimeMapTest, EvictionAtCapacity) {
  // 1 shard, maxSize 1 → maxEntriesPerShard_ = 1
  AccessTimeMap m(1, /*maxSize=*/1);

  m.set(0, 100);
  ASSERT_EQ(m.size(), 1);

  // inserting a second key in the same shard evicts the first
  m.set(1, 200);
  ASSERT_EQ(m.size(), 1);
  ASSERT_EQ(m.get(0), std::nullopt);
  ASSERT_EQ(m.get(1), 200);

  // updating the existing key at capacity does NOT trigger eviction
  m.set(1, 300);
  ASSERT_EQ(m.size(), 1);
  ASSERT_EQ(m.get(1), 300);
}

TEST(AccessTimeMapTest, UnboundedByDefault) {
  AccessTimeMap m(4); // default maxSize = 0 → unbounded

  const size_t n = 1000;
  for (size_t i = 0; i < n; ++i) {
    m.set(i, static_cast<uint32_t>(i));
  }

  ASSERT_EQ(m.size(), n);
  for (size_t i = 0; i < n; ++i) {
    ASSERT_EQ(m.get(i), static_cast<uint32_t>(i));
  }
}

TEST(AccessTimeMapTest, ConcurrentSetAndGet) {
  constexpr unsigned int kNumShards = 8;
  constexpr unsigned int kNumThreads = 8;
  constexpr unsigned int kKeysPerThread = 500;

  AccessTimeMap m(kNumShards);

  // Phase 1: concurrent sets on non-overlapping key ranges
  {
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (unsigned int t = 0; t < kNumThreads; ++t) {
      threads.emplace_back([&m, t]() {
        uint64_t base = static_cast<uint64_t>(t) * kKeysPerThread;
        for (unsigned int i = 0; i < kKeysPerThread; ++i) {
          m.set(base + i, static_cast<uint32_t>(i));
        }
      });
    }
    for (auto& th : threads) {
      th.join();
    }
  }

  ASSERT_EQ(m.size(), kNumThreads * kKeysPerThread);

  // Phase 2: concurrent mixed operations (get, set, remove, getAndRemove)
  {
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (unsigned int t = 0; t < kNumThreads; ++t) {
      threads.emplace_back([&m, t]() {
        uint64_t base = static_cast<uint64_t>(t) * kKeysPerThread;
        for (unsigned int i = 0; i < kKeysPerThread; ++i) {
          auto key = base + i;
          // read existing
          m.get(key);
          // overwrite
          m.set(key, static_cast<uint32_t>(i + 1));
          // remove odd keys
          if (i % 2 == 1) {
            m.remove(key);
          }
        }
      });
    }
    for (auto& th : threads) {
      th.join();
    }
  }

  // After removing odd keys, each thread should have kKeysPerThread/2 remaining
  ASSERT_EQ(m.size(), kNumThreads * (kKeysPerThread / 2));

  // Verify even keys are still present and odd keys are gone
  for (unsigned int t = 0; t < kNumThreads; ++t) {
    uint64_t base = static_cast<uint64_t>(t) * kKeysPerThread;
    for (unsigned int i = 0; i < kKeysPerThread; ++i) {
      if (i % 2 == 0) {
        ASSERT_NE(m.get(base + i), std::nullopt);
      } else {
        ASSERT_EQ(m.get(base + i), std::nullopt);
      }
    }
  }
}

TEST(AccessTimeMapTest, CounterBasic) {
  AccessTimeMap m(4);
  util::StatsMap statsMap;

  // set 3 keys
  m.set(1, 10);
  m.set(2, 20);
  m.set(3, 30);

  // get: 2 hits, 1 miss
  m.get(1);
  m.get(2);
  m.get(999);

  // getAndRemove: 1 hit, 1 miss
  m.getAndRemove(3);
  m.getAndRemove(888);

  // remove
  m.remove(1);

  m.getCounters(statsMap.createCountVisitor());
  auto counters = statsMap.toMap();

  EXPECT_EQ(counters["navy_atm_sets"], 3);
  EXPECT_EQ(counters["navy_atm_gets"], 3);
  EXPECT_EQ(counters["navy_atm_get_and_removes"], 2);
  EXPECT_EQ(counters["navy_atm_removes"], 1);
  EXPECT_EQ(counters["navy_atm_hits"], 3);
  EXPECT_EQ(counters["navy_atm_misses"], 2);
  EXPECT_EQ(counters["navy_atm_size"], 1);
}

TEST(AccessTimeMapTest, CounterEviction) {
  // 1 shard, maxSize 2 → maxEntriesPerShard_ = 2
  AccessTimeMap m(1, /*maxSize=*/2);
  util::StatsMap statsMap;

  m.set(0, 100);
  m.set(1, 200);

  // shard is full; inserting a new key triggers eviction
  m.set(2, 300);
  m.getCounters(statsMap.createCountVisitor());
  EXPECT_EQ(statsMap.toMap()["navy_atm_evictions"], 1);

  // updating an existing key at capacity does NOT trigger eviction
  statsMap = util::StatsMap{};
  m.set(2, 400);
  m.getCounters(statsMap.createCountVisitor());
  EXPECT_EQ(statsMap.toMap()["navy_atm_evictions"], 1);
}

TEST(AccessTimeMapTest, CounterTypes) {
  AccessTimeMap m(4);
  m.set(1, 10);
  m.get(1);

  std::map<std::string, util::CounterVisitor::CounterType> types;
  m.getCounters(util::CounterVisitor(
      [&](folly::StringPiece name,
          double,
          util::CounterVisitor::CounterType t) { types[name.str()] = t; }));

  EXPECT_EQ(types["navy_atm_size"], util::CounterVisitor::CounterType::COUNT);
  EXPECT_EQ(types["navy_atm_sets"], util::CounterVisitor::CounterType::RATE);
  EXPECT_EQ(types["navy_atm_gets"], util::CounterVisitor::CounterType::RATE);
  EXPECT_EQ(types["navy_atm_get_and_removes"],
            util::CounterVisitor::CounterType::RATE);
  EXPECT_EQ(types["navy_atm_removes"], util::CounterVisitor::CounterType::RATE);
  EXPECT_EQ(types["navy_atm_hits"], util::CounterVisitor::CounterType::RATE);
  EXPECT_EQ(types["navy_atm_misses"], util::CounterVisitor::CounterType::RATE);
  EXPECT_EQ(types["navy_atm_evictions"],
            util::CounterVisitor::CounterType::RATE);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
