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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <random>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/compact_cache/CCacheCreator.h"

using namespace facebook::cachelib;

DEFINE_double(write_percentage, 0.05, "Percentage of write operations");
DEFINE_double(miss_percentage, 0.05, "Percentage of misses in read operations");
DEFINE_uint64(cache_size, 10UL * 1024UL * 1024UL * 1024UL, "size of cache");
DEFINE_uint64(num_keys, 10UL * 1000UL * 1000UL, "number of keys");
DEFINE_uint64(num_ops, 100UL * 1000UL * 1000UL, "number of operations");

inline uint32_t getKey(uint32_t i) { return i % FLAGS_num_keys; }

template <size_t KeySize>
struct CacheTestImpl {
  struct FOLLY_PACK_ATTR Key {
    uint32_t value;
    uint8_t _[KeySize - sizeof(uint32_t)]{};

    bool isEmpty() { return value == 0; }
    bool operator==(const Key& rhs) {
      return std::memcmp(this, &rhs, sizeof(Key)) == 0;
    }

    explicit Key(uint32_t i) : value(i) {}
  };

  std::unique_ptr<LruAllocator> cache_;
  PoolId itemPool;

  using CCacheType = typename CCacheCreator<CCacheAllocator, Key>::type;
  CCacheType* ccache_{nullptr};

  CacheTestImpl() {
    LruAllocator::Config config;
    config.size = FLAGS_cache_size;
    config.enableCompactCache();

    // 16 million buckets, 1 million locks
    LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                            20 /* locks power */};
    config.accessConfig = accessConfig;

    cache_ = std::make_unique<LruAllocator>(config);

    // 50% goes to item pool and 50% goes to compact cache
    const auto usableMemory = cache_->getCacheMemoryStats().ramCacheSize;
    itemPool = cache_->addPool("item_cache", usableMemory / 2);

    ccache_ = cache_->template addCompactCache<CCacheType>("compact_cache",
                                                           usableMemory / 2);

    // Fill up both caches
    const auto numKeys = FLAGS_num_keys;
    for (uint32_t i = 0; i < numKeys; ++i) {
      Key key{i};
      util::allocateAccessible(*cache_, itemPool, util::castToKey(key), 0);
    }
    for (uint32_t i = 0; i < numKeys; ++i) {
      Key key{i};
      ccache_->set(key);
    }
  }
};

template <size_t KeySize>
void runCacheRW(bool isItemCache) {
  const auto numWritePct = FLAGS_write_percentage;
  const auto numMissPct = FLAGS_miss_percentage;
  const auto numOps = FLAGS_num_ops;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::discrete_distribution<> rwDist({1 - numWritePct, numWritePct});
  std::discrete_distribution<> missDist({1 - numMissPct, numMissPct});

  using CacheTest = CacheTestImpl<KeySize>;
  std::unique_ptr<CacheTest> t;
  BENCHMARK_SUSPEND { t = std::make_unique<CacheTest>(); };

  for (uint32_t i = 0; i < numOps; ++i) {
    typename CacheTest::Key key{getKey(i)};
    if (rwDist(gen) == 0) {
      if (missDist(gen) == 0) {
        if (isItemCache) {
          t->cache_->find(util::castToKey(key));
        } else {
          t->ccache_->get(key);
        }
      } else {
        if (isItemCache) {
          t->cache_->find("this is a miss");
        } else {
          typename CacheTest::Key missKey{std::numeric_limits<uint32_t>::max()};
          t->ccache_->get(missKey);
        }
      }
    } else {
      if (isItemCache) {
        util::allocateAccessible(
            *(t->cache_), t->itemPool, util::castToKey(key), 0);
      } else {
        t->ccache_->set(key);
      }
    }
  }
}

BENCHMARK(ItemCache10) { runCacheRW<10>(true); }
BENCHMARK_RELATIVE(CompactCache10) { runCacheRW<10>(false); }
BENCHMARK(ItemCache32) { runCacheRW<32>(true); }
BENCHMARK_RELATIVE(CompactCache32) { runCacheRW<32>(false); }
BENCHMARK(ItemCache64) { runCacheRW<64>(true); }
BENCHMARK_RELATIVE(CompactCache64) { runCacheRW<64>(false); }
BENCHMARK(ItemCache100) { runCacheRW<100>(true); }
BENCHMARK_RELATIVE(CompactCache100) { runCacheRW<100>(false); }
BENCHMARK(ItemCache200) { runCacheRW<200>(true); }
BENCHMARK_RELATIVE(CompactCache200) { runCacheRW<200>(false); }
BENCHMARK(ItemCache400) { runCacheRW<200>(true); }
BENCHMARK_RELATIVE(CompactCache400) { runCacheRW<200>(false); }

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
}
