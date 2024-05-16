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

/**
 * Performance Results As of 4/26/2024. Running on dev6_xlarge (166 cores).
 */

/**
 * CacheLib is configured with (threads / threads ) = 1 pool.
 * ============================================================================
 * [...]marks/CachelibContentionBenchmark.cpp     relative  time/iter   iters/s
 * ============================================================================
 * Find(32)                                                    15.39s    64.99m
 * FindThreadLocal(32)                             90.282%     17.04s    58.67m
 * Find(64)                                                     7.96s   125.66m
 * FindThreadLocal(64)                             99.529%      8.00s   125.07m
 * Find(128)                                                    4.63s   215.91m
 * FindThreadLocal(128)                            122.13%      3.79s   263.68m
 */

/**
 * CacheLib is configured with (threads / 16) pools.
 * ============================================================================
 * [...]marks/CachelibContentionBenchmark.cpp     relative  time/iter   iters/s
 * ============================================================================
 * Find(32)                                                    15.22s    65.70m
 * FindThreadLocal(32)                             92.523%     16.45s    60.79m
 * Find(64)                                                     7.62s   131.30m
 * FindThreadLocal(64)                             99.331%      7.67s   130.42m
 * Find(128)                                                    4.01s   249.14m
 * FindThreadLocal(128)                            108.93%      3.68s   271.40m
 * Insert(32)                                                 3.38min     4.93m
 * InsertThreadLocal(32)                           3869.8%      5.24s   190.85m
 * Insert(64)                                                 3.70min     4.51m
 * InsertThreadLocal(64)                           4139.7%      5.36s   186.67m
 * Insert(128)                                                4.00min     4.17m
 * InsertThreadLocal(128)                          3306.9%      7.26s   137.83m
 */

/**
 * CacheLib is configured with (threads / 4) pools.
 * ============================================================================
 * [...]marks/CachelibContentionBenchmark.cpp     relative  time/iter   iters/s
 * ============================================================================
 * Find(32)                                                    14.33s    69.78m
 * FindThreadLocal(32)                             87.268%     16.42s    60.89m
 * Find(64)                                                     6.84s   146.12m
 * FindThreadLocal(64)                             90.345%      7.58s   132.01m
 * Find(128)                                                    3.59s   278.42m
 * FindThreadLocal(128)                            96.015%      3.74s   267.33m
 * Insert(32)                                                 1.16min    14.35m
 * InsertThreadLocal(32)                           1290.3%      5.40s   185.13m
 * Insert(64)                                                 1.19min    14.06m
 * InsertThreadLocal(64)                           1266.0%      5.62s   178.01m
 * Insert(128)                                                1.25min    13.33m
 * InsertThreadLocal(128)                          1018.8%      7.36s   135.80m
 */

/**
 * CacheLib is configured with (threads / 1) pools.
 * ============================================================================
 * [...]marks/CachelibContentionBenchmark.cpp     relative  time/iter   iters/s
 * ============================================================================
 * Find(32)                                                    14.33s    69.78m
 * FindThreadLocal(32)                             87.268%     16.42s    60.89m
 * Find(64)                                                     6.84s   146.12m
 * FindThreadLocal(64)                             90.345%      7.58s   132.01m
 * Find(128)                                                    3.59s   278.42m
 * FindThreadLocal(128)                            96.015%      3.74s   267.33m
 * Insert(32)                                                  35.30s    28.33m
 * InsertThreadLocal(32)                           700.52%      5.04s   198.44m
 * Insert(64)                                                  36.92s    27.08m
 * InsertThreadLocal(64)                           686.74%      5.38s   185.99m
 */

#include <folly/Benchmark.h>
#include <folly/container/EvictingCacheMap.h>
#include <folly/init/Init.h>

#include <cmath>
#include <numeric>
#include <random>

#include "cachelib/allocator/CacheAllocator.h"

const size_t kLookupOps = 1e7;             // 10m
const size_t kInsertOps = 1e5;             // 100k
const size_t kNumLoops = 100;              // Number of loops to run lookups
const size_t kNumItems = 1024 * 1024;      // 1 million
const double kAlpha = 1.0;                 // Shapes zipf distribution
const int kZipfSeed = 111;                 // Seed for Zipf distribution
const size_t kCacheLibThreadsPerPool = 16; // Parallelism

class ZipfDistribution {
 public:
  ZipfDistribution(int seed, double min, double max, double alpha)
      : gen_(seed), dis_(min, max), alpha_(alpha) {}

  size_t generate() {
    double x = dis_(gen_);
    double y = std::pow(x, 1.0 / alpha_);
    return static_cast<size_t>(y);
  }

 private:
  std::mt19937 gen_;
  std::uniform_real_distribution<double> dis_;
  double alpha_;
};

void Find(size_t iters, size_t numThreads) {
  using namespace facebook::cachelib;

  const size_t numPools = std::max(1ul, numThreads / kCacheLibThreadsPerPool);
  std::unique_ptr<LruAllocator> cache;
  std::vector<std::vector<size_t>> lookupKeyIndices;

  BENCHMARK_SUSPEND {
    LruAllocator::Config config;
    config.setCacheSize(500ul * 1024ul * 1024ul); // 500 MB

    // 16 million buckets, 1 million locks
    LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                            20 /* locks power */};
    config.setAccessConfig(accessConfig);
    cache = std::make_unique<LruAllocator>(config);

    const auto ramCacheSize = cache->getCacheMemoryStats().ramCacheSize;
    const auto perPoolSize = ramCacheSize / numPools;
    for (size_t i = 0; i < numPools; ++i) {
      cache->addPool(folly::sformat("pid_{}", i), perPoolSize);
    }

    for (size_t i = 0; i < kNumItems; ++i) {
      auto key = folly::sformat("key_{}", i);
      auto handle = cache->allocate(static_cast<PoolId>(i % numPools), key, 64);
      if (handle) {
        cache->insertOrReplace(handle);
      } else {
        throw std::runtime_error(
            folly::sformat("allocation cannot fail! key: {}", key));
      }
    }

    ZipfDistribution zipf(kZipfSeed, 0, kNumItems, kAlpha);
    for (size_t i = 0; i < numThreads; ++i) {
      lookupKeyIndices.push_back(std::vector<size_t>());
    }
    for (size_t i = 0; i < kLookupOps; ++i) {
      size_t keyIndex = zipf.generate();
      lookupKeyIndices[keyIndex % numThreads].push_back(keyIndex);
    }
  }

  auto runLookups = [&](size_t threadId) {
    auto& lookupKeys = lookupKeyIndices[threadId];
    for (size_t loop = 0; loop < kNumLoops; ++loop) {
      for (size_t i = 0; i < lookupKeys.size(); ++i) {
        auto key = folly::sformat("key_{}", lookupKeys[i]);
        auto it = cache->find(key);
        folly::doNotOptimizeAway(it);
      }
    }
  };

  while (iters--) {
    std::vector<std::thread> ts;
    for (size_t i = 0; i < numThreads; ++i) {
      ts.push_back(std::thread(runLookups, i));
    }
    for (auto& t : ts) {
      t.join();
    }
  }

  BENCHMARK_SUSPEND { cache.reset(); }
}

void FindThreadLocal(size_t iters, size_t numThreads) {
  using CacheMap = folly::EvictingCacheMap<std::string, std::string>;

  std::vector<std::unique_ptr<CacheMap>> caches;
  std::vector<std::vector<size_t>> lookupKeyIndices;

  BENCHMARK_SUSPEND {
    for (size_t i = 0; i < numThreads; ++i) {
      auto cache =
          std::make_unique<CacheMap>(kNumItems / numThreads /* maxSize */);
      caches.push_back(std::move(cache));

      lookupKeyIndices.push_back(std::vector<size_t>());
    }

    for (size_t i = 0; i < kNumItems; ++i) {
      auto key = folly::sformat("key_{}", i);
      auto value = "dummy_value_123456789012345678901234567890";
      caches[i % numThreads]->insert(key, std::move(value));
    }

    ZipfDistribution zipf(kZipfSeed, 0, kNumItems, kAlpha);
    for (size_t i = 0; i < kLookupOps; ++i) {
      size_t keyIndex = zipf.generate();
      lookupKeyIndices[keyIndex % numThreads].push_back(keyIndex);
    }
  }

  auto runLookups = [&](size_t threadId) {
    auto& cache = caches[threadId];
    auto& lookupKeys = lookupKeyIndices[threadId];
    for (size_t loop = 0; loop < kNumLoops; ++loop) {
      for (size_t i = 0; i < lookupKeys.size(); ++i) {
        auto key = folly::sformat("key_{}", lookupKeys[i]);
        auto it = cache->find(key);
        if (it == cache->end()) {
          throw std::runtime_error(folly::sformat(
              "key not found in EvictingCacheMap: {}, threadId: {}", key,
              threadId));
        }
        folly::doNotOptimizeAway(it);
      }
    }
  };

  while (iters--) {
    std::vector<std::thread> ts;
    for (size_t i = 0; i < numThreads; ++i) {
      ts.push_back(std::thread(runLookups, i));
    }
    for (auto& t : ts) {
      t.join();
    }
  }

  BENCHMARK_SUSPEND {
    for (auto& c : caches) {
      c->clear();
      c.reset();
    }
  }
}

void Insert(size_t iters, size_t numThreads) {
  using namespace facebook::cachelib;

  const size_t numPools = std::max(1ul, numThreads / kCacheLibThreadsPerPool);
  std::unique_ptr<LruAllocator> cache;

  BENCHMARK_SUSPEND {
    LruAllocator::Config config;
    config.setCacheSize(500ul * 1024ul * 1024ul); // 500 MB

    // 16 million buckets, 1 million locks
    LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                            20 /* locks power */};
    config.setAccessConfig(accessConfig);
    cache = std::make_unique<LruAllocator>(config);

    const auto ramCacheSize = cache->getCacheMemoryStats().ramCacheSize;
    const auto perPoolSize = ramCacheSize / numPools;
    for (size_t i = 0; i < numPools; ++i) {
      cache->addPool(folly::sformat("pid_{}", i), perPoolSize);
    }

    // Initially allocate a lot more items to ensure we fill up the cache
    for (size_t i = 0; i < kNumItems * 10; ++i) {
      auto key = folly::sformat("key_{}", i);
      auto handle = cache->allocate(static_cast<PoolId>(i % numPools), key, 64);
      if (handle) {
        cache->insertOrReplace(handle);
      } else {
        throw std::runtime_error(
            folly::sformat("allocation cannot fail! key: {}", key));
      }
    }
  }

  auto runInserts = [&](size_t /* threadId */) {
    for (size_t loop = 0; loop < kNumLoops; ++loop) {
      for (size_t i = 0; i < kInsertOps; ++i) {
        auto key = folly::sformat("key_{}_{}", loop, i);
        auto handle =
            cache->allocate(static_cast<PoolId>(i % numPools), key, 64);
        if (handle) {
          cache->insertOrReplace(handle);
        } else {
          throw std::runtime_error(
              folly::sformat("allocation cannot fail! key: {}", key));
        }
      }
    }
  };

  while (iters--) {
    std::vector<std::thread> ts;
    for (size_t i = 0; i < numThreads; ++i) {
      ts.push_back(std::thread(runInserts, i));
    }
    for (auto& t : ts) {
      t.join();
    }
  }

  BENCHMARK_SUSPEND { cache.reset(); }
}

void InsertThreadLocal(size_t iters, size_t numThreads) {
  using CacheMap = folly::EvictingCacheMap<std::string, std::string>;

  std::vector<std::unique_ptr<CacheMap>> caches;

  BENCHMARK_SUSPEND {
    for (size_t i = 0; i < numThreads; ++i) {
      auto cache =
          std::make_unique<CacheMap>(kNumItems / numThreads /* maxSize */);
      caches.push_back(std::move(cache));
    }

    for (size_t i = 0; i < kNumItems; ++i) {
      auto key = folly::sformat("key_{}", i);
      auto value = "dummy_value_123456789012345678901234567890";
      caches[i % numThreads]->insert(key, std::move(value));
    }
  }

  auto runInserts = [&](size_t threadId) {
    auto& cache = caches[threadId];
    for (size_t loop = 0; loop < kNumLoops; ++loop) {
      for (size_t i = 0; i < kInsertOps; ++i) {
        auto key = folly::sformat("key_{}_{}", loop, i);
        auto value = "dummy_value_123456789012345678901234567890";
        cache->insert(key, std::move(value));
      }
    }
  };

  while (iters--) {
    std::vector<std::thread> ts;
    for (size_t i = 0; i < numThreads; ++i) {
      ts.push_back(std::thread(runInserts, i));
    }
    for (auto& t : ts) {
      t.join();
    }
  }

  BENCHMARK_SUSPEND {
    for (auto& c : caches) {
      c->clear();
      c.reset();
    }
  }
}

BENCHMARK_PARAM(Find, 32)
BENCHMARK_RELATIVE_PARAM(FindThreadLocal, 32)
BENCHMARK_PARAM(Find, 64)
BENCHMARK_RELATIVE_PARAM(FindThreadLocal, 64)
BENCHMARK_PARAM(Find, 128)
BENCHMARK_RELATIVE_PARAM(FindThreadLocal, 128)

BENCHMARK_PARAM(Insert, 32)
BENCHMARK_RELATIVE_PARAM(InsertThreadLocal, 32)
BENCHMARK_PARAM(Insert, 64)
BENCHMARK_RELATIVE_PARAM(InsertThreadLocal, 64)
BENCHMARK_PARAM(Insert, 128)
BENCHMARK_RELATIVE_PARAM(InsertThreadLocal, 128)

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();

  return 0;
}
