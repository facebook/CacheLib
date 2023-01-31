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

/*
clang-format off
Microbenchmarks for variou CacheAllocator operations
Results are at the bottom of this file

Various latency numbers circa 2012
----------------------------------
L1 cache reference                           0.5 ns
Branch mispredict                            5   ns
L2 cache reference                           7   ns                      14x L1 cache
Mutex lock/unlock                           25   ns
Main memory reference                      100   ns                      20x L2 cache, 200x L1 cache
Compress 1K bytes with Zippy             3,000   ns        3 us
Send 1K bytes over 1 Gbps network       10,000   ns       10 us
Read 4K randomly from SSD*             150,000   ns      150 us          ~1GB/sec SSD
Read 1 MB sequentially from memory     250,000   ns      250 us
Round trip within same datacenter      500,000   ns      500 us
Read 1 MB sequentially from SSD*     1,000,000   ns    1,000 us    1 ms  ~1GB/sec SSD, 4X memory
Disk seek                           10,000,000   ns   10,000 us   10 ms  20x datacenter roundtrip
Read 1 MB sequentially from disk    20,000,000   ns   20,000 us   20 ms  80x memory, 20X SSD
Send packet CA->Netherlands->CA    150,000,000   ns  150,000 us  150 ms
clang-format on
*/

#include <folly/Benchmark.h>
#include <folly/BenchmarkUtil.h>
#include <folly/init/Init.h>

#include <chrono>
#include <random>
#include <string>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/benchmarks/BenchmarkUtils.h"
#include "cachelib/common/BytesEqual.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/testing/SeqPoints.h"

namespace facebook {
namespace cachelib {
namespace {
std::unique_ptr<LruAllocator> getCache(unsigned int htPower = 20) {
  LruAllocator::Config config;
  config.setCacheSize(1024 * 1024 * 1024);
  // Hashtable: 1024 ht locks, 1M buckets
  config.setAccessConfig(LruAllocator::AccessConfig{htPower, 10});
  // Allocation Sizes: Min: 64 bytes. Max: 1 MB. Growth factor 108%.
  config.setDefaultAllocSizes(1.08, 1024 * 1024, 64, false);

  // Disable background workers
  config.enablePoolRebalancing({}, std::chrono::seconds{0});
  config.enableItemReaperInBackground(std::chrono::seconds{0});

  auto cache = std::make_unique<LruAllocator>(config);
  cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);
  return cache;
}
} // namespace

void runDifferentHTSizes(int htPower, uint64_t numObjects) {
  constexpr int kNumThreads = 16;
  constexpr uint64_t kLoops = 10'000'000;

  auto cache = getCache();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < numObjects; i++) {
    // Length of key should be 10 bytes
    auto key = folly::sformat("k_{: <8}", i);
    auto hdl = cache->allocate(0, key, 100);
    XCHECK(hdl);
    cache->insertOrReplace(hdl);
    keys.push_back(key);
  }

  navy::SeqPoints sp;
  auto readOps = [&] {
    sp.wait(0);

    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> dist(0, numObjects - 1);
    for (uint64_t loop = 0; loop < kLoops; loop++) {
      const auto& key = keys[dist(gen)];
      auto hdl = cache->peek(key);
      folly::doNotOptimizeAway(hdl);
    }
  };

  // Create readers
  std::vector<std::thread> rs;
  for (int i = 0; i < kNumThreads; i++) {
    rs.push_back(std::thread{readOps});
  }

  {
    Timer t{folly::sformat("Peek - {: <2} HT Power, {: <8} Objects", htPower,
                           numObjects),
            kLoops};
    sp.reached(0); // Start the operations
    for (auto& r : rs) {
      r.join();
    }
  }
}

void runFindMultiThreads(int numThreads,
                         bool isPeek,
                         uint64_t objSize,
                         uint64_t writePct) {
  constexpr uint64_t kObjects = 100'000;
  constexpr uint64_t kLoops = 10'000'000;

  auto cache = getCache();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kObjects; i++) {
    // Length of key should be 10 bytes
    auto key = folly::sformat("k_{: <8}", i);
    auto hdl = cache->allocate(0, key, objSize);
    XCHECK(hdl);
    cache->insertOrReplace(hdl);
    keys.push_back(key);
  }

  navy::SeqPoints sp;
  auto readOps = [&] {
    sp.wait(0);

    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> dist(0, kObjects - 1);
    for (uint64_t loop = 0; loop < kLoops; loop++) {
      const auto& key = keys[dist(gen)];
      auto hdl = isPeek ? cache->peek(key) : cache->find(key);
      folly::doNotOptimizeAway(hdl);
    }
  };
  auto writeOps = [&] {
    sp.wait(0);

    if (writePct == 0) {
      return;
    }

    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> dist(0,
                                                 kObjects * writePct / 100 - 1);
    for (uint64_t loop = 0; loop < kLoops / 10; loop++) {
      const auto& key = keys[dist(gen)];
      auto hdl = cache->allocate(0, key, objSize);
      XCHECK(hdl);
      cache->insertOrReplace(hdl);
      folly::doNotOptimizeAway(hdl);
    }
  };

  // Create readers
  std::vector<std::thread> rs;
  for (int i = 0; i < numThreads; i++) {
    rs.push_back(std::thread{readOps});
  }

  // Create writers
  std::vector<std::thread> ws;
  for (int i = 0; i < 4; i++) {
    ws.push_back(std::thread{writeOps});
  }

  {
    Timer t{folly::sformat("{} - {: <2} Threads, {: <4} Bytes, {: <2}% Write",
                           isPeek ? "Peek" : "Find", numThreads, objSize,
                           writePct),
            kLoops};
    sp.reached(0); // Start the operations
    for (auto& r : rs) {
      r.join();
    }
  }
  for (auto& w : ws) {
    w.join();
  }
}

void runFindMissMultiThreads(int numThreads, bool isPeek) {
  // All lookups in this test are misses
  constexpr uint64_t kObjects = 100'000;
  constexpr uint64_t kLoops = 10'000'000;

  auto cache = getCache();
  std::vector<std::string> keys;
  // Populate a bunch of keys that don't exist in cache
  for (uint64_t i = kObjects; i < 2 * kObjects; i++) {
    // Length of key should be 10 bytes
    auto key = folly::sformat("k_{: <8}", i);
    keys.push_back(key);
  }

  // Fill up cache with some other keys
  for (uint64_t i = 0; i < kObjects; i++) {
    // Length of key should be 10 bytes
    auto key = folly::sformat("k_{: <8}", i);
    auto hdl = cache->allocate(0, key, 100);
    XCHECK(hdl);
    cache->insertOrReplace(hdl);
  }

  navy::SeqPoints sp;
  auto readOps = [&] {
    sp.wait(0);

    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> dist(0, kObjects - 1);
    for (uint64_t loop = 0; loop < kLoops; loop++) {
      const auto& key = keys[dist(gen)];
      auto hdl = isPeek ? cache->peek(key) : cache->find(key);
      folly::doNotOptimizeAway(hdl);
    }
  };
  std::vector<std::thread> rs;
  for (int i = 0; i < numThreads; i++) {
    rs.push_back(std::thread{readOps});
  }

  {
    Timer t{folly::sformat("{} - All Misses - {: <2} Threads",
                           isPeek ? "Peek" : "Find", numThreads),
            kLoops};
    sp.reached(0); // Start the operations
    for (auto& r : rs) {
      r.join();
    }
  }
}

void runAllocateMultiThreads(int numThreads,
                             bool preFillupCache,
                             std::vector<uint32_t> payloadSizes) {
  constexpr uint64_t kLoops = 1;
  constexpr uint64_t kObjects = 100'000;

  auto cache = getCache();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kObjects; i++) {
    // Length of key should be 10 bytes
    auto key = folly::sformat("k_{: <8}", i);
    keys.push_back(key);
  }

  if (preFillupCache) {
    uint64_t i = keys.size();
    std::vector<LruAllocator::WriteHandle> handles;
    while (true) {
      // Length of key should be 10 bytes
      auto key = folly::sformat("k_{: <8}", i);
      auto hdl = cache->allocate(0, key, payloadSizes[i % payloadSizes.size()]);
      if (!hdl) {
        // Cache is full. Stop prefill.
        break;
      }
      cache->insertOrReplace(hdl);
      handles.push_back(std::move(hdl));
      i++;
    }
  }

  navy::SeqPoints sp;
  auto writeOps = [&](uint64_t start, uint64_t end) {
    sp.wait(0);

    for (uint64_t loops = 0; loops < kLoops; loops++) {
      for (uint64_t i = start; i < end; i++) {
        auto& key = keys[i];
        auto hdl =
            cache->allocate(0, key, payloadSizes[i % payloadSizes.size()]);
        XCHECK(hdl);
        cache->insertOrReplace(hdl);
      }
    }
  };

  // Create writers
  std::vector<std::thread> ws;
  uint64_t startIndex = 0;
  uint64_t chunkSize = kObjects / numThreads;
  uint64_t totalItemsPerThread = chunkSize * kLoops;
  for (int i = 0; i < numThreads; i++) {
    ws.push_back(std::thread{writeOps, startIndex, startIndex + chunkSize});
    startIndex += chunkSize;
  }

  {
    Timer t{folly::sformat("Allocate - {} - {: <2} Threads, {: <2} Sizes",
                           preFillupCache ? "Eviction" : "New     ", numThreads,
                           payloadSizes.size()),
            totalItemsPerThread};
    sp.reached(0); // Start the operations
    for (auto& w : ws) {
      w.join();
    }
  }
}
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  printMsg("Benchmark Starting Now");
  printMsg("Becnhmarks (Different HT Sizes)");
  runDifferentHTSizes(12, 1000);
  std::set<int> htPowers{16, 20, 24, 28};
  std::set<uint64_t> numObjects{10'000, 100'000, 1'000'000, 10'000'000};
  for (auto h : htPowers) {
    std::cout << "---------\n";
    for (auto o : numObjects) {
      runDifferentHTSizes(h, o);
    }
  }

  printMsg("Becnhmarks (100K Objects)");
  std::set<uint64_t> threads{1, 4, 8, 16, 32, 64};
  std::set<bool> findOrPeek{true, false};
  for (auto t : threads) {
    std::cout << "---------\n";
    for (auto f : findOrPeek) {
      runFindMissMultiThreads(t, f);
    }
  }

  std::cout << "---------\n";
  std::set<uint64_t> objSizes{0, 100, 1000, 10000};
  for (auto o : objSizes) {
    runFindMultiThreads(32, false, o, 0);
  }

  std::set<uint64_t> writePcts{0, 5, 10, 20};
  for (auto t : threads) {
    for (auto f : findOrPeek) {
      std::cout << "---------\n";
      for (auto w : writePcts) {
        runFindMultiThreads(t, f, 100, w);
      }
    }
  }

  std::set<bool> preFillupCache{true, false};
  std::set<std::vector<uint32_t>> setOfPayloadSizes{
      {5000},
      {1000, 5000, 10000},
      {0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}};
  for (auto t : threads) {
    for (auto p : preFillupCache) {
      std::cout << "---------\n";
      for (auto s : setOfPayloadSizes) {
        runAllocateMultiThreads(t, p, s);
      }
    }
  }
  printMsg("Becnhmarks have completed");
}

/*
clang-format off
Hardware Spec: T1 Skylake

Architecture:        x86_64
CPU op-mode(s):      32-bit, 64-bit
Byte Order:          Little Endian
CPU(s):              36
On-line CPU(s) list: 0-35
Thread(s) per core:  2
Core(s) per socket:  18
Socket(s):           1
NUMA node(s):        1
Vendor ID:           GenuineIntel
CPU family:          6
Model:               85
Model name:          Intel(R) Xeon(R) D-2191A CPU @ 1.60GHz
Stepping:            4
CPU MHz:             1527.578
CPU max MHz:         1601.0000
CPU min MHz:         800.0000
BogoMIPS:            3200.00
Virtualization:      VT-x
L1d cache:           32K
L1i cache:           32K
L2 cache:            1024K
L3 cache:            25344K
NUMA node0 CPU(s):   0-35

-------- Benchmark Starting Now --------------------------------------------------------------------
-------- Becnhmarks (Different HT Sizes) -----------------------------------------------------------
[Peek - 12 HT Power, 1000     Objects                        ] Per-Op: 199   ns, 317   cycles
---------
[Peek - 16 HT Power, 10000    Objects                        ] Per-Op: 202   ns, 323   cycles
[Peek - 16 HT Power, 100000   Objects                        ] Per-Op: 216   ns, 345   cycles
[Peek - 16 HT Power, 1000000  Objects                        ] Per-Op: 414   ns, 660   cycles
[Peek - 16 HT Power, 10000000 Objects                        ] Per-Op: 1120  ns, 1788  cycles
---------
[Peek - 20 HT Power, 10000    Objects                        ] Per-Op: 202   ns, 322   cycles
[Peek - 20 HT Power, 100000   Objects                        ] Per-Op: 216   ns, 345   cycles
[Peek - 20 HT Power, 1000000  Objects                        ] Per-Op: 412   ns, 657   cycles
[Peek - 20 HT Power, 10000000 Objects                        ] Per-Op: 1131  ns, 1806  cycles
---------
[Peek - 24 HT Power, 10000    Objects                        ] Per-Op: 203   ns, 324   cycles
[Peek - 24 HT Power, 100000   Objects                        ] Per-Op: 219   ns, 350   cycles
[Peek - 24 HT Power, 1000000  Objects                        ] Per-Op: 412   ns, 657   cycles
[Peek - 24 HT Power, 10000000 Objects                        ] Per-Op: 1137  ns, 1815  cycles
---------
[Peek - 28 HT Power, 10000    Objects                        ] Per-Op: 216   ns, 345   cycles
[Peek - 28 HT Power, 100000   Objects                        ] Per-Op: 323   ns, 515   cycles
[Peek - 28 HT Power, 1000000  Objects                        ] Per-Op: 417   ns, 666   cycles
[Peek - 28 HT Power, 10000000 Objects                        ] Per-Op: 1123  ns, 1794  cycles
-------- Becnhmarks (100K Objects) -----------------------------------------------------------------
---------
[Find - All Misses - 1  Threads                              ] Per-Op: 101   ns, 162   cycles
[Peek - All Misses - 1  Threads                              ] Per-Op: 86    ns, 138   cycles
---------
[Find - All Misses - 4  Threads                              ] Per-Op: 103   ns, 165   cycles
[Peek - All Misses - 4  Threads                              ] Per-Op: 90    ns, 144   cycles
---------
[Find - All Misses - 8  Threads                              ] Per-Op: 110   ns, 176   cycles
[Peek - All Misses - 8  Threads                              ] Per-Op: 91    ns, 146   cycles
---------
[Find - All Misses - 16 Threads                              ] Per-Op: 114   ns, 182   cycles
[Peek - All Misses - 16 Threads                              ] Per-Op: 94    ns, 151   cycles
---------
[Find - All Misses - 32 Threads                              ] Per-Op: 265   ns, 424   cycles
[Peek - All Misses - 32 Threads                              ] Per-Op: 252   ns, 403   cycles
---------
[Find - All Misses - 64 Threads                              ] Per-Op: 376   ns, 600   cycles
[Peek - All Misses - 64 Threads                              ] Per-Op: 308   ns, 492   cycles
---------
[Find - 32 Threads, 0    Bytes, 0 % Write                    ] Per-Op: 340   ns, 542   cycles
[Find - 32 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 337   ns, 539   cycles
[Find - 32 Threads, 1000 Bytes, 0 % Write                    ] Per-Op: 340   ns, 543   cycles
[Find - 32 Threads, 10000 Bytes, 0 % Write                   ] Per-Op: 359   ns, 574   cycles
---------
[Find - 1  Threads, 100  Bytes, 0 % Write                    ] Per-Op: 207   ns, 331   cycles
[Find - 1  Threads, 100  Bytes, 5 % Write                    ] Per-Op: 295   ns, 471   cycles
[Find - 1  Threads, 100  Bytes, 10% Write                    ] Per-Op: 337   ns, 538   cycles
[Find - 1  Threads, 100  Bytes, 20% Write                    ] Per-Op: 360   ns, 576   cycles
---------
[Peek - 1  Threads, 100  Bytes, 0 % Write                    ] Per-Op: 180   ns, 288   cycles
[Peek - 1  Threads, 100  Bytes, 5 % Write                    ] Per-Op: 221   ns, 353   cycles
[Peek - 1  Threads, 100  Bytes, 10% Write                    ] Per-Op: 208   ns, 333   cycles
[Peek - 1  Threads, 100  Bytes, 20% Write                    ] Per-Op: 207   ns, 330   cycles
---------
[Find - 4  Threads, 100  Bytes, 0 % Write                    ] Per-Op: 217   ns, 347   cycles
[Find - 4  Threads, 100  Bytes, 5 % Write                    ] Per-Op: 363   ns, 579   cycles
[Find - 4  Threads, 100  Bytes, 10% Write                    ] Per-Op: 375   ns, 598   cycles
[Find - 4  Threads, 100  Bytes, 20% Write                    ] Per-Op: 380   ns, 607   cycles
---------
[Peek - 4  Threads, 100  Bytes, 0 % Write                    ] Per-Op: 184   ns, 293   cycles
[Peek - 4  Threads, 100  Bytes, 5 % Write                    ] Per-Op: 280   ns, 448   cycles
[Peek - 4  Threads, 100  Bytes, 10% Write                    ] Per-Op: 293   ns, 467   cycles
[Peek - 4  Threads, 100  Bytes, 20% Write                    ] Per-Op: 316   ns, 505   cycles
---------
[Find - 8  Threads, 100  Bytes, 0 % Write                    ] Per-Op: 255   ns, 407   cycles
[Find - 8  Threads, 100  Bytes, 5 % Write                    ] Per-Op: 402   ns, 642   cycles
[Find - 8  Threads, 100  Bytes, 10% Write                    ] Per-Op: 392   ns, 626   cycles
[Find - 8  Threads, 100  Bytes, 20% Write                    ] Per-Op: 384   ns, 614   cycles
---------
[Peek - 8  Threads, 100  Bytes, 0 % Write                    ] Per-Op: 198   ns, 316   cycles
[Peek - 8  Threads, 100  Bytes, 5 % Write                    ] Per-Op: 325   ns, 519   cycles
[Peek - 8  Threads, 100  Bytes, 10% Write                    ] Per-Op: 317   ns, 506   cycles
[Peek - 8  Threads, 100  Bytes, 20% Write                    ] Per-Op: 323   ns, 516   cycles
---------
[Find - 16 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 256   ns, 410   cycles
[Find - 16 Threads, 100  Bytes, 5 % Write                    ] Per-Op: 453   ns, 723   cycles
[Find - 16 Threads, 100  Bytes, 10% Write                    ] Per-Op: 459   ns, 733   cycles
[Find - 16 Threads, 100  Bytes, 20% Write                    ] Per-Op: 425   ns, 678   cycles
---------
[Peek - 16 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 236   ns, 377   cycles
[Peek - 16 Threads, 100  Bytes, 5 % Write                    ] Per-Op: 367   ns, 586   cycles
[Peek - 16 Threads, 100  Bytes, 10% Write                    ] Per-Op: 382   ns, 610   cycles
[Peek - 16 Threads, 100  Bytes, 20% Write                    ] Per-Op: 393   ns, 628   cycles
---------
[Find - 32 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 401   ns, 641   cycles
[Find - 32 Threads, 100  Bytes, 5 % Write                    ] Per-Op: 516   ns, 824   cycles
[Find - 32 Threads, 100  Bytes, 10% Write                    ] Per-Op: 468   ns, 747   cycles
[Find - 32 Threads, 100  Bytes, 20% Write                    ] Per-Op: 456   ns, 728   cycles
---------
[Peek - 32 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 298   ns, 476   cycles
[Peek - 32 Threads, 100  Bytes, 5 % Write                    ] Per-Op: 369   ns, 590   cycles
[Peek - 32 Threads, 100  Bytes, 10% Write                    ] Per-Op: 411   ns, 657   cycles
[Peek - 32 Threads, 100  Bytes, 20% Write                    ] Per-Op: 396   ns, 632   cycles
---------
[Find - 64 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 642   ns, 1026  cycles
[Find - 64 Threads, 100  Bytes, 5 % Write                    ] Per-Op: 1087  ns, 1736  cycles
[Find - 64 Threads, 100  Bytes, 10% Write                    ] Per-Op: 1092  ns, 1744  cycles
[Find - 64 Threads, 100  Bytes, 20% Write                    ] Per-Op: 1080  ns, 1724  cycles
---------
[Peek - 64 Threads, 100  Bytes, 0 % Write                    ] Per-Op: 549   ns, 876   cycles
[Peek - 64 Threads, 100  Bytes, 5 % Write                    ] Per-Op: 1027  ns, 1639  cycles
[Peek - 64 Threads, 100  Bytes, 10% Write                    ] Per-Op: 1008  ns, 1610  cycles
[Peek - 64 Threads, 100  Bytes, 20% Write                    ] Per-Op: 1018  ns, 1626  cycles
---------
[Allocate - New      - 1  Threads, 11 Sizes                  ] Per-Op: 1772  ns, 2830  cycles
[Allocate - New      - 1  Threads, 3  Sizes                  ] Per-Op: 1677  ns, 2677  cycles
[Allocate - New      - 1  Threads, 1  Sizes                  ] Per-Op: 2062  ns, 3291  cycles
---------
[Allocate - Eviction - 1  Threads, 11 Sizes                  ] Per-Op: 982   ns, 1569  cycles
[Allocate - Eviction - 1  Threads, 3  Sizes                  ] Per-Op: 1068  ns, 1705  cycles
[Allocate - Eviction - 1  Threads, 1  Sizes                  ] Per-Op: 1039  ns, 1659  cycles
---------
[Allocate - New      - 4  Threads, 11 Sizes                  ] Per-Op: 2680  ns, 4279  cycles
[Allocate - New      - 4  Threads, 3  Sizes                  ] Per-Op: 2845  ns, 4542  cycles
[Allocate - New      - 4  Threads, 1  Sizes                  ] Per-Op: 3376  ns, 5389  cycles
---------
[Allocate - Eviction - 4  Threads, 11 Sizes                  ] Per-Op: 1989  ns, 3176  cycles
[Allocate - Eviction - 4  Threads, 3  Sizes                  ] Per-Op: 2564  ns, 4093  cycles
[Allocate - Eviction - 4  Threads, 1  Sizes                  ] Per-Op: 4412  ns, 7043  cycles
---------
[Allocate - New      - 8  Threads, 11 Sizes                  ] Per-Op: 3032  ns, 4840  cycles
[Allocate - New      - 8  Threads, 3  Sizes                  ] Per-Op: 3339  ns, 5330  cycles
[Allocate - New      - 8  Threads, 1  Sizes                  ] Per-Op: 4523  ns, 7220  cycles
---------
[Allocate - Eviction - 8  Threads, 11 Sizes                  ] Per-Op: 2524  ns, 4028  cycles
[Allocate - Eviction - 8  Threads, 3  Sizes                  ] Per-Op: 3836  ns, 6124  cycles
[Allocate - Eviction - 8  Threads, 1  Sizes                  ] Per-Op: 8916  ns, 14232 cycles
---------
[Allocate - New      - 16 Threads, 11 Sizes                  ] Per-Op: 4106  ns, 6554  cycles
[Allocate - New      - 16 Threads, 3  Sizes                  ] Per-Op: 4643  ns, 7412  cycles
[Allocate - New      - 16 Threads, 1  Sizes                  ] Per-Op: 7628  ns, 12176 cycles
---------
[Allocate - Eviction - 16 Threads, 11 Sizes                  ] Per-Op: 3686  ns, 5884  cycles
[Allocate - Eviction - 16 Threads, 3  Sizes                  ] Per-Op: 7184  ns, 11467 cycles
[Allocate - Eviction - 16 Threads, 1  Sizes                  ] Per-Op: 18559 ns, 29625 cycles
---------
[Allocate - New      - 32 Threads, 11 Sizes                  ] Per-Op: 8358  ns, 13341 cycles
[Allocate - New      - 32 Threads, 3  Sizes                  ] Per-Op: 9577  ns, 15287 cycles
[Allocate - New      - 32 Threads, 1  Sizes                  ] Per-Op: 16754 ns, 26743 cycles
---------
[Allocate - Eviction - 32 Threads, 11 Sizes                  ] Per-Op: 6055  ns, 9666  cycles
[Allocate - Eviction - 32 Threads, 3  Sizes                  ] Per-Op: 14372 ns, 22942 cycles
[Allocate - Eviction - 32 Threads, 1  Sizes                  ] Per-Op: 37691 ns, 60163 cycles
---------
[Allocate - New      - 64 Threads, 11 Sizes                  ] Per-Op: 17692 ns, 28242 cycles
[Allocate - New      - 64 Threads, 3  Sizes                  ] Per-Op: 20727 ns, 33086 cycles
[Allocate - New      - 64 Threads, 1  Sizes                  ] Per-Op: 35300 ns, 56348 cycles
---------
[Allocate - Eviction - 64 Threads, 11 Sizes                  ] Per-Op: 26014 ns, 41524 cycles
[Allocate - Eviction - 64 Threads, 3  Sizes                  ] Per-Op: 34119 ns, 54463 cycles
[Allocate - Eviction - 64 Threads, 1  Sizes                  ] Per-Op: 76896 ns, 122744 cycles
-------- Becnhmarks have completed -----------------------------------------------------------------
clang-format on
*/
