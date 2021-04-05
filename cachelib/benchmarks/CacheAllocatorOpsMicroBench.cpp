/*
clang-format off
Microbenchmarks for variou small operations and CacheAllocator operations
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
#include <folly/init/Init.h>

#include <chrono>
#include <random>
#include <string>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/BytesEqual.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/testing/SeqPoints.h"
#include "folly/BenchmarkUtil.h"

namespace facebook {
namespace cachelib {
namespace {
class Timer {
 public:
  explicit Timer(std::string name, uint64_t ops)
      : name_{std::move(name)}, ops_{ops} {
    startTime_ = std::chrono::system_clock::now();
    startCycles_ = __rdtsc();
  }
  ~Timer() {
    endTime_ = std::chrono::system_clock::now();
    endCycles_ = __rdtsc();

    std::chrono::nanoseconds durationTime = endTime_ - startTime_;
    uint64_t durationCycles = endCycles_ - startCycles_;
    std::cout << folly::sformat("[{: <40}] Per-Op: {: <5} ns, {: <5} cycles",
                                name_, durationTime.count() / ops_,
                                durationCycles / ops_)
              << std::endl;
  }

 private:
  const std::string name_;
  const uint64_t ops_;
  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  uint64_t startCycles_;
  uint64_t endCycles_;
};

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
  cache->addPool("default", cache->getCacheMemoryStats().cacheSize);
  return cache;
}
} // namespace

void randomGenCost() {
  constexpr uint64_t kOps = 10'000'000;
  std::mt19937 gen;
  std::uniform_int_distribution<uint64_t> dist(0, 10000);

  {
    Timer t{"Random Number", kOps};
    for (uint64_t i = 0; i < kOps; i++) {
      auto num = dist(gen);
      folly::doNotOptimizeAway(num);
    }
  }
}

void takeTimeStamp() {
  constexpr uint64_t kOps = 10'000'000;
  {
    Timer t{"Timestamp", kOps};
    for (uint64_t i = 0; i < kOps; i++) {
      auto now = std::chrono::system_clock::now();
      folly::doNotOptimizeAway(now);
    }
  }
}

void takeCpuCycles() {
  constexpr uint64_t kOps = 10'000'000;
  {
    Timer t{"CPU Cycles", kOps};
    for (uint64_t i = 0; i < kOps; i++) {
      uint64_t tsc = __rdtsc();
      folly::doNotOptimizeAway(tsc);
    }
  }
}

void percentileStatsAdd() {
  constexpr uint64_t kOps = 10'000'000;
  util::PercentileStats stats;
  {
    Timer t{"PercentileStats Add", kOps};
    for (uint64_t i = 0; i < kOps; i++) {
      double d = 1.2345;
      folly::doNotOptimizeAway(d);
      stats.trackValue(d);
    }
  }
}

void atomicCounterAdd() {
  constexpr uint64_t kThreads = 32;
  constexpr uint64_t kOps = 10'000'000;
  AtomicCounter atomicCounter;
  navy::SeqPoints sp;
  auto addStats = [&]() {
    sp.wait(0);
    for (uint64_t j = 0; j < kOps; j++) {
      atomicCounter.inc();
    }
  };

  std::vector<std::thread> ts;
  for (uint64_t i = 0; i < kThreads; i++) {
    ts.push_back(std::thread{addStats});
  }

  {
    Timer timer{"AtomicCounter Add", kOps};
    sp.reached(0);
    for (auto& t : ts) {
      t.join();
    }
  }
}

void tlCounterAdd() {
  constexpr uint64_t kThreads = 32;
  constexpr uint64_t kOps = 10'000'000;
  TLCounter tlCounter;
  navy::SeqPoints sp;
  auto addStats = [&]() {
    sp.wait(0);
    for (uint64_t j = 0; j < kOps; j++) {
      tlCounter.inc();
    }
  };

  std::vector<std::thread> ts;
  for (uint64_t i = 0; i < kThreads; i++) {
    ts.push_back(std::thread{addStats});
  }

  {
    Timer timer{"TLCounter Add", kOps};
    sp.reached(0);
    for (auto& t : ts) {
      t.join();
    }
  }
}

void createSmallString() {
  // Just a simple test to figure out cost of creating a small string
  constexpr uint64_t kOps = 10'000'000;
  {
    Timer t{"Create Small String", kOps};
    for (uint64_t i = 0; i < kOps; i++) {
      // Read from the vector of strings (first memory load)
      auto key = folly::sformat("{: <10}", i);
      folly::doNotOptimizeAway(key);
    }
  }
}

void compareString(int len) {
  // Just a simple test to figure out cost of reading a string
  constexpr uint64_t kOps = 10'000'000;
  constexpr uint64_t kObjects = 100'000;
  std::vector<std::string> keys;
  const std::string keyCopy(len, 'a');
  for (uint64_t i = 0; i < kObjects; i++) {
    keys.push_back(keyCopy);
  }

  std::mt19937 gen;
  std::uniform_int_distribution<int> dist(0, kObjects - 1);
  {
    Timer t{folly::sformat("Read String - {: <3}", len), kOps};
    for (uint64_t i = 0; i < kOps; i++) {
      // Read from the vector of strings (first memory load)
      auto& k1 = keys[dist(gen)];
      auto& k2 = keys[dist(gen)];
      auto equal = bytesEqual(k1.data(), k2.data(), k1.size());
      folly::doNotOptimizeAway(equal);
    }
  }
}

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
    std::vector<LruAllocator::ItemHandle> handles;
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

  std::cout << "----------- Benchmark various small operations --------\n";
  randomGenCost();
  takeTimeStamp();
  takeCpuCycles();
  percentileStatsAdd();
  atomicCounterAdd();
  tlCounterAdd();
  createSmallString();
  compareString(1);
  compareString(10);
  compareString(100);

  std::cout << "----------- Becnhmarks (Different HT Sizes) -----------\n";
  runDifferentHTSizes(12, 1000);
  std::set<int> htPowers{16, 20, 24, 28};
  std::set<uint64_t> numObjects{10'000, 100'000, 1'000'000, 10'000'000};
  for (auto h : htPowers) {
    std::cout << "---------\n";
    for (auto o : numObjects) {
      runDifferentHTSizes(h, o);
    }
  }

  std::cout << "----------- Becnhmarks (100K Objects) -----------------\n";
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
  std::cout << "----------- Becnhmarks have completed -----------------\n";
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

----------- Cost of various small operations ----------
[Random Number                           ] Per-Op: 18    ns, 29    cycles
[Timestamp                               ] Per-Op: 24    ns, 39    cycles
[CPU Cycles                              ] Per-Op: 9     ns, 14    cycles
[PercentileStats Add                     ] Per-Op: 86    ns, 137   cycles
[AtomicCounter Add                       ] Per-Op: 853   ns, 1362  cycles
[TLCounter Add                           ] Per-Op: 4     ns, 6     cycles
[Create Small String                     ] Per-Op: 78    ns, 125   cycles
[Read String - 1                         ] Per-Op: 44    ns, 71    cycles
[Read String - 10                        ] Per-Op: 44    ns, 70    cycles
[Read String - 100                       ] Per-Op: 108   ns, 173   cycles
----------- Becnhmarks (Different HT Sizes) -----------
[Peek - 12 HT Power, 1000     Objects    ] Per-Op: 298   ns, 476   cycles
---------
[Peek - 16 HT Power, 10000    Objects    ] Per-Op: 297   ns, 474   cycles
[Peek - 16 HT Power, 100000   Objects    ] Per-Op: 214   ns, 342   cycles
[Peek - 16 HT Power, 1000000  Objects    ] Per-Op: 418   ns, 668   cycles
[Peek - 16 HT Power, 10000000 Objects    ] Per-Op: 1117  ns, 1783  cycles
---------
[Peek - 20 HT Power, 10000    Objects    ] Per-Op: 206   ns, 329   cycles
[Peek - 20 HT Power, 100000   Objects    ] Per-Op: 220   ns, 352   cycles
[Peek - 20 HT Power, 1000000  Objects    ] Per-Op: 428   ns, 684   cycles
[Peek - 20 HT Power, 10000000 Objects    ] Per-Op: 1116  ns, 1782  cycles
---------
[Peek - 24 HT Power, 10000    Objects    ] Per-Op: 208   ns, 332   cycles
[Peek - 24 HT Power, 100000   Objects    ] Per-Op: 215   ns, 343   cycles
[Peek - 24 HT Power, 1000000  Objects    ] Per-Op: 414   ns, 660   cycles
[Peek - 24 HT Power, 10000000 Objects    ] Per-Op: 1135  ns, 1812  cycles
---------
[Peek - 28 HT Power, 10000    Objects    ] Per-Op: 205   ns, 328   cycles
[Peek - 28 HT Power, 100000   Objects    ] Per-Op: 218   ns, 348   cycles
[Peek - 28 HT Power, 1000000  Objects    ] Per-Op: 414   ns, 662   cycles
[Peek - 28 HT Power, 10000000 Objects    ] Per-Op: 1113  ns, 1777  cycles
----------- Becnhmarks (100K Objects) -----------------
---------
[Find - All Misses - 1  Threads          ] Per-Op: 97    ns, 156   cycles
[Peek - All Misses - 1  Threads          ] Per-Op: 86    ns, 138   cycles
---------
[Find - All Misses - 4  Threads          ] Per-Op: 108   ns, 172   cycles
[Peek - All Misses - 4  Threads          ] Per-Op: 87    ns, 139   cycles
---------
[Find - All Misses - 8  Threads          ] Per-Op: 111   ns, 177   cycles
[Peek - All Misses - 8  Threads          ] Per-Op: 100   ns, 160   cycles
---------
[Find - All Misses - 16 Threads          ] Per-Op: 217   ns, 347   cycles
[Peek - All Misses - 16 Threads          ] Per-Op: 215   ns, 343   cycles
---------
[Find - All Misses - 32 Threads          ] Per-Op: 166   ns, 265   cycles
[Peek - All Misses - 32 Threads          ] Per-Op: 257   ns, 410   cycles
---------
[Find - All Misses - 64 Threads          ] Per-Op: 344   ns, 550   cycles
[Peek - All Misses - 64 Threads          ] Per-Op: 307   ns, 491   cycles
---------
[Find - 32 Threads, 0    Bytes, 0 % Write] Per-Op: 413   ns, 659   cycles
[Find - 32 Threads, 100  Bytes, 0 % Write] Per-Op: 336   ns, 537   cycles
[Find - 32 Threads, 1000 Bytes, 0 % Write] Per-Op: 340   ns, 544   cycles
[Find - 32 Threads, 10000 Bytes, 0 % Write] Per-Op: 361   ns, 577   cycles
---------
[Find - 1  Threads, 0 % Write            ] Per-Op: 242   ns, 387   cycles
[Find - 1  Threads, 5 % Write            ] Per-Op: 303   ns, 483   cycles
[Find - 1  Threads, 10% Write            ] Per-Op: 324   ns, 518   cycles
[Find - 1  Threads, 20% Write            ] Per-Op: 360   ns, 574   cycles
---------
[Peek - 1  Threads, 0 % Write            ] Per-Op: 195   ns, 312   cycles
[Peek - 1  Threads, 5 % Write            ] Per-Op: 215   ns, 343   cycles
[Peek - 1  Threads, 10% Write            ] Per-Op: 211   ns, 337   cycles
[Peek - 1  Threads, 20% Write            ] Per-Op: 207   ns, 331   cycles
---------
[Find - 4  Threads, 0 % Write            ] Per-Op: 290   ns, 463   cycles
[Find - 4  Threads, 5 % Write            ] Per-Op: 369   ns, 589   cycles
[Find - 4  Threads, 10% Write            ] Per-Op: 373   ns, 596   cycles
[Find - 4  Threads, 20% Write            ] Per-Op: 384   ns, 614   cycles
---------
[Peek - 4  Threads, 0 % Write            ] Per-Op: 184   ns, 295   cycles
[Peek - 4  Threads, 5 % Write            ] Per-Op: 280   ns, 448   cycles
[Peek - 4  Threads, 10% Write            ] Per-Op: 281   ns, 449   cycles
[Peek - 4  Threads, 20% Write            ] Per-Op: 280   ns, 448   cycles
---------
[Find - 8  Threads, 0 % Write            ] Per-Op: 340   ns, 544   cycles
[Find - 8  Threads, 5 % Write            ] Per-Op: 390   ns, 623   cycles
[Find - 8  Threads, 10% Write            ] Per-Op: 390   ns, 623   cycles
[Find - 8  Threads, 20% Write            ] Per-Op: 392   ns, 626   cycles
---------
[Peek - 8  Threads, 0 % Write            ] Per-Op: 198   ns, 316   cycles
[Peek - 8  Threads, 5 % Write            ] Per-Op: 313   ns, 500   cycles
[Peek - 8  Threads, 10% Write            ] Per-Op: 317   ns, 507   cycles
[Peek - 8  Threads, 20% Write            ] Per-Op: 334   ns, 533   cycles
---------
[Find - 16 Threads, 0 % Write            ] Per-Op: 276   ns, 441   cycles
[Find - 16 Threads, 5 % Write            ] Per-Op: 453   ns, 723   cycles
[Find - 16 Threads, 10% Write            ] Per-Op: 456   ns, 728   cycles
[Find - 16 Threads, 20% Write            ] Per-Op: 418   ns, 668   cycles
---------
[Peek - 16 Threads, 0 % Write            ] Per-Op: 212   ns, 339   cycles
[Peek - 16 Threads, 5 % Write            ] Per-Op: 376   ns, 601   cycles
[Peek - 16 Threads, 10% Write            ] Per-Op: 387   ns, 618   cycles
[Peek - 16 Threads, 20% Write            ] Per-Op: 393   ns, 628   cycles
---------
[Find - 32 Threads, 0 % Write            ] Per-Op: 336   ns, 537   cycles
[Find - 32 Threads, 5 % Write            ] Per-Op: 537   ns, 858   cycles
[Find - 32 Threads, 10% Write            ] Per-Op: 534   ns, 853   cycles
[Find - 32 Threads, 20% Write            ] Per-Op: 492   ns, 786   cycles
---------
[Peek - 32 Threads, 0 % Write            ] Per-Op: 302   ns, 483   cycles
[Peek - 32 Threads, 5 % Write            ] Per-Op: 404   ns, 645   cycles
[Peek - 32 Threads, 10% Write            ] Per-Op: 334   ns, 533   cycles
[Peek - 32 Threads, 20% Write            ] Per-Op: 340   ns, 543   cycles
---------
[Find - 64 Threads, 0 % Write            ] Per-Op: 645   ns, 1030  cycles
[Find - 64 Threads, 5 % Write            ] Per-Op: 1066  ns, 1702  cycles
[Find - 64 Threads, 10% Write            ] Per-Op: 1080  ns, 1724  cycles
[Find - 64 Threads, 20% Write            ] Per-Op: 1096  ns, 1750  cycles
---------
[Peek - 64 Threads, 0 % Write            ] Per-Op: 548   ns, 876   cycles
[Peek - 64 Threads, 5 % Write            ] Per-Op: 1003  ns, 1601  cycles
[Peek - 64 Threads, 10% Write            ] Per-Op: 1024  ns, 1634  cycles
[Peek - 64 Threads, 20% Write            ] Per-Op: 1007  ns, 1607  cycles
---------
[Allocate - New      - 1  Threads, 11 Sizes] Per-Op: 1724  ns, 2751  cycles
[Allocate - New      - 1  Threads, 3  Sizes] Per-Op: 1657  ns, 2645  cycles
[Allocate - New      - 1  Threads, 1  Sizes] Per-Op: 2066  ns, 3298  cycles
---------
[Allocate - Eviction - 1  Threads, 11 Sizes] Per-Op: 1033  ns, 1649  cycles
[Allocate - Eviction - 1  Threads, 3  Sizes] Per-Op: 1151  ns, 1838  cycles
[Allocate - Eviction - 1  Threads, 1  Sizes] Per-Op: 1013  ns, 1617  cycles
---------
[Allocate - New      - 4  Threads, 11 Sizes] Per-Op: 2609  ns, 4166  cycles
[Allocate - New      - 4  Threads, 3  Sizes] Per-Op: 2800  ns, 4470  cycles
[Allocate - New      - 4  Threads, 1  Sizes] Per-Op: 3305  ns, 5276  cycles
---------
[Allocate - Eviction - 4  Threads, 11 Sizes] Per-Op: 1987  ns, 3173  cycles
[Allocate - Eviction - 4  Threads, 3  Sizes] Per-Op: 2678  ns, 4275  cycles
[Allocate - Eviction - 4  Threads, 1  Sizes] Per-Op: 4579  ns, 7310  cycles
---------
[Allocate - New      - 8  Threads, 11 Sizes] Per-Op: 2986  ns, 4767  cycles
[Allocate - New      - 8  Threads, 3  Sizes] Per-Op: 3398  ns, 5425  cycles
[Allocate - New      - 8  Threads, 1  Sizes] Per-Op: 4460  ns, 7119  cycles
---------
[Allocate - Eviction - 8  Threads, 11 Sizes] Per-Op: 2585  ns, 4127  cycles
[Allocate - Eviction - 8  Threads, 3  Sizes] Per-Op: 3986  ns, 6362  cycles
[Allocate - Eviction - 8  Threads, 1  Sizes] Per-Op: 8871  ns, 14161 cycles
---------
[Allocate - New      - 16 Threads, 11 Sizes] Per-Op: 3971  ns, 6339  cycles
[Allocate - New      - 16 Threads, 3  Sizes] Per-Op: 4553  ns, 7269  cycles
[Allocate - New      - 16 Threads, 1  Sizes] Per-Op: 7728  ns, 12337 cycles
---------
[Allocate - Eviction - 16 Threads, 11 Sizes] Per-Op: 3760  ns, 6002  cycles
[Allocate - Eviction - 16 Threads, 3  Sizes] Per-Op: 7178  ns, 11458 cycles
[Allocate - Eviction - 16 Threads, 1  Sizes] Per-Op: 18597 ns, 29686 cycles
---------
[Allocate - New      - 32 Threads, 11 Sizes] Per-Op: 7859  ns, 12545 cycles
[Allocate - New      - 32 Threads, 3  Sizes] Per-Op: 9095  ns, 14518 cycles
[Allocate - New      - 32 Threads, 1  Sizes] Per-Op: 16748 ns, 26735 cycles
---------
[Allocate - Eviction - 32 Threads, 11 Sizes] Per-Op: 6091  ns, 9723  cycles
[Allocate - Eviction - 32 Threads, 3  Sizes] Per-Op: 14225 ns, 22706 cycles
[Allocate - Eviction - 32 Threads, 1  Sizes] Per-Op: 37407 ns, 59711 cycles
---------
[Allocate - New      - 64 Threads, 11 Sizes] Per-Op: 17162 ns, 27394 cycles
[Allocate - New      - 64 Threads, 3  Sizes] Per-Op: 21258 ns, 33933 cycles
[Allocate - New      - 64 Threads, 1  Sizes] Per-Op: 35349 ns, 56426 cycles
---------
[Allocate - Eviction - 64 Threads, 11 Sizes] Per-Op: 25214 ns, 40249 cycles
[Allocate - Eviction - 64 Threads, 3  Sizes] Per-Op: 33873 ns, 54070 cycles
[Allocate - Eviction - 64 Threads, 1  Sizes] Per-Op: 77066 ns, 123017 cycles
----------- Becnhmarks have completed -----------------
clang-format on
*/
