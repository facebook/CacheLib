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
Microbenchmarks to explore strategy to improve existence check
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
template <size_t PayloadSize>
struct ObjectImpl {
  explicit ObjectImpl(std::string k) : key(std::move(k)) {}
  std::string key;
  std::array<uint8_t, PayloadSize> payload;
};

struct Bucket {
  using Object = ObjectImpl<100>;

  void setObject(Object* o) { obj = o; }
  const std::string& getKey() const { return obj->key; }

  Object* obj{};
  Bucket* next{};
};

struct BucketWithKey {
  using Object = ObjectImpl<100>;

  void setObject(Object* o) {
    obj = o;
    key = o->key;
  }
  const std::string& getKey() const { return key; }

  std::string key;
  Object* obj{};
  BucketWithKey* next{};
};

struct SharedMutex {
  auto getReadLock() { return folly::SharedMutex::ReadHolder{l}; }
  auto getWriteLock() { return folly::SharedMutex::WriteHolder{l}; }
  folly::SharedMutex l{};
};

struct alignas(folly::hardware_destructive_interference_size)
    SharedMutexAligned {
  auto getReadLock() { return folly::SharedMutex::ReadHolder{l}; }
  auto getWriteLock() { return folly::SharedMutex::WriteHolder{l}; }
  folly::SharedMutex l{};
};

struct SpinLock {
  auto getReadLock() { return std::lock_guard<folly::MicroSpinLock>(l); }
  auto getWriteLock() { return std::lock_guard<folly::MicroSpinLock>(l); }
  folly::MicroSpinLock l{};
};

struct alignas(folly::hardware_destructive_interference_size) SpinLockAligned {
  auto getReadLock() { return std::lock_guard<folly::MicroSpinLock>(l); }
  auto getWriteLock() { return std::lock_guard<folly::MicroSpinLock>(l); }
  folly::MicroSpinLock l{};
};

template <typename BucketT, typename LockT>
class HashTableImpl {
 public:
  using BucketType = BucketT;
  using Object = typename BucketType::Object;
  using Lock = LockT;

  HashTableImpl(int bucketPower, int lockPower)
      : locks_((1ULL << lockPower)), buckets_((1ULL << bucketPower)) {}

  ~HashTableImpl() {
    for (auto b : buckets_) {
      auto* curr = b.next;
      while (curr != nullptr) {
        auto* next = curr->next;
        delete curr;
        curr = next;
      }
    }
  }

  void insert(Object* obj) {
    auto& bucket = getBucket(obj->key);
    auto w = getLock(obj->key).getWriteLock();
    auto* curBucket = &bucket;
    while (curBucket->obj) {
      if (!curBucket->next) {
        curBucket->next = new BucketType;
      }
      curBucket = curBucket->next;
    }
    curBucket->setObject(obj);
  }

  Object* lookup(const std::string& key) {
    auto& bucket = getBucket(key);
    auto r = getLock(key).getReadLock();
    auto* curBucket = &bucket;
    while (curBucket) {
      if (curBucket->getKey() == key) {
        return curBucket->obj;
      }
      curBucket = curBucket->next;
    }
    return nullptr;
  }

  template <size_t BATCH_SIZE>
  void multiLookup(const std::array<std::string, BATCH_SIZE>& keys,
                   std::array<Object*, BATCH_SIZE>& objects) {
    for (size_t i = 0; i < keys.size(); i++) {
      objects[i] = lookup(keys[i]);
    }
  }

  template <size_t BATCH_SIZE>
  void multiLookup(const std::array<std::string, BATCH_SIZE>& keys,
                   std::array<Object*, BATCH_SIZE>& objects,
                   bool prefetchObject) {
    std::array<BucketType*, BATCH_SIZE> buckets;
    std::array<Lock*, BATCH_SIZE> locks;

    for (size_t i = 0; i < keys.size(); i++) {
      MurmurHash2 hasher;
      uint32_t hash = hasher(keys[i].data(), keys[i].size());
      buckets[i] = &buckets_[static_cast<size_t>(hash) % buckets_.size()];
      prefetchRead(buckets[i]);
      locks[i] = &locks_[static_cast<size_t>(hash % locks_.size())];
    }

    if (prefetchObject) {
      for (size_t i = 0; i < keys.size(); i++) {
        prefetchRead(buckets[i]->obj);
      }
    }

    for (size_t i = 0; i < keys.size(); i++) {
      auto r = locks[i]->getReadLock();
      auto* curBucket = buckets[i];
      while (curBucket) {
        if (curBucket->getKey() == keys[i]) {
          objects[i] = curBucket->obj;
          break;
        }
        curBucket = curBucket->next;
      }
      objects[i] = nullptr;
    }
  }

 private:
  static FOLLY_ALWAYS_INLINE void prefetchRead(void* ptr) {
    __builtin_prefetch(ptr, /* read or write */ 0, /* locality hint */ 3);
  }

  BucketType& getBucket(const std::string& key) {
    return buckets_[getHash(key) % buckets_.size()];
  }

  Lock& getLock(const std::string& key) {
    return locks_[getHash(key) % locks_.size()];
  }

  uint32_t getHash(const std::string& key) {
    MurmurHash2 hasher;
    return hasher(key.data(), key.size());
  }

  std::vector<Lock> locks_;
  std::vector<BucketType> buckets_;
};
} // namespace

template <typename BucketT, typename LockT>
void testSequential(int numThreads,
                    int htBucketPower,
                    int htLockPower,
                    uint64_t numObjects,
                    const char* msg = "reg") {
  using HashTable = HashTableImpl<BucketT, LockT>;
  using Object = typename HashTable::Object;

  constexpr uint64_t kLoops = 10'000'000;

  std::vector<std::string> keys;
  std::vector<std::unique_ptr<Object>> objects;
  std::unique_ptr<HashTable> ht;
  BENCHMARK_SUSPEND {
    ht = std::make_unique<HashTable>(htBucketPower, htLockPower);
    for (uint64_t i = 0; i < numObjects; i++) {
      auto key = folly::sformat("k_{:<8}", i);
      keys.push_back(key);
      objects.push_back(std::make_unique<Object>(key));
      ht->insert(objects.back().get());
    }
  }

  navy::SeqPoints sp;
  auto readOps = [&] {
    sp.wait(0);

    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> dist(0, numObjects - 1);
    for (uint64_t loop = 0; loop < kLoops; loop++) {
      const auto& key = keys[dist(gen)];
      auto object = ht->lookup(key);
      folly::doNotOptimizeAway(object);
    }
  };

  std::vector<std::thread> rs;
  for (int i = 0; i < numThreads; i++) {
    rs.push_back(std::thread{readOps});
  }

  {
    Timer t{
        folly::sformat(
            "Sequential_{} - {: <2} T, {: <2} HB, {: <2} HL, {: <8} Objects",
            msg, numThreads, htBucketPower, htLockPower, numObjects),
        kLoops};
    sp.reached(0); // Start the operations
    for (auto& r : rs) {
      r.join();
    }
  }
}

template <typename BucketT, typename LockT, size_t BATCH_SIZE>
void testBatch(int numThreads,
               int htBucketPower,
               int htLockPower,
               uint64_t numObjects,
               bool doesPrefetchObject,
               const char* msg = "reg") {
  using HashTable = HashTableImpl<BucketT, LockT>;
  using Object = typename HashTable::Object;

  constexpr uint64_t kLoops = 10'000'000;

  std::vector<std::string> keys;
  std::vector<std::unique_ptr<Object>> objects;
  std::unique_ptr<HashTable> ht;
  BENCHMARK_SUSPEND {
    ht = std::make_unique<HashTable>(htBucketPower, htLockPower);
    for (uint64_t i = 0; i < numObjects; i++) {
      auto key = folly::sformat("k_{:<8}", i);
      keys.push_back(key);
      objects.push_back(std::make_unique<Object>(key));
      ht->insert(objects.back().get());
    }
  }

  navy::SeqPoints sp;
  auto readOps = [&] {
    sp.wait(0);

    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> dist(0, numObjects - 1);
    for (uint64_t loop = 0; loop < kLoops / BATCH_SIZE; loop++) {
      std::array<Object*, BATCH_SIZE> objects;
      std::array<std::string, BATCH_SIZE> batchedKeys;
      BENCHMARK_SUSPEND {
        for (auto& key : batchedKeys) {
          key = keys[dist(gen)];
        }
      }
      ht->template multiLookup<BATCH_SIZE>(batchedKeys, objects,
                                           doesPrefetchObject);
      folly::doNotOptimizeAway(objects);
    }
  };

  std::vector<std::thread> rs;
  for (int i = 0; i < numThreads; i++) {
    rs.push_back(std::thread{readOps});
  }

  {
    Timer t{folly::sformat("Prefetch{} - {: <4} B, {: <2} T, {: <2} HB, {: <2} "
                           "HL, {: <8} Objects",
                           msg, BATCH_SIZE, numThreads, htBucketPower,
                           htLockPower, numObjects),
            kLoops};
    sp.reached(0); // Start the operations
    for (auto& r : rs) {
      r.join();
    }
  }
}
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib;

int main(int argc, char** argv) {
  static_assert(sizeof(SharedMutex) < sizeof(SharedMutexAligned), "alignment");
  static_assert(sizeof(SpinLock) < sizeof(SpinLockAligned), "alignment");

  folly::init(&argc, &argv);

  // clang-format off
  printMsg("Benchmark Starting Now");

  // These benchmarks are trying to compare the performance between
  // different lock implementation, and also variou alignment on locks
  printMsg("Bucket + SharedMutex");
  testSequential<Bucket, SharedMutex>(16, 24, 10, 1'000'000);
  testBatch<Bucket, SharedMutex, 16>(16, 24, 10, 1'000'000, true);
  printMsg("Bucket + SharedMutexAligned");
  testSequential<Bucket, SharedMutexAligned>(16, 24, 10, 1'000'000);
  testBatch<Bucket, SharedMutexAligned, 16>(16, 24, 10, 1'000'000, true);
  printMsg("Bucket + SpinLock");
  testSequential<Bucket, SpinLock>(16, 24, 10, 1'000'000);
  testSequential<Bucket, SpinLock>(16, 24, 16, 1'000'000);
  testSequential<Bucket, SpinLock>(16, 24, 20, 1'000'000);
  testBatch<Bucket, SpinLock, 16>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SpinLock, 16>(16, 24, 16, 1'000'000, true);
  testBatch<Bucket, SpinLock, 16>(16, 24, 20, 1'000'000, true);
  printMsg("Bucket + SpinLockAligned");
  testSequential<Bucket, SpinLockAligned>(16, 24, 10, 1'000'000);
  testSequential<Bucket, SpinLockAligned>(16, 24, 16, 1'000'000);
  testSequential<Bucket, SpinLockAligned>(16, 24, 20, 1'000'000);
  testBatch<Bucket, SpinLockAligned, 16>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SpinLockAligned, 16>(16, 24, 16, 1'000'000, true);
  testBatch<Bucket, SpinLockAligned, 16>(16, 24, 20, 1'000'000, true);

  // These benchmarks compare how sequential mode performs with different
  // amount of objects
  printMsg("Different Object Sizes");
  testSequential<Bucket, SharedMutex>(1, 14, 10, 1000);
  testSequential<Bucket, SharedMutex>(1, 14, 10, 10'000);
  testSequential<Bucket, SharedMutex>(1, 16, 10, 10'000);
  testSequential<Bucket, SharedMutex>(1, 16, 10, 100'000);
  testSequential<Bucket, SharedMutex>(1, 20, 10, 100'000);
  testSequential<Bucket, SharedMutex>(1, 20, 10, 1'000'000);
  testSequential<Bucket, SharedMutex>(1, 24, 10, 1'000'000);
  testSequential<Bucket, SharedMutex>(1, 24, 10, 10'000'000);
  testSequential<Bucket, SharedMutex>(1, 26, 10, 10'000'000);

  // These bnechmarks compare the different prefetching batch sizes
  printMsg("Different Prefetching Batch Sizes");
  testBatch<Bucket, SharedMutex, 1>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 2>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 4>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 8>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 16>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 32>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 64>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 128>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 256>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 1024>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 4096>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SharedMutex, 8192>(16, 24, 10, 1'000'000, true);

  // These benchmarks compare how sequential and batch modes perform with
  // different object sizes and different hashtable sizes. In addition,
  // we also compare against a hashtable with key embedded
  printMsg("Sequential vs. Prefetching");
  for (auto t : {1, 4, 16}) {
    for (auto b : {24, 26}) {
      for (auto o : {100'000, 1'000'000, 10'000'000}) {
        std::cout << "--------\n";
        testSequential<Bucket, SharedMutex>(t, b, 10, o);
        testSequential<BucketWithKey, SharedMutex>(t, b, 10, o, "key");
        testBatch<Bucket, SharedMutex, 16>(t, b, 10, o, true);
        testBatch<BucketWithKey, SharedMutex, 16>(t, b, 10, o, false, "key");
      }
    }
  }

  printMsg("Becnhmarks have completed");
  // clang-format on
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
CPU MHz:             1855.037
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
-------- Bucket + SharedMutex ----------------------------------------------------------------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 310   ns, 495   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 191   ns, 305   cycles
-------- Bucket + SharedMutexAligned ---------------------------------------------------------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 308   ns, 491   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 192   ns, 306   cycles
-------- Bucket + SpinLock -------------------------------------------------------------------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 431   ns, 689   cycles
[Sequential_reg - 16 T, 24 HB, 16 HL, 1000000  Objects       ] Per-Op: 367   ns, 586   cycles
[Sequential_reg - 16 T, 24 HB, 20 HL, 1000000  Objects       ] Per-Op: 361   ns, 576   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 280   ns, 447   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 16 HL, 1000000  Objects  ] Per-Op: 204   ns, 325   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 20 HL, 1000000  Objects  ] Per-Op: 209   ns, 334   cycles
-------- Bucket + SpinLockAligned ------------------------------------------------------------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 368   ns, 588   cycles
[Sequential_reg - 16 T, 24 HB, 16 HL, 1000000  Objects       ] Per-Op: 403   ns, 644   cycles
[Sequential_reg - 16 T, 24 HB, 20 HL, 1000000  Objects       ] Per-Op: 424   ns, 677   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 205   ns, 327   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 16 HL, 1000000  Objects  ] Per-Op: 217   ns, 346   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 20 HL, 1000000  Objects  ] Per-Op: 222   ns, 355   cycles
-------- Different Object Sizes --------------------------------------------------------------------
[Sequential_reg - 1  T, 14 HB, 10 HL, 1000     Objects       ] Per-Op: 82    ns, 132   cycles
[Sequential_reg - 1  T, 14 HB, 10 HL, 10000    Objects       ] Per-Op: 100   ns, 160   cycles
[Sequential_reg - 1  T, 16 HB, 10 HL, 10000    Objects       ] Per-Op: 90    ns, 144   cycles
[Sequential_reg - 1  T, 16 HB, 10 HL, 100000   Objects       ] Per-Op: 195   ns, 311   cycles
[Sequential_reg - 1  T, 20 HB, 10 HL, 100000   Objects       ] Per-Op: 183   ns, 292   cycles
[Sequential_reg - 1  T, 20 HB, 10 HL, 1000000  Objects       ] Per-Op: 367   ns, 586   cycles
[Sequential_reg - 1  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 304   ns, 485   cycles
[Sequential_reg - 1  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 398   ns, 636   cycles
[Sequential_reg - 1  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 359   ns, 573   cycles
-------- Different Prefetching Batch Sizes ---------------------------------------------------------
[Prefetchreg - 1    B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 1456  ns, 2325  cycles
[Prefetchreg - 2    B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 743   ns, 1187  cycles
[Prefetchreg - 4    B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 375   ns, 599   cycles
[Prefetchreg - 8    B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 214   ns, 342   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 196   ns, 313   cycles
[Prefetchreg - 32   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 190   ns, 304   cycles
[Prefetchreg - 64   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 186   ns, 298   cycles
[Prefetchreg - 128  B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 187   ns, 298   cycles
[Prefetchreg - 256  B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 186   ns, 297   cycles
[Prefetchreg - 1024 B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 203   ns, 324   cycles
[Prefetchreg - 4096 B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 228   ns, 365   cycles
[Prefetchreg - 8192 B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 243   ns, 389   cycles
-------- Sequential vs. Prefetching ----------------------------------------------------------------
--------
[Sequential_reg - 1  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 174   ns, 278   cycles
[Sequential_key - 1  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 116   ns, 185   cycles
[Prefetchreg - 16   B, 1  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 106   ns, 169   cycles
[Prefetchkey - 16   B, 1  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 98    ns, 157   cycles
--------
[Sequential_reg - 1  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 304   ns, 485   cycles
[Sequential_key - 1  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 194   ns, 309   cycles
[Prefetchreg - 16   B, 1  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 168   ns, 268   cycles
[Prefetchkey - 16   B, 1  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 168   ns, 269   cycles
--------
[Sequential_reg - 1  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 397   ns, 635   cycles
[Sequential_key - 1  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 246   ns, 393   cycles
[Prefetchreg - 16   B, 1  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 307   ns, 490   cycles
[Prefetchkey - 16   B, 1  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 249   ns, 398   cycles
--------
[Sequential_reg - 1  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 175   ns, 279   cycles
[Sequential_key - 1  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 126   ns, 201   cycles
[Prefetchreg - 16   B, 1  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 100   ns, 160   cycles
[Prefetchkey - 16   B, 1  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 103   ns, 165   cycles
--------
[Sequential_reg - 1  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 315   ns, 503   cycles
[Sequential_key - 1  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 215   ns, 343   cycles
[Prefetchreg - 16   B, 1  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 186   ns, 297   cycles
[Prefetchkey - 16   B, 1  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 192   ns, 307   cycles
--------
[Sequential_reg - 1  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 373   ns, 595   cycles
[Sequential_key - 1  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 233   ns, 372   cycles
[Prefetchreg - 16   B, 1  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 294   ns, 470   cycles
[Prefetchkey - 16   B, 1  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 258   ns, 411   cycles
--------
[Sequential_reg - 4  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 156   ns, 250   cycles
[Sequential_key - 4  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 113   ns, 181   cycles
[Prefetchreg - 16   B, 4  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 106   ns, 170   cycles
[Prefetchkey - 16   B, 4  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 122   ns, 196   cycles
--------
[Sequential_reg - 4  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 303   ns, 484   cycles
[Sequential_key - 4  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 196   ns, 313   cycles
[Prefetchreg - 16   B, 4  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 188   ns, 301   cycles
[Prefetchkey - 16   B, 4  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 196   ns, 314   cycles
--------
[Sequential_reg - 4  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 432   ns, 690   cycles
[Sequential_key - 4  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 270   ns, 431   cycles
[Prefetchreg - 16   B, 4  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 310   ns, 495   cycles
[Prefetchkey - 16   B, 4  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 275   ns, 439   cycles
--------
[Sequential_reg - 4  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 171   ns, 273   cycles
[Sequential_key - 4  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 189   ns, 303   cycles
[Prefetchreg - 16   B, 4  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 108   ns, 173   cycles
[Prefetchkey - 16   B, 4  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 110   ns, 176   cycles
--------
[Sequential_reg - 4  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 311   ns, 496   cycles
[Sequential_key - 4  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 222   ns, 355   cycles
[Prefetchreg - 16   B, 4  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 185   ns, 296   cycles
[Prefetchkey - 16   B, 4  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 284   ns, 454   cycles
--------
[Sequential_reg - 4  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 369   ns, 590   cycles
[Sequential_key - 4  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 247   ns, 394   cycles
[Prefetchreg - 16   B, 4  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 273   ns, 437   cycles
[Prefetchkey - 16   B, 4  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 257   ns, 411   cycles
--------
[Sequential_reg - 16 T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 166   ns, 265   cycles
[Sequential_key - 16 T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 221   ns, 354   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 119   ns, 190   cycles
[Prefetchkey - 16   B, 16 T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 157   ns, 250   cycles
--------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 306   ns, 489   cycles
[Sequential_key - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 204   ns, 327   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 269   ns, 429   cycles
[Prefetchkey - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 193   ns, 308   cycles
--------
[Sequential_reg - 16 T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 419   ns, 668   cycles
[Sequential_key - 16 T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 270   ns, 432   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 311   ns, 496   cycles
[Prefetchkey - 16   B, 16 T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 284   ns, 453   cycles
--------
[Sequential_reg - 16 T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 239   ns, 381   cycles
[Sequential_key - 16 T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 187   ns, 299   cycles
[Prefetchreg - 16   B, 16 T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 120   ns, 192   cycles
[Prefetchkey - 16   B, 16 T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 119   ns, 191   cycles
--------
[Sequential_reg - 16 T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 320   ns, 511   cycles
[Sequential_key - 16 T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 231   ns, 368   cycles
[Prefetchreg - 16   B, 16 T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 195   ns, 312   cycles
[Prefetchkey - 16   B, 16 T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 213   ns, 340   cycles
--------
[Sequential_reg - 16 T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 382   ns, 610   cycles
[Sequential_key - 16 T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 300   ns, 479   cycles
[Prefetchreg - 16   B, 16 T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 285   ns, 454   cycles
[Prefetchkey - 16   B, 16 T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 268   ns, 428   cycles
-------- Becnhmarks have completed -----------------------------------------------------------------
clang-format on
*/
