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
    std::cout << folly::sformat("[{: <60}] Per-Op: {: <5} ns, {: <5} cycles",
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
  std::cout << "---- Benchmark Starting Now --------------\n";

  // These benchmarks are trying to compare the performance between
  // different lock implementation, and also variou alignment on locks
  std::cout << "---- Bucket + SharedMutex ----------------\n";
  testSequential<Bucket, SharedMutex>(16, 24, 10, 1'000'000);
  testBatch<Bucket, SharedMutex, 16>(16, 24, 10, 1'000'000, true);
  std::cout << "---- Bucket + SharedMutexAligned ----------------\n";
  testSequential<Bucket, SharedMutexAligned>(16, 24, 10, 1'000'000);
  testBatch<Bucket, SharedMutexAligned, 16>(16, 24, 10, 1'000'000, true);
  std::cout << "---- Bucket + SpinLock -------------------\n";
  testSequential<Bucket, SpinLock>(16, 24, 10, 1'000'000);
  testSequential<Bucket, SpinLock>(16, 24, 16, 1'000'000);
  testSequential<Bucket, SpinLock>(16, 24, 20, 1'000'000);
  testBatch<Bucket, SpinLock, 16>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SpinLock, 16>(16, 24, 16, 1'000'000, true);
  testBatch<Bucket, SpinLock, 16>(16, 24, 20, 1'000'000, true);
  std::cout << "---- Bucket + SpinLockAligned -------------------\n";
  testSequential<Bucket, SpinLockAligned>(16, 24, 10, 1'000'000);
  testSequential<Bucket, SpinLockAligned>(16, 24, 16, 1'000'000);
  testSequential<Bucket, SpinLockAligned>(16, 24, 20, 1'000'000);
  testBatch<Bucket, SpinLockAligned, 16>(16, 24, 10, 1'000'000, true);
  testBatch<Bucket, SpinLockAligned, 16>(16, 24, 16, 1'000'000, true);
  testBatch<Bucket, SpinLockAligned, 16>(16, 24, 20, 1'000'000, true);

  // These benchmarks compare how sequential mode performs with different
  // amount of objects
  std::cout << "---- Different Object Sizes ---\n";
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
  std::cout << "---- Different Prefetching Batch Sizes ---\n";
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
  std::cout << "---- Sequential vs. Prefetching ----------\n";
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

  std::cout << "---- Benchmark Ended ---------------------\n";
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

---- Benchmark Starting Now --------------
---- Bucket + SharedMutex ----------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 345   ns, 551   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 289   ns, 461   cycles
---- Bucket + SharedMutexAligned ----------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 346   ns, 552   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 224   ns, 357   cycles
---- Bucket + SpinLock -------------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 500   ns, 799   cycles
[Sequential_reg - 16 T, 24 HB, 16 HL, 1000000  Objects       ] Per-Op: 440   ns, 703   cycles
[Sequential_reg - 16 T, 24 HB, 20 HL, 1000000  Objects       ] Per-Op: 505   ns, 806   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 324   ns, 517   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 16 HL, 1000000  Objects   ] Per-Op: 290   ns, 463   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 20 HL, 1000000  Objects   ] Per-Op: 289   ns, 461   cycles
---- Bucket + SpinLockAligned -------------------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 453   ns, 724   cycles
[Sequential_reg - 16 T, 24 HB, 16 HL, 1000000  Objects       ] Per-Op: 510   ns, 815   cycles
[Sequential_reg - 16 T, 24 HB, 20 HL, 1000000  Objects       ] Per-Op: 527   ns, 841   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 251   ns, 401   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 16 HL, 1000000  Objects   ] Per-Op: 290   ns, 463   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 20 HL, 1000000  Objects   ] Per-Op: 269   ns, 429   cycles
---- Different Object Sizes ---
[Sequential_reg - 1  T, 14 HB, 10 HL, 1000     Objects       ] Per-Op: 85    ns, 136   cycles
[Sequential_reg - 1  T, 14 HB, 10 HL, 10000    Objects       ] Per-Op: 112   ns, 179   cycles
[Sequential_reg - 1  T, 16 HB, 10 HL, 10000    Objects       ] Per-Op: 104   ns, 166   cycles
[Sequential_reg - 1  T, 16 HB, 10 HL, 100000   Objects       ] Per-Op: 368   ns, 587   cycles
[Sequential_reg - 1  T, 20 HB, 10 HL, 100000   Objects       ] Per-Op: 326   ns, 521   cycles
[Sequential_reg - 1  T, 20 HB, 10 HL, 1000000  Objects       ] Per-Op: 505   ns, 807   cycles
[Sequential_reg - 1  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 351   ns, 560   cycles
[Sequential_reg - 1  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 610   ns, 975   cycles
[Sequential_reg - 1  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 588   ns, 938   cycles
---- Different Prefetching Batch Sizes ---
[Pretch_reg - 1    B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 1445  ns, 2307  cycles
[Pretch_reg - 2    B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 815   ns, 1301  cycles
[Pretch_reg - 4    B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 403   ns, 644   cycles
[Pretch_reg - 8    B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 281   ns, 448   cycles
[Pretch_reg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 281   ns, 448   cycles
[Pretch_reg - 32   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 245   ns, 392   cycles
[Pretch_reg - 64   B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 260   ns, 415   cycles
[Pretch_reg - 128  B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 288   ns, 460   cycles
[Pretch_reg - 256  B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 280   ns, 447   cycles
[Pretch_reg - 1024 B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 318   ns, 507   cycles
[Pretch_reg - 4096 B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 390   ns, 622   cycles
[Pretch_reg - 8192 B, 16 T, 24 HB, 10 HL, 1000000  Objects   ] Per-Op: 429   ns, 686   cycles
---- Sequential vs. Prefetching ----------
--------
[Sequential_reg - 1  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 320   ns, 512   cycles
[Sequential_key - 1  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 296   ns, 472   cycles
[Prefetchreg - 16   B, 1  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 179   ns, 286   cycles
[Prefetchkey - 16   B, 1  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 193   ns, 308   cycles
--------
[Sequential_reg - 1  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 445   ns, 711   cycles
[Sequential_key - 1  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 286   ns, 456   cycles
[Prefetchreg - 16   B, 1  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 259   ns, 414   cycles
[Prefetchkey - 16   B, 1  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 255   ns, 407   cycles
--------
[Sequential_reg - 1  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 685   ns, 1094  cycles
[Sequential_key - 1  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 374   ns, 597   cycles
[Prefetchreg - 16   B, 1  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 481   ns, 768   cycles
[Prefetchkey - 16   B, 1  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 411   ns, 657   cycles
--------
[Sequential_reg - 1  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 426   ns, 680   cycles
[Sequential_key - 1  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 324   ns, 517   cycles
[Prefetchreg - 16   B, 1  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 231   ns, 369   cycles
[Prefetchkey - 16   B, 1  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 254   ns, 406   cycles
--------
[Sequential_reg - 1  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 507   ns, 810   cycles
[Sequential_key - 1  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 313   ns, 501   cycles
[Prefetchreg - 16   B, 1  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 305   ns, 487   cycles
[Prefetchkey - 16   B, 1  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 309   ns, 493   cycles
--------
[Sequential_reg - 1  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 626   ns, 1000  cycles
[Sequential_key - 1  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 364   ns, 581   cycles
[Prefetchreg - 16   B, 1  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 435   ns, 694   cycles
[Prefetchkey - 16   B, 1  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 377   ns, 602   cycles
--------
[Sequential_reg - 4  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 286   ns, 456   cycles
[Sequential_key - 4  T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 229   ns, 365   cycles
[Prefetchreg - 16   B, 4  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 169   ns, 270   cycles
[Prefetchkey - 16   B, 4  T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 170   ns, 272   cycles
--------
[Sequential_reg - 4  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 351   ns, 561   cycles
[Sequential_key - 4  T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 247   ns, 395   cycles
[Prefetchreg - 16   B, 4  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 228   ns, 364   cycles
[Prefetchkey - 16   B, 4  T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 246   ns, 392   cycles
--------
[Sequential_reg - 4  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 516   ns, 823   cycles
[Sequential_key - 4  T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 342   ns, 546   cycles
[Prefetchreg - 16   B, 4  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 384   ns, 614   cycles
[Prefetchkey - 16   B, 4  T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 337   ns, 538   cycles
--------
[Sequential_reg - 4  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 332   ns, 531   cycles
[Sequential_key - 4  T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 242   ns, 386   cycles
[Prefetchreg - 16   B, 4  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 190   ns, 304   cycles
[Prefetchkey - 16   B, 4  T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 193   ns, 309   cycles
--------
[Sequential_reg - 4  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 386   ns, 617   cycles
[Sequential_key - 4  T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 275   ns, 439   cycles
[Prefetchreg - 16   B, 4  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 246   ns, 392   cycles
[Prefetchkey - 16   B, 4  T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 261   ns, 417   cycles
--------
[Sequential_reg - 4  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 487   ns, 778   cycles
[Sequential_key - 4  T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 301   ns, 481   cycles
[Prefetchreg - 16   B, 4  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 348   ns, 556   cycles
[Prefetchkey - 16   B, 4  T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 311   ns, 496   cycles
--------
[Sequential_reg - 16 T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 368   ns, 588   cycles
[Sequential_key - 16 T, 24 HB, 10 HL, 100000   Objects       ] Per-Op: 284   ns, 453   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 211   ns, 336   cycles
[Prefetchkey - 16   B, 16 T, 24 HB, 10 HL, 100000   Objects  ] Per-Op: 224   ns, 358   cycles
--------
[Sequential_reg - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 462   ns, 737   cycles
[Sequential_key - 16 T, 24 HB, 10 HL, 1000000  Objects       ] Per-Op: 428   ns, 683   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 272   ns, 435   cycles
[Prefetchkey - 16   B, 16 T, 24 HB, 10 HL, 1000000  Objects  ] Per-Op: 336   ns, 537   cycles
--------
[Sequential_reg - 16 T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 645   ns, 1030  cycles
[Sequential_key - 16 T, 24 HB, 10 HL, 10000000 Objects       ] Per-Op: 442   ns, 706   cycles
[Prefetchreg - 16   B, 16 T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 389   ns, 621   cycles
[Prefetchkey - 16   B, 16 T, 24 HB, 10 HL, 10000000 Objects  ] Per-Op: 388   ns, 620   cycles
--------
[Sequential_reg - 16 T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 436   ns, 696   cycles
[Sequential_key - 16 T, 26 HB, 10 HL, 100000   Objects       ] Per-Op: 333   ns, 532   cycles
[Prefetchreg - 16   B, 16 T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 225   ns, 359   cycles
[Prefetchkey - 16   B, 16 T, 26 HB, 10 HL, 100000   Objects  ] Per-Op: 271   ns, 432   cycles
--------
[Sequential_reg - 16 T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 485   ns, 775   cycles
[Sequential_key - 16 T, 26 HB, 10 HL, 1000000  Objects       ] Per-Op: 454   ns, 725   cycles
[Prefetchreg - 16   B, 16 T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 312   ns, 498   cycles
[Prefetchkey - 16   B, 16 T, 26 HB, 10 HL, 1000000  Objects  ] Per-Op: 318   ns, 507   cycles
--------
[Sequential_reg - 16 T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 624   ns, 996   cycles
[Sequential_key - 16 T, 26 HB, 10 HL, 10000000 Objects       ] Per-Op: 442   ns, 707   cycles
[Prefetchreg - 16   B, 16 T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 337   ns, 538   cycles
[Prefetchkey - 16   B, 16 T, 26 HB, 10 HL, 10000000 Objects  ] Per-Op: 326   ns, 521   cycles
---- Benchmark Ended ---------------------
clang-format on
*/
