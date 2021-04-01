#include <folly/Benchmark.h>
#include <folly/container/EvictingCacheMap.h>
#include <folly/container/F14Map.h>

#include <map>
#include <scoped_allocator>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/benchmarks/objcache/Common.h"
#include "cachelib/benchmarks/objcache/gen-cpp2/ObjectCache_types.h"
#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/ObjectCache.h"
#include "folly/BenchmarkUtil.h"

namespace facebook {
namespace cachelib {
namespace benchmark {
using namespace facebook::cachelib::objcache;

constexpr uint64_t kNumObjects = 100'000;
constexpr uint64_t kCacheSize = 10 * 1024 * 1024 * 1024ul; // 10GB
const std::string kString = "hello I am a long lonnnnnnnnnnnnng string";

LruAllocator::Config getConfig() {
  LruAllocator::Config config;
  config.setCacheSize(kCacheSize);
  config.setAccessConfig(LruAllocator::AccessConfig{20, 10});
  config.setDefaultAllocSizes(1.08, 1024 * 1024, 64, false);
  return config;
}

template <typename T>
void updateObject(T& obj) {
  for (int j = 0; j < 100; j++) {
    obj->m1_ref().value().insert(std::make_pair(j, j));
  }
  for (int j = 0; j < 4; j++) {
    obj->m2_ref().value().insert(std::make_pair(j, kString));
  }
  for (int j = 0; j < 4; j++) {
    obj->m3_ref().value()[static_cast<int16_t>(j)];
    // Small vector. Assume max 8 entries.
    for (int k = 0; k < 8; k++) {
      obj->m3_ref().value()[static_cast<int16_t>(j)].push_back(k);
    }
  }
  for (int j = 0; j < 2; j++) {
    obj->m4_ref().value()[static_cast<int16_t>(j)];
    for (int k = 0; k < 2; k++) {
      obj->m4_ref().value()[static_cast<int16_t>(j)].insert(
          std::make_pair(k, kString));
    }
  }
}

template <typename ObjectType>
void testFullHeapSize() {
  using Cache = folly::EvictingCacheMap<std::string, ObjectType>;
  std::unique_ptr<Cache> cache;
  BENCHMARK_SUSPEND { cache = std::make_unique<Cache>(kNumObjects); }

  auto memBefore = util::getRSSBytes();
  for (uint64_t i = 0; i < kNumObjects; i++) {
    auto obj = std::make_unique<ObjectType>();
    updateObject(obj);
    cache->set(folly::sformat("key_{}", i), std::move(*obj));
  }

  auto memAfter = util::getRSSBytes();
  constexpr size_t kMb = 1024 * 1024;
  XLOGF(INFO,
        "mem before: {} MB, mem after: {} MB, growth: {} MB, per-item: {} KB",
        memBefore / kMb,
        memAfter / kMb,
        (memAfter - memBefore) / kMb,
        (memAfter - memBefore) / kNumObjects / 1024);

  // We intentionally do not clean up the cache so that we can get
  // accurate heap space tracking for the subsequent tests
  cache.release();
}

template <typename ObjectType, typename Protocol>
void testSerializedObjectSize() {
  std::unique_ptr<LruAllocator> cache;
  BENCHMARK_SUSPEND {
    cache = std::make_unique<LruAllocator>(getConfig());
    cache->addPool("default", cache->getCacheMemoryStats().cacheSize);
  }

  auto memBefore = util::getRSSBytes();
  for (uint64_t i = 0; i < kNumObjects; i++) {
    auto obj = std::make_unique<ObjectType>();
    updateObject(obj);

    auto iobuf = Serializer::serializeToIOBuf<ObjectType, Protocol>(*obj);
    auto hdl = cache->allocate(
        0 /* default pool */, folly::sformat("key_{}", i), iobuf->length());
    XCHECK(hdl);
    XLOG_EVERY_MS(INFO, 10'000) << "serialized item bytes: " << hdl->getSize();
    std::memcpy(hdl->getMemory(), iobuf->data(), iobuf->length());
    cache->insertOrReplace(hdl);
  }
  auto memAfter = util::getRSSBytes();

  auto poolStats = cache->getPoolStats(0);
  constexpr size_t kMb = 1024 * 1024;
  XLOGF(INFO,
        "mem before: {} MB, mem after: {} MB, growth: {} MB, per-item: {} KB, "
        "fragmentation: {} MB",
        memBefore / kMb,
        memAfter / kMb,
        (memAfter - memBefore) / kMb,
        (memAfter - memBefore) / kNumObjects / 1024,
        poolStats.totalFragmentation() / kMb);
}

template <typename ObjectType>
void testObjectSize() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND {
    LruObjectCache::Config config;
    config.setCacheAllocatorConfig(getConfig());
    config.getCacheAllocatorConfig().setDefaultAllocSizes(
        1.08, 1024 * 1024, 64, false);
    cache = std::make_unique<LruObjectCache>(config);
    cache->getCacheAlloc().addPool(
        "default", cache->getCacheAlloc().getCacheMemoryStats().cacheSize);
  }

  auto memBefore = util::getRSSBytes();
  for (uint64_t i = 0; i < kNumObjects; i++) {
    auto obj =
        cache->create<ObjectType>(0 /* pool id */, folly::sformat("key_{}", i));
    XCHECK(obj);
    updateObject(obj);

    if (i == 0) {
      auto bytesOccupied = obj.viewItemHandle()->getSize();
      if (obj.viewItemHandle()->hasChainedItem()) {
        auto chainedAllocs =
            cache->getCacheAlloc().viewAsChainedAllocs(obj.viewItemHandle());
        for (const auto& c : chainedAllocs.getChain()) {
          bytesOccupied += c.getSize();
        }
      }
      auto* metadata =
          obj->get_allocator().getAllocatorResource().viewMetadata();
      XLOG(INFO) << "item bytes: " << metadata->usedBytes
                 << ", total occupied bytes: " << bytesOccupied;
    }
    cache->insertOrReplace(obj);
  }
  auto memAfter = util::getRSSBytes();

  auto poolStats = cache->getCacheAlloc().getPoolStats(0);
  constexpr size_t kMb = 1024 * 1024;
  XLOGF(INFO,
        "mem before: {} MB, mem after: {} MB, growth: {} MB, per-item: {} KB, "
        "fragmentation: {} MB",
        memBefore / kMb,
        memAfter / kMb,
        (memAfter - memBefore) / kMb,
        (memAfter - memBefore) / kNumObjects / 1024,
        poolStats.totalFragmentation() / kMb);
}

template <typename ObjectType>
void testObjectSizeWithCompaction() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND {
    LruObjectCache::Config config;
    config.setCacheAllocatorConfig(getConfig());
    config.getCacheAllocatorConfig().setDefaultAllocSizes(
        1.08, 1024 * 1024, 64, false);
    cache = std::make_unique<LruObjectCache>(config);
    cache->getCacheAlloc().addPool(
        "default", cache->getCacheAlloc().getCacheMemoryStats().cacheSize);
  }

  auto memBefore = util::getRSSBytes();
  for (uint64_t i = 0; i < kNumObjects; i++) {
    auto fullObj =
        cache->create<ObjectType>(0 /* poolId */, folly::sformat("key_{}", i));
    updateObject(fullObj);
    auto obj = cache->createCompact<ObjectType>(
        0 /* pool id */, folly::sformat("key_{}", i), *fullObj);
    XCHECK(obj);
    if (i == 0) {
      auto bytesOccupied = obj.viewItemHandle()->getSize();
      if (obj.viewItemHandle()->hasChainedItem()) {
        auto chainedAllocs =
            cache->getCacheAlloc().viewAsChainedAllocs(obj.viewItemHandle());
        for (const auto& c : chainedAllocs.getChain()) {
          bytesOccupied += c.getSize();
        }
      }
      auto* metadata =
          obj->get_allocator().getAllocatorResource().viewMetadata();
      XLOG(INFO) << "item bytes: " << metadata->usedBytes
                 << ", total occupied bytes: " << bytesOccupied;
    }
    cache->insertOrReplace(obj);
  }
  auto memAfter = util::getRSSBytes();

  auto poolStats = cache->getCacheAlloc().getPoolStats(0);
  constexpr size_t kMb = 1024 * 1024;
  XLOGF(INFO,
        "mem before: {} MB, mem after: {} MB, growth: {} MB, per-item: {} KB, "
        "fragmentation: {} MB",
        memBefore / kMb,
        memAfter / kMb,
        (memAfter - memBefore) / kMb,
        (memAfter - memBefore) / kNumObjects / 1024,
        poolStats.totalFragmentation() / kMb);
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib::benchmark;

/*
============================================================================
cachelib/benchmarks/objcache/ObjectCacheAllocation.cpprelative  time/iter
iters/s
============================================================================
NOOP                                                         0.00fs  Infinity
----------------------------------------------------------------------------
 mem before: 7 MB, mem after: 668 MB, growth: 661 MB, per-item: 6 KB
full_heap_std_object                                          1.18s  848.29m
----------------------------------------------------------------------------
 serialized item bytes: 1737
 mem before: 675 MB, mem after: 850 MB, growth: 174 MB, per-item: 1 KB,
fragmentation: 5 MB serialized_std_object 1.08s  928.00m serialized item bytes:
675 mem before: 675 MB, mem after: 748 MB, growth: 72 MB, per-item: 0 KB,
fragmentation: 4 MB compact_serialized_std_object                    108.08%
997.07ms     1.00 serialized item bytes: 1737 mem before: 675 MB, mem after: 850
MB, growth: 174 MB, per-item: 1 KB, fragmentation: 5 MB
serialized_custom_object_with_alloc              121.99%      1.13s  884.25m
----------------------------------------------------------------------------
 item bytes: 0, total occupied bytes: 282
 mem before: 675 MB, mem after: 1340 MB, growth: 664 MB, per-item: 6 KB,
fragmentation: 1 MB std_object 1.21s  828.22m item bytes: 6650, total occupied
bytes: 8110 mem before: 1308 MB, mem after: 2137 MB, growth: 829 MB, per-item: 8
KB, fragmentation: 32 MB
std_object_with_alloc                             49.09%      2.46s  406.55m
 item bytes: 6650, total occupied bytes: 6964
 mem before: 1308 MB, mem after: 2017 MB, growth: 709 MB, per-item: 7 KB,
fragmentation: 40 MB std_object_with_alloc_with_compaction             56.20%
2.15s  465.47m item bytes: 5860, total occupied bytes: 16502 mem before: 1308
MB, mem after: 2984 MB, growth: 1676 MB, per-item: 17 KB, fragmentation: 72 MB
custom_object_with_alloc                          39.05%      3.09s  323.42m
 item bytes: 5844, total occupied bytes: 6118
 mem before: 1308 MB, mem after: 1915 MB, growth: 607 MB, per-item: 6 KB,
fragmentation: 19 MB custom_object_with_alloc_with_compaction          66.45%
1.82s  550.32m
----------------------------------------------------------------------------
============================================================================
*/

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

// Note that this leaks memory because there're heap allocations and we don't
// clean them up after tearing down cache. But that's okay in this benchmark
// as we just want to measure the memory usage.
BENCHMARK(full_heap_std_object) { testFullHeapSize<StdObject>(); }
BENCHMARK_DRAW_LINE();

BENCHMARK(serialized_std_object) {
  testSerializedObjectSize<StdObject, apache::thrift::BinarySerializer>();
}
BENCHMARK_RELATIVE(compact_serialized_std_object) {
  testSerializedObjectSize<StdObject, apache::thrift::CompactSerializer>();
}
BENCHMARK_RELATIVE(serialized_custom_object_with_alloc) {
  testSerializedObjectSize<CustomObjectWithAlloc,
                           apache::thrift::BinarySerializer>();
}
BENCHMARK_DRAW_LINE();

// Note that this leaks memory because there're heap allocations and we don't
// clean them up after tearing down cache. But that's okay in this benchmark
// as we just want to measure the memory usage.
BENCHMARK(std_object) { testObjectSize<StdObject>(); }
BENCHMARK_RELATIVE(std_object_with_alloc) {
  testObjectSize<StdObjectWithAlloc>();
}
BENCHMARK_RELATIVE(std_object_with_alloc_with_compaction) {
  testObjectSizeWithCompaction<StdObjectWithAlloc>();
}
BENCHMARK_RELATIVE(custom_object_with_alloc) {
  testObjectSize<CustomObjectWithAlloc>();
}
BENCHMARK_RELATIVE(custom_object_with_alloc_with_compaction) {
  testObjectSizeWithCompaction<CustomObjectWithAlloc>();
}
BENCHMARK_DRAW_LINE();

int main() { folly::runBenchmarks(); }
