#include <folly/Benchmark.h>
#include <folly/Random.h>
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

constexpr uint64_t kReadLoops = 5;
constexpr uint64_t kNumObjects = 100'000;
constexpr uint64_t kCacheSize = 10 * 1024 * 1024 * 1024ul; // 10GB
const std::string kString = "hello I am a long lonnnnnnnnnnnnng string";

LruAllocator::Config getConfig() {
  LruAllocator::Config config;
  config.setCacheSize(kCacheSize);
  config.setAccessConfig(LruAllocator::AccessConfig{20, 10});
  config.configureChainedItems(LruAllocator::AccessConfig{20, 10}, 10);
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
  using Cache =
      folly::EvictingCacheMap<std::string, std::shared_ptr<ObjectType>>;
  std::unique_ptr<Cache> cache;
  BENCHMARK_SUSPEND {
    cache = std::make_unique<Cache>(kNumObjects);

    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = std::make_shared<ObjectType>();
      updateObject(obj);
      cache->set(folly::sformat("key_{}", i), std::move(obj));
    }
  }

  for (uint64_t loop = 0; loop < kReadLoops; loop++) {
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto res = cache->get(folly::sformat("key_{}", i));
      folly::doNotOptimizeAway(res);
    }
  }

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

    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = std::make_unique<ObjectType>();
      updateObject(obj);

      auto iobuf = Serializer::serializeToIOBuf<ObjectType, Protocol>(*obj);
      auto hdl = cache->allocate(
          0 /* default pool */, folly::sformat("key_{}", i), iobuf->length());
      XCHECK(hdl);
      std::memcpy(hdl->getMemory(), iobuf->data(), iobuf->length());
      cache->insertOrReplace(hdl);
    }
  }

  for (uint64_t loop = 0; loop < kReadLoops; loop++) {
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto hdl = cache->find(folly::sformat("key_{}", i));
      Deserializer deserializer{
          reinterpret_cast<const uint8_t*>(hdl->getMemory()),
          reinterpret_cast<const uint8_t*>(hdl->getMemory()) + hdl->getSize()};
      auto obj = deserializer.deserialize<ObjectType, Protocol>();
      folly::doNotOptimizeAway(obj);
    }
  }
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

    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = cache->create<ObjectType>(0 /* pool id */,
                                           folly::sformat("key_{}", i));
      XCHECK(obj);
      updateObject(obj);
      cache->insertOrReplace(obj);
    }
  }
  for (uint64_t loop = 0; loop < kReadLoops; loop++) {
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = cache->find<ObjectType>(folly::sformat("key_{}", i));
      folly::doNotOptimizeAway(obj);
    }
  }
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

    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto fullObj = cache->create<ObjectType>(0 /* poolId */,
                                               folly::sformat("key_{}", i));
      updateObject(fullObj);
      auto obj = cache->createCompact<ObjectType>(
          0 /* pool id */, folly::sformat("key_{}", i), *fullObj);
      XCHECK(obj);
      cache->insertOrReplace(obj);
    }
  }

  for (uint64_t loop = 0; loop < kReadLoops; loop++) {
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = cache->find<ObjectType>(folly::sformat("key_{}", i));
      folly::doNotOptimizeAway(obj);
    }
  }
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib::benchmark;

/*
============================================================================
cachelib/benchmarks/objcache/ObjectCacheRead.cpprelative  time/iter  iters/s
============================================================================
NOOP                                                         0.00fs  Infinity
----------------------------------------------------------------------------
full_heap_std_object                                       231.75ms     4.32
serialized_std_object                             11.18%      2.07s  482.29m
compact_serialized_std_object                     10.02%      2.31s  432.47m
serialized_custom_object_with_alloc               11.02%      2.10s  475.64m
std_object_with_alloc                             28.02%   827.21ms     1.21
std_object_with_alloc_with_compaction             85.56%   270.86ms     3.69
custom_object_with_alloc                          20.31%      1.14s  876.36m
custom_object_with_alloc_with_compaction         105.03%   220.65ms     4.53
----------------------------------------------------------------------------
============================================================================
*/

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

// Note that this leaks memory because there're heap allocations and we don't
// clean them up after tearing down cache. But that's okay in this benchmark
// as we just want to measure the memory usage.
BENCHMARK(full_heap_std_object) { testFullHeapSize<StdObject>(); }
BENCHMARK_RELATIVE(serialized_std_object) {
  testSerializedObjectSize<StdObject, apache::thrift::BinarySerializer>();
}
BENCHMARK_RELATIVE(compact_serialized_std_object) {
  testSerializedObjectSize<StdObject, apache::thrift::CompactSerializer>();
}
BENCHMARK_RELATIVE(serialized_custom_object_with_alloc) {
  testSerializedObjectSize<CustomObjectWithAlloc,
                           apache::thrift::BinarySerializer>();
}

// Note that this leaks memory because there're heap allocations and we don't
// clean them up after tearing down cache. But that's okay in this benchmark
// as we just want to measure the memory usage.
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
