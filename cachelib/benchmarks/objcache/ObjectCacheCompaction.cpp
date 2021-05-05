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

constexpr uint64_t kLoops = 10;
constexpr uint64_t kNumObjects = 10'000;
constexpr uint64_t kCacheSize = 10 * 1024 * 1024 * 1024ul; // 10GB
const std::string kString = "hello I am a long lonnnnnnnnnnnnng string";

std::unique_ptr<LruObjectCache> getCache() {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(kCacheSize);
  cacheAllocatorConfig.setAccessConfig(LruAllocator::AccessConfig{20, 10});
  cacheAllocatorConfig.configureChainedItems(LruAllocator::AccessConfig{20, 10},
                                             10);
  cacheAllocatorConfig.setDefaultAllocSizes(1.08, 1024 * 1024, 64, false);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.getCacheAllocatorConfig().setDefaultAllocSizes(1.08, 1024 * 1024, 64,
                                                        false);
  auto cache = std::make_unique<LruObjectCache>(config);
  cache->getCacheAlloc().addPool(
      "default", cache->getCacheAlloc().getCacheMemoryStats().cacheSize);
  return cache;
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
void createObject() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND { cache = getCache(); }
  for (uint64_t loop = 0; loop < kLoops; loop++) {
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = cache->create<ObjectType>(0 /* pool id */,
                                           folly::sformat("key_{}", i));
      updateObject(obj);
      cache->insertOrReplace(obj);
    }
  }
}

template <typename ObjectType>
void copyObject() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND { cache = getCache(); }
  for (uint64_t loop = 0; loop < kLoops; loop++) {
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto tmp = std::make_unique<ObjectType>();
      BENCHMARK_SUSPEND { updateObject(tmp); }
      auto obj = cache->create<ObjectType>(0 /* pool id */,
                                           folly::sformat("key_{}", i), *tmp);
      cache->insertOrReplace(obj);
    }
  }
}

template <typename ObjectType>
void compactObject() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND { cache = getCache(); }
  for (uint64_t loop = 0; loop < kLoops; loop++) {
    BENCHMARK_SUSPEND {
      for (uint64_t i = 0; i < kNumObjects; i++) {
        auto obj = cache->create<ObjectType>(0 /* pool id */,
                                             folly::sformat("key_{}", i));
        updateObject(obj);
        cache->insertOrReplace(obj);
      }
    }
    for (uint64_t i = 0; i < kNumObjects; i++) {
      auto obj = cache->find<ObjectType>(folly::sformat("key_{}", i));
      auto compactedObj = cache->createCompact<ObjectType>(
          0 /* pool id */, obj.viewItemHandle()->getKey(), *obj);
      cache->insertOrReplace(compactedObj);
    }
  }
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib::benchmark;

/*
============================================================================
cachelib/benchmarks/objcache/ObjectCacheCompaction.cpprelative  time/iter
iters/s
============================================================================
NOOP                                                         0.00fs  Infinity
----------------------------------------------------------------------------
create_object                                                 1.45s  689.49m
copy_object                                       98.24%      1.48s  677.37m
compact_object                                   137.21%      1.06s  946.02m
============================================================================
*/

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

BENCHMARK(create_object) { createObject<StdObjectWithAlloc>(); }
BENCHMARK_RELATIVE(copy_object) { copyObject<StdObjectWithAlloc>(); }
BENCHMARK_RELATIVE(compact_object) { compactObject<StdObjectWithAlloc>(); }

int main() { folly::runBenchmarks(); }
