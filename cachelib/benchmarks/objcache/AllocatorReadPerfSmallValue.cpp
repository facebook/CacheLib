#include <folly/Benchmark.h>
#include <thrift/lib/cpp2/frozen/FrozenUtil.h>

#include <map>
#include <scoped_allocator>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/benchmarks/objcache/gen-cpp2/AllocatorSmallValue_layouts.h"
#include "cachelib/benchmarks/objcache/gen-cpp2/AllocatorSmallValue_types.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/experimental/objcache/Allocator.h"

namespace facebook {
namespace cachelib {
namespace benchmark {
using namespace facebook::cachelib::objcache;
using Mbr = MonotonicBufferResource<CacheDescriptor<LruAllocator>>;
using StringAlloc = Allocator<char, Mbr>;
using String = std::basic_string<char, std::char_traits<char>, StringAlloc>;

namespace {
std::unique_ptr<LruAllocator> cache;
PoolId poolId;
} // namespace

void setupCache() {
  LruAllocator::Config config;

  config.setCacheSize(5ul * 1024ul * 1024ul * 1024ul); // 5 GB

  // 16 million buckets, 1 million locks
  LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                          20 /* locks power */};
  config.setAccessConfig(accessConfig);
  config.configureChainedItems(accessConfig);

  cache = std::make_unique<LruAllocator>(config);
  poolId = cache->addPool("default", cache->getCacheMemoryStats().cacheSize);
}

void testGlobalAllocatorVector(int numLoops, int numElems) {
  using IntVec = std::vector<int>;
  IntVec intVec;
  BENCHMARK_SUSPEND {
    for (int i = 0; i < numElems; i++) {
      intVec.push_back(i);
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    auto ret = intVec[loop % numElems];
    folly::doNotOptimizeAway(ret);
  }
}

void testSerializedVector(int numLoops, int numElems, bool frozen) {
  folly::IOBuf iobuf;
  apache::thrift::frozen::MappedFrozen<Vector> view;
  BENCHMARK_SUSPEND {
    Vector v;
    for (int i = 0; i < numElems; i++) {
      v.m_ref().value().push_back(i);
    }
    if (frozen) {
      auto str = apache::thrift::frozen::freezeToString(v);
      view = apache::thrift::frozen::mapFrozen<Vector>(std::move(str));
    } else {
      iobuf = *Serializer::serializeToIOBuf(v);
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    if (frozen) {
      auto ret = view.m()[loop % numElems];
      folly::doNotOptimizeAway(ret);
    } else {
      Deserializer deserializer{iobuf.data(), iobuf.data() + iobuf.length()};
      Vector v;
      deserializer.deserialize(v);
      auto ret = v.m_ref().value()[loop % numElems];
      folly::doNotOptimizeAway(ret);
    }
  }
}

void testCachelibAllocatorVector(int numLoops, int numElems) {
  using IntVec = std::vector<int, Allocator<int, Mbr>>;
  auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
      *cache, poolId, "my vec", 0 /* reserved bytes */,
      0 /* additional bytes */, 1 /* alignment */);
  IntVec intVec{Allocator<int, Mbr>{mbr}};
  BENCHMARK_SUSPEND {
    for (int i = 0; i < numElems; i++) {
      intVec.push_back(i);
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    auto ret = intVec[loop % numElems];
    folly::doNotOptimizeAway(ret);
  }
}

void testGlobalAllocatorMap(int numLoops, int numElems) {
  using Map = std::map<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  Map aMap;
  BENCHMARK_SUSPEND {
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, kText));
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    auto ret = aMap.find(loop % numElems);
    folly::doNotOptimizeAway(ret);
  }
}

void testSerializedMap(int numLoops, int numElems, bool frozen) {
  folly::IOBuf iobuf;
  apache::thrift::frozen::MappedFrozen<Map> view;
  const folly::StringPiece kText{"12345678901234567890"};
  BENCHMARK_SUSPEND {
    Map v;
    for (int i = 0; i < numElems; i++) {
      v.m_ref().value().insert(std::make_pair(i, kText));
    }
    if (frozen) {
      auto str = apache::thrift::frozen::freezeToString(v);
      view = apache::thrift::frozen::mapFrozen<Map>(std::move(str));
    } else {
      iobuf = *Serializer::serializeToIOBuf(v);
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    if (frozen) {
      auto ret = view.m().find(loop % numElems);
      folly::doNotOptimizeAway(ret);
    } else {
      Deserializer deserializer{iobuf.data(), iobuf.data() + iobuf.length()};
      Map v;
      deserializer.deserialize(v);
      auto ret = v.m_ref().value().find(loop % numElems);
      folly::doNotOptimizeAway(ret);
    }
  }
}

void testCachelibAllocatorMap(int numLoops, int numElems) {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::map<int, String, std::less<int>, ScopedAlloc>;
  const folly::StringPiece kText{"12345678901234567890"};
  auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
      *cache, poolId, "my map", 0 /* reserved bytes */,
      0 /* additional bytes */, 1 /* alignment */);
  Map aMap(ScopedAlloc{mbr});
  BENCHMARK_SUSPEND {
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, kText));
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    auto ret = aMap.find(loop % numElems);
    folly::doNotOptimizeAway(ret);
  }
}

void testGlobalAllocatorUMap(int numLoops, int numElems) {
  using Map = std::unordered_map<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  Map aMap;
  BENCHMARK_SUSPEND {
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, kText));
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    auto ret = aMap.find(loop % numElems);
    folly::doNotOptimizeAway(ret);
  }
}

void testSerializedUMap(int numLoops, int numElems, bool frozen) {
  folly::IOBuf iobuf;
  apache::thrift::frozen::MappedFrozen<UMap> view;
  const folly::StringPiece kText{"12345678901234567890"};
  BENCHMARK_SUSPEND {
    UMap v;
    for (int i = 0; i < numElems; i++) {
      v.m_ref().value().insert(std::make_pair(i, kText));
    }
    if (frozen) {
      auto str = apache::thrift::frozen::freezeToString(v);
      view = apache::thrift::frozen::mapFrozen<UMap>(std::move(str));
    } else {
      iobuf = *Serializer::serializeToIOBuf(v);
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    if (frozen) {
      auto ret = view.m().find(loop % numElems);
      folly::doNotOptimizeAway(ret);
    } else {
      Deserializer deserializer{iobuf.data(), iobuf.data() + iobuf.length()};
      UMap v;
      deserializer.deserialize(v);
      auto ret = v.m_ref().value().find(loop % numElems);
      folly::doNotOptimizeAway(ret);
    }
  }
}

void testCachelibAllocatorUMap(int numLoops, int numElems) {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::unordered_map<int, String, std::hash<int>,
                                 std::equal_to<int>, ScopedAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
      *cache, poolId, "my map", 0 /* reserved bytes */,
      0 /* additional bytes */, 1 /* alignment */);
  Map aMap(ScopedAlloc{mbr});
  BENCHMARK_SUSPEND {
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, kText));
    }
  }
  for (int loop = 0; loop < numLoops; loop++) {
    auto ret = aMap.find(loop % numElems);
    folly::doNotOptimizeAway(ret);
  }
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

/*
============================================================================
AllocatorReadPerfSmallValue.cpprelative                   time/iter  iters/s
============================================================================
NOOP                                                         0.00fs  Infinity
----------------------------------------------------------------------------
global_vector_10                                             2.79ms   358.26
serialized_vector_10                               4.56%    61.27ms    16.32
frozen_vector_10                                  15.46%    18.05ms    55.40
cl_vector_10                                      99.78%     2.80ms   357.49
----------------------------------------------------------------------------
global_vector_100                                            2.79ms   358.32
serialized_vector_100                              0.99%   282.42ms     3.54
frozen_vector_100                                 16.72%    16.69ms    59.92
cl_vector_100                                     99.87%     2.79ms   357.84
----------------------------------------------------------------------------
global_vector_1000                                           2.68ms   372.84
serialized_vector_1000                             0.11%      2.52s  397.49m
frozen_vector_1000                                14.98%    17.91ms    55.85
cl_vector_1000                                    96.02%     2.79ms   357.99
----------------------------------------------------------------------------
global_map_10                                                8.45ms   118.40
serialized_map_10                                  1.35%   624.63ms     1.60
frozen_map_10                                     13.87%    60.88ms    16.43
cl_map_10                                         97.62%     8.65ms   115.58
----------------------------------------------------------------------------
global_map_100                                               1.87ms   533.34
serialized_map_100                                 0.30%   623.13ms     1.60
frozen_map_100                                    22.20%     8.45ms   118.40
cl_map_100                                        98.95%     1.89ms   527.74
----------------------------------------------------------------------------
global_map_1000                                            287.84us    3.47K
serialized_map_1000                                0.03%      1.08s  928.59m
frozen_map_1000                                   21.32%     1.35ms   740.56
cl_map_1000                                      112.15%   256.66us    3.90K
----------------------------------------------------------------------------
global_umap_10                                              11.33ms    88.29
serialized_umap_10                                 1.58%   718.25ms     1.39
frozen_umap_10                                    22.57%    50.19ms    19.93
cl_umap_10                                        97.27%    11.64ms    85.88
----------------------------------------------------------------------------
global_umap_100                                              1.10ms   909.45
serialized_umap_100                                0.17%   633.07ms     1.58
frozen_umap_100                                   20.19%     5.44ms   183.66
cl_umap_100                                      100.38%     1.10ms   912.91
----------------------------------------------------------------------------
global_umap_1000                                           130.66us    7.65K
serialized_umap_1000                               0.01%      1.04s  958.50m
frozen_umap_1000                                  26.24%   497.92us    2.01K
cl_umap_1000                                     121.05%   107.93us    9.27K
----------------------------------------------------------------------------
============================================================================
*/

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

BENCHMARK(global_vector_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(1'000'000, 10);
}
BENCHMARK_RELATIVE(serialized_vector_10) {
  facebook::cachelib::benchmark::testSerializedVector(1'000'000, 10, false);
}
BENCHMARK_RELATIVE(frozen_vector_10) {
  facebook::cachelib::benchmark::testSerializedVector(1'000'000, 10, true);
}
BENCHMARK_RELATIVE(cl_vector_10) {
  facebook::cachelib::benchmark::testCachelibAllocatorVector(1'000'000, 10);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_vector_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(1'000'000, 100);
}
BENCHMARK_RELATIVE(serialized_vector_100) {
  facebook::cachelib::benchmark::testSerializedVector(1'000'000, 100, false);
}
BENCHMARK_RELATIVE(frozen_vector_100) {
  facebook::cachelib::benchmark::testSerializedVector(1'000'000, 100, true);
}
BENCHMARK_RELATIVE(cl_vector_100) {
  facebook::cachelib::benchmark::testCachelibAllocatorVector(1'000'000, 100);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_vector_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(1'000'000, 1000);
}
BENCHMARK_RELATIVE(serialized_vector_1000) {
  facebook::cachelib::benchmark::testSerializedVector(1'000'000, 1000, false);
}
BENCHMARK_RELATIVE(frozen_vector_1000) {
  facebook::cachelib::benchmark::testSerializedVector(1'000'000, 1000, true);
}
BENCHMARK_RELATIVE(cl_vector_1000) {
  facebook::cachelib::benchmark::testCachelibAllocatorVector(1'000'000, 1000);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(global_map_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(1'000'000, 10);
}
BENCHMARK_RELATIVE(serialized_map_10) {
  facebook::cachelib::benchmark::testSerializedMap(1'000'000, 10, false);
}
BENCHMARK_RELATIVE(frozen_map_10) {
  facebook::cachelib::benchmark::testSerializedMap(1'000'000, 10, true);
}
BENCHMARK_RELATIVE(cl_map_10) {
  facebook::cachelib::benchmark::testCachelibAllocatorMap(1'000'000, 10);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_map_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(100'000, 100);
}
BENCHMARK_RELATIVE(serialized_map_100) {
  facebook::cachelib::benchmark::testSerializedMap(100'000, 100, false);
}
BENCHMARK_RELATIVE(frozen_map_100) {
  facebook::cachelib::benchmark::testSerializedMap(100'000, 100, true);
}
BENCHMARK_RELATIVE(cl_map_100) {
  facebook::cachelib::benchmark::testCachelibAllocatorMap(100'000, 100);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_map_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(10'000, 1000);
}
BENCHMARK_RELATIVE(serialized_map_1000) {
  facebook::cachelib::benchmark::testSerializedMap(10'000, 1000, false);
}
BENCHMARK_RELATIVE(frozen_map_1000) {
  facebook::cachelib::benchmark::testSerializedMap(10'000, 1000, true);
}
BENCHMARK_RELATIVE(cl_map_1000) {
  facebook::cachelib::benchmark::testCachelibAllocatorMap(10'000, 1000);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(global_umap_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(1'000'000, 10);
}
BENCHMARK_RELATIVE(serialized_umap_10) {
  facebook::cachelib::benchmark::testSerializedUMap(1'000'000, 10, false);
}
BENCHMARK_RELATIVE(frozen_umap_10) {
  facebook::cachelib::benchmark::testSerializedUMap(1'000'000, 10, true);
}
BENCHMARK_RELATIVE(cl_umap_10) {
  facebook::cachelib::benchmark::testCachelibAllocatorUMap(1'000'000, 10);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_umap_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(100'000, 100);
}
BENCHMARK_RELATIVE(serialized_umap_100) {
  facebook::cachelib::benchmark::testSerializedUMap(100'000, 100, false);
}
BENCHMARK_RELATIVE(frozen_umap_100) {
  facebook::cachelib::benchmark::testSerializedUMap(100'000, 100, true);
}
BENCHMARK_RELATIVE(cl_umap_100) {
  facebook::cachelib::benchmark::testCachelibAllocatorUMap(100'000, 100);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_umap_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(10'000, 1000);
}
BENCHMARK_RELATIVE(serialized_umap_1000) {
  facebook::cachelib::benchmark::testSerializedUMap(10'000, 1000, false);
}
BENCHMARK_RELATIVE(frozen_umap_1000) {
  facebook::cachelib::benchmark::testSerializedUMap(10'000, 1000, true);
}
BENCHMARK_RELATIVE(cl_umap_1000) {
  facebook::cachelib::benchmark::testCachelibAllocatorUMap(10'000, 1000);
}
BENCHMARK_DRAW_LINE();

int main() {
  facebook::cachelib::benchmark::setupCache();
  folly::runBenchmarks();
}
