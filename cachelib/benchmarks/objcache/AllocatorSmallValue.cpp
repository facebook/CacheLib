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

void testGlobalAllocatorVector(int numLoops, int numElems, bool copy) {
  using IntVec = std::vector<int>;
  for (int loop = 0; loop < numLoops; loop++) {
    IntVec intVec;
    for (int i = 0; i < numElems; i++) {
      intVec.push_back(i);
    }
    if (copy) {
      auto v2 = intVec;
      folly::doNotOptimizeAway(v2);
    }
  }
}

void testSerializedVector(int numLoops, int numElems, bool frozen) {
  for (int loop = 0; loop < numLoops; loop++) {
    Vector v;
    for (int i = 0; i < numElems; i++) {
      v.m_ref().value().push_back(i);
    }
    if (frozen) {
      auto str = apache::thrift::frozen::freezeToString(v);
      folly::doNotOptimizeAway(str);
      auto view = apache::thrift::frozen::mapFrozen<Vector>(
          folly::StringPiece{str.data(), str.size()});
      folly::doNotOptimizeAway(view);
    } else {
      auto iobuf = Serializer::serializeToIOBuf(v);
      folly::doNotOptimizeAway(iobuf);
    }
  }
}

void testCachelibAllocatorVector(int numLoops, int numElems) {
  using IntVec = std::vector<int, Allocator<int, Mbr>>;
  for (int loop = 0; loop < numLoops; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my vec", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    IntVec intVec{Allocator<int, Mbr>{mbr}};
    for (int i = 0; i < numElems; i++) {
      intVec.push_back(i);
    }
  }
}

void testGlobalAllocatorMap(int numLoops, int numElems, bool copy) {
  using Map = std::map<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < numLoops; loop++) {
    Map aMap;
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, kText));
    }
    if (copy) {
      auto m2 = aMap;
      folly::doNotOptimizeAway(m2);
    }
  }
}

void testSerializedMap(int numLoops, int numElems, bool frozen) {
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < numLoops; loop++) {
    Map m;
    for (int i = 0; i < numElems; i++) {
      m.m_ref().value().insert(std::make_pair(i, kText));
    }
    if (frozen) {
      auto str = apache::thrift::frozen::freezeToString(m);
      folly::doNotOptimizeAway(str);
      auto view = apache::thrift::frozen::mapFrozen<Vector>(
          folly::StringPiece{str.data(), str.size()});
      folly::doNotOptimizeAway(view);
    } else {
      auto iobuf = Serializer::serializeToIOBuf(m);
      folly::doNotOptimizeAway(iobuf);
    }
  }
}

void testCachelibAllocatorMap(int numLoops, int numElems) {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::map<int, String, std::less<int>, ScopedAlloc>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < numLoops; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my map", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    Map aMap(ScopedAlloc{mbr});
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, String{kText, StringAlloc{mbr}}));
    }
  }
}

void testGlobalAllocatorUMap(int numLoops, int numElems, bool copy) {
  using Map = std::unordered_map<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < numLoops; loop++) {
    Map aMap;
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
    if (copy) {
      auto m2 = aMap;
      folly::doNotOptimizeAway(m2);
    }
  }
}

void testSerializedUMap(int numLoops, int numElems, bool frozen) {
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < numLoops; loop++) {
    UMap m;
    for (int i = 0; i < numElems; i++) {
      m.m_ref().value().insert(std::make_pair(i, kText));
    }
    if (frozen) {
      auto str = apache::thrift::frozen::freezeToString(m);
      folly::doNotOptimizeAway(str);
      auto view = apache::thrift::frozen::mapFrozen<Vector>(
          folly::StringPiece{str.data(), str.size()});
      folly::doNotOptimizeAway(view);
    } else {
      auto iobuf = Serializer::serializeToIOBuf(m);
      folly::doNotOptimizeAway(iobuf);
    }
  }
}

void testCachelibAllocatorUMap(int numLoops, int numElems) {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::unordered_map<int, String, std::hash<int>,
                                 std::equal_to<int>, ScopedAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < numLoops; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my map", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    Map aMap(ScopedAlloc{mbr});
    for (int i = 0; i < numElems; i++) {
      aMap.insert(std::make_pair(
          i, String{kText.begin(), kText.end(), ScopedAlloc{mbr}}));
    }
  }
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

/*
============================================================================
AllocatorSmallValue.cpprelative                           time/iter  iters/s
============================================================================
NOOP                                                         0.00fs  Infinity
----------------------------------------------------------------------------
global_vector_10                                            37.51ms    26.66
copy_vector_10                                    84.42%    44.43ms    22.51
serialized_vector_10                              34.12%   109.93ms     9.10
frozen_vector_10                                   2.65%      1.42s  705.53m
cl_vector_10                                       1.71%      2.20s  455.03m
----------------------------------------------------------------------------
global_vector_100                                           19.45ms    51.41
copy_vector_100                                   45.36%    42.88ms    23.32
serialized_vector_100                             35.90%    54.19ms    18.45
frozen_vector_100                                  2.87%   678.49ms     1.47
cl_vector_100                                      2.81%   692.96ms     1.44
----------------------------------------------------------------------------
global_vector_1000                                          94.20ms    10.62
copy_vector_1000                                  74.21%   126.94ms     7.88
serialized_vector_1000                            40.81%   230.84ms     4.33
frozen_vector_1000                                 3.74%      2.52s  396.76m
cl_vector_1000                                    15.04%   626.45ms     1.60
----------------------------------------------------------------------------
global_map_10                                              209.76ms     4.77
copy_map_10                                       59.91%   350.13ms     2.86
serialized_map_10                                 63.03%   332.77ms     3.01
frozen_map_10                                      6.18%      3.40s  294.51m
cl_map_10                                          8.10%      2.59s  386.03m
----------------------------------------------------------------------------
global_map_100                                             680.36ms     1.47
copy_map_100                                      68.28%   996.44ms     1.00
serialized_map_100                                88.42%   769.50ms     1.30
frozen_map_100                                    17.94%      3.79s  263.70m
cl_map_100                                        49.30%      1.38s  724.59m
----------------------------------------------------------------------------
global_map_1000                                               1.38s  724.00m
copy_map_1000                                     60.06%      2.30s  434.80m
serialized_map_1000                               85.98%      1.61s  622.48m
frozen_map_1000                                   30.95%      4.46s  224.04m
cl_map_1000                                      118.19%      1.17s  855.71m
----------------------------------------------------------------------------
global_umap_10                                             335.02ms     2.98
copy_umap_10                                      63.35%   528.85ms     1.89
serialized_umap_10                               104.00%   322.13ms     3.10
frozen_umap_10                                    10.01%      3.35s  298.89m
cl_umap_10                                        12.59%      2.66s  375.74m
----------------------------------------------------------------------------
global_umap_100                                            704.11ms     1.42
copy_umap_100                                     66.53%      1.06s  944.88m
serialized_umap_100                               92.38%   762.22ms     1.31
frozen_umap_100                                   17.58%      4.00s  249.70m
cl_umap_100                                       51.41%      1.37s  730.09m
----------------------------------------------------------------------------
global_umap_1000                                              1.09s  918.94m
copy_umap_1000                                    57.69%      1.89s  530.14m
serialized_umap_1000                              68.94%      1.58s  633.50m
frozen_umap_1000                                  24.81%      4.39s  227.96m
cl_umap_1000                                     131.15%   829.77ms     1.21
----------------------------------------------------------------------------
============================================================================
*/

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

BENCHMARK(global_vector_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(500'000, 10, false);
}
BENCHMARK_RELATIVE(copy_vector_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(500'000, 10, true);
}
BENCHMARK_RELATIVE(serialized_vector_10) {
  facebook::cachelib::benchmark::testSerializedVector(500'000, 10, false);
}
BENCHMARK_RELATIVE(frozen_vector_10) {
  facebook::cachelib::benchmark::testSerializedVector(500'000, 10, true);
}
BENCHMARK_RELATIVE(cl_vector_10) {
  facebook::cachelib::benchmark::testCachelibAllocatorVector(500'000, 10);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_vector_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(100'000, 100, false);
}
BENCHMARK_RELATIVE(copy_vector_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(100'000, 100, true);
}
BENCHMARK_RELATIVE(serialized_vector_100) {
  facebook::cachelib::benchmark::testSerializedVector(100'000, 100, false);
}
BENCHMARK_RELATIVE(frozen_vector_100) {
  facebook::cachelib::benchmark::testSerializedVector(100'000, 100, true);
}
BENCHMARK_RELATIVE(cl_vector_100) {
  facebook::cachelib::benchmark::testCachelibAllocatorVector(100'000, 100);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_vector_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(50'000, 1000, false);
}
BENCHMARK_RELATIVE(copy_vector_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorVector(50'000, 1000, true);
}
BENCHMARK_RELATIVE(serialized_vector_1000) {
  facebook::cachelib::benchmark::testSerializedVector(50'000, 1000, false);
}
BENCHMARK_RELATIVE(frozen_vector_1000) {
  facebook::cachelib::benchmark::testSerializedVector(50'000, 1000, true);
}
BENCHMARK_RELATIVE(cl_vector_1000) {
  facebook::cachelib::benchmark::testCachelibAllocatorVector(50'000, 1000);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(global_map_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(500'000, 10, false);
}
BENCHMARK_RELATIVE(copy_map_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(500'000, 10, true);
}
BENCHMARK_RELATIVE(serialized_map_10) {
  facebook::cachelib::benchmark::testSerializedMap(500'000, 10, false);
}
BENCHMARK_RELATIVE(frozen_map_10) {
  facebook::cachelib::benchmark::testSerializedMap(500'000, 10, true);
}
BENCHMARK_RELATIVE(cl_map_10) {
  facebook::cachelib::benchmark::testCachelibAllocatorMap(500'000, 10);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_map_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(100'000, 100, false);
}
BENCHMARK_RELATIVE(copy_map_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(100'000, 100, true);
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
  facebook::cachelib::benchmark::testGlobalAllocatorMap(10'000, 1000, false);
}
BENCHMARK_RELATIVE(copy_map_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorMap(10'000, 1000, true);
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
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(500'000, 10, false);
}
BENCHMARK_RELATIVE(copy_umap_10) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(500'000, 10, true);
}
BENCHMARK_RELATIVE(serialized_umap_10) {
  facebook::cachelib::benchmark::testSerializedMap(500'000, 10, false);
}
BENCHMARK_RELATIVE(frozen_umap_10) {
  facebook::cachelib::benchmark::testSerializedMap(500'000, 10, true);
}
BENCHMARK_RELATIVE(cl_umap_10) {
  facebook::cachelib::benchmark::testCachelibAllocatorUMap(500'000, 10);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_umap_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(100'000, 100, false);
}
BENCHMARK_RELATIVE(copy_umap_100) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(100'000, 100, true);
}
BENCHMARK_RELATIVE(serialized_umap_100) {
  facebook::cachelib::benchmark::testSerializedMap(100'000, 100, false);
}
BENCHMARK_RELATIVE(frozen_umap_100) {
  facebook::cachelib::benchmark::testSerializedMap(100'000, 100, true);
}
BENCHMARK_RELATIVE(cl_umap_100) {
  facebook::cachelib::benchmark::testCachelibAllocatorUMap(100'000, 100);
}
BENCHMARK_DRAW_LINE();
BENCHMARK(global_umap_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(10'000, 1000, false);
}
BENCHMARK_RELATIVE(copy_umap_1000) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMap(10'000, 1000, true);
}
BENCHMARK_RELATIVE(serialized_umap_1000) {
  facebook::cachelib::benchmark::testSerializedMap(10'000, 1000, false);
}
BENCHMARK_RELATIVE(frozen_umap_1000) {
  facebook::cachelib::benchmark::testSerializedMap(10'000, 1000, true);
}
BENCHMARK_RELATIVE(cl_umap_1000) {
  facebook::cachelib::benchmark::testCachelibAllocatorUMap(10'000, 1000);
}
BENCHMARK_DRAW_LINE();

int main() {
  facebook::cachelib::benchmark::setupCache();
  folly::runBenchmarks();
}
