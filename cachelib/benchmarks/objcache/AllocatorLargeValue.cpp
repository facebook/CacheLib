#include <folly/Benchmark.h>
#include <folly/container/F14Map.h>

#include <map>
#include <scoped_allocator>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/experimental/objcache/Allocator.h"

namespace facebook {
namespace cachelib {
namespace benchmark {
using namespace facebook::cachelib::objcache;
using Mbr = MonotonicBufferResource<CacheDescriptor<LruAllocator>>;
using StringAlloc = Allocator<char, Mbr>;
using String = std::basic_string<char, std::char_traits<char>, StringAlloc>;
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

namespace std {
template <>
struct hash<facebook::cachelib::benchmark::String> {
  std::size_t operator()(
      facebook::cachelib::benchmark::String const& s) const noexcept {
    return std::hash<std::string_view>{}({s.data(), s.size()});
  }
};
} // namespace std

namespace facebook {
namespace cachelib {
namespace benchmark {
using namespace facebook::cachelib::objcache;

constexpr int kLoopCount = 2000;

namespace {
// A simple allocator that uses the global allocator. This is just for testing
// to assess the performance of a barebone custom allocator.
class SimpleAllocatorResource {
 public:
  void* allocate(size_t bytes, size_t /* alignment */) {
    return std::allocator<uint8_t>{}.allocate(bytes);
  }

  void deallocate(void* alloc, size_t bytes, size_t /* alignment */) {
    std::allocator<uint8_t>{}.deallocate(reinterpret_cast<uint8_t*>(alloc),
                                         bytes);
  }

  bool isEqual(const SimpleAllocatorResource&) const noexcept { return true; }
};

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

void testGlobalAllocatorVectorAllocation() {
  using IntVec = std::vector<int>;
  for (int loop = 0; loop < kLoopCount; loop++) {
    IntVec intVec;
    for (int i = 0; i < 1'000'000; i++) {
      intVec.push_back(i);
    }
  }
}

void testSimpleCustomAllocatorVectorAllocation() {
  using IntVec = std::vector<int, Allocator<int, SimpleAllocatorResource>>;
  for (int loop = 0; loop < kLoopCount; loop++) {
    IntVec intVec;
    for (int i = 0; i < 1'000'000; i++) {
      intVec.push_back(i);
    }
  }
}

void testMonotonicResourceFallbackVectorAllocation() {
  using IntVec = std::vector<int, Allocator<int, Mbr>>;
  for (int loop = 0; loop < kLoopCount; loop++) {
    IntVec intVec;
    for (int i = 0; i < 1'000'000; i++) {
      intVec.push_back(i);
    }
  }
}

void testMonotonicResourceCachelibVectorAllocation() {
  using IntVec = std::vector<int, Allocator<int, Mbr>>;
  for (int loop = 0; loop < kLoopCount; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my vec", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    IntVec intVec{Allocator<int, Mbr>{mbr}};
    for (int i = 0; i < 1'000'000; i++) {
      intVec.push_back(i);
    }
    if (loop == 0) {
      auto* metadata = mbr.viewMetadata();
      uint32_t totalStorageBytes = hdl->getSize();
      auto chainedAllocs = cache->viewAsChainedAllocs(hdl);
      for (const auto& c : chainedAllocs.getChain()) {
        totalStorageBytes += c.getSize();
      }
      XLOGF(INFO, "used bytes: {}, total storage bytes: {}",
            metadata->usedBytes, totalStorageBytes);
    }
  }
}

void testGlobalAllocatorMapAllocation() {
  using Map = std::map<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
  }
}

void testSimpleCustomAllocatorMapAllocation() {
  using Map = std::map<
      int, std::string, std::less<int>,
      Allocator<std::pair<const int, std::string>, SimpleAllocatorResource>>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
  }
}

void testMonotonicResourceFallbackMapAllocation() {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::map<int, String, std::less<int>, ScopedAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, String{kText.begin(), kText.end()}));
    }
  }
}

void testMonotonicResourceCachelibMapAllocation() {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::map<int, String, std::less<int>, ScopedAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my map", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    Map aMap(ScopedAlloc{mbr});
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(
          i, String{kText.begin(), kText.end(), ScopedAlloc{mbr}}));
    }
    if (loop == 0) {
      auto* metadata = mbr.viewMetadata();
      uint32_t totalStorageBytes = hdl->getSize();
      auto chainedAllocs = cache->viewAsChainedAllocs(hdl);
      for (const auto& c : chainedAllocs.getChain()) {
        totalStorageBytes += c.getSize();
      }
      XLOGF(INFO, "used bytes: {}, total storage bytes: {}",
            metadata->usedBytes, totalStorageBytes);
    }
  }
}

void testGlobalAllocatorUMapAllocation() {
  using Map = std::unordered_map<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
  }
}

void testSimpleCustomAllocatorUMapAllocation() {
  using Map = std::unordered_map<
      int, std::string, std::hash<int>, std::equal_to<int>,
      Allocator<std::pair<const int, std::string>, SimpleAllocatorResource>>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
  }
}

void testMonotonicResourceFallbackUMapAllocation() {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::unordered_map<int, String, std::hash<int>,
                                 std::equal_to<int>, ScopedAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, String{kText.begin(), kText.end()}));
    }
  }
}

void testMonotonicResourceCachelibUMapAllocation() {
  using Alloc = Allocator<char, Mbr>;
  using ScopedAlloc = std::scoped_allocator_adaptor<Alloc>;
  using Map = std::unordered_map<int, String, std::hash<int>,
                                 std::equal_to<int>, ScopedAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my map", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    Map aMap(ScopedAlloc{mbr});
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(
          i, String{kText.begin(), kText.end(), ScopedAlloc{mbr}}));
    }
    if (loop == 0) {
      auto* metadata = mbr.viewMetadata();
      uint32_t totalStorageBytes = hdl->getSize();
      auto chainedAllocs = cache->viewAsChainedAllocs(hdl);
      for (const auto& c : chainedAllocs.getChain()) {
        totalStorageBytes += c.getSize();
      }
      XLOGF(INFO, "used bytes: {}, total storage bytes: {}",
            metadata->usedBytes, totalStorageBytes);
    }
  }
}

void testGlobalAllocatorF14MapAllocation() {
  using Map = folly::F14FastMap<int, std::string>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
  }
}

void testSimpleCustomAllocatorF14MapAllocation() {
  using Map = folly::F14FastMap<
      int, std::string, std::hash<int>, std::equal_to<int>,
      Allocator<std::pair<const int, std::string>, SimpleAllocatorResource>>;
  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, std::string{kText.begin(), kText.end()}));
    }
  }
}

void testMonotonicResourceFallbackF14MapAllocation() {
  using MapAlloc = Allocator<std::pair<const int, String>, Mbr>;
  using Map = folly::F14FastMap<int, String, std::hash<int>, std::equal_to<int>,
                                MapAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    Map aMap;
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(i, String{kText.begin(), kText.end()}));
    }
  }
}

void testMonotonicResourceCachelibF14MapAllocation() {
  using MapAlloc = Allocator<std::pair<const int, String>, Mbr>;
  using Map = folly::F14FastMap<int, String, std::hash<int>, std::equal_to<int>,
                                MapAlloc>;

  const folly::StringPiece kText{"12345678901234567890"};
  for (int loop = 0; loop < kLoopCount; loop++) {
    auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
        *cache, poolId, "my map", 0 /* reserved bytes */,
        0 /* additional bytes */, 1 /* alignment */);
    Map aMap(MapAlloc{mbr});
    for (int i = 0; i < 10'000; i++) {
      aMap.insert(std::make_pair(
          i, String{kText.begin(), kText.end(), StringAlloc{mbr}}));
    }
    if (loop == 0) {
      auto* metadata = mbr.viewMetadata();
      uint32_t totalStorageBytes = hdl->getSize();
      auto chainedAllocs = cache->viewAsChainedAllocs(hdl);
      for (const auto& c : chainedAllocs.getChain()) {
        totalStorageBytes += c.getSize();
      }
      XLOGF(INFO, "used bytes: {}, total storage bytes: {}",
            metadata->usedBytes, totalStorageBytes);
    }
  }
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

/*
============================================================================
cachelib/benchmarks/objcache/Allocator.cpp      relative  time/iter  iters/s
============================================================================
global_allocator_vector_allocation                            2.05s  488.56m
simple_custom_allocator_vector_allocation         89.79%      2.28s  438.66m
monotonic_resource_fallback_vector_allocation     31.40%      6.52s  153.42m
monotonic_resource_cachelib_vector_allocation     31.62%      6.47s  154.46m
============================================================================
# cachelib-backed allocator stats:
fast allocs: 2, slow allocs: 19,
allocated bytes: 8388604, total storage bytes: 12581040

MonotonicBufferResource has too much code and I wasn't able to get it
inlined sufficiently so that the vector allocation path is completely
inlined. Even if we inline all "allocate" functions in our control,
vector's `_M_realloc_insert` function is still not inlined. Neither is
`allocator_traits<>::allocate()` function. Not able to inline hurts
the performance significantly. We're only half as fast as a fully
inlined vector allocation path.

See the assembly code below.
   1. Fully Inlined     - P161278494
   2. Not Inlined       - P161280463
   3. Partially Inlined - P161282577
I was able to get (1) by deleting the "slow allocate" path. (2) is
what we have now. (3) is if I add FOLLY_ALWAYS_INLINE to all the
allocation path. (2) and (3) perform identical. (1) performs as fast
as the "global" or "simple_custom" allocator.
*/
BENCHMARK(global_allocator_vector_allocation) {
  facebook::cachelib::benchmark::testGlobalAllocatorVectorAllocation();
}
BENCHMARK_RELATIVE(simple_custom_allocator_vector_allocation) {
  facebook::cachelib::benchmark::testSimpleCustomAllocatorVectorAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_fallback_vector_allocation) {
  facebook::cachelib::benchmark::
      testMonotonicResourceFallbackVectorAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_cachelib_vector_allocation) {
  facebook::cachelib::benchmark::
      testMonotonicResourceCachelibVectorAllocation();
}
BENCHMARK_DRAW_LINE();

/*
============================================================================
cachelib/benchmarks/objcache/Allocator.cpp      relative  time/iter  iters/s
============================================================================
global_allocator_map_allocation                               4.03s  248.02m
simple_custom_allocator_map_allocation           100.80%      4.00s  250.00m
monotonic_resource_fallback_map_allocation        97.55%      4.13s  241.94m
monotonic_resource_cachelib_map_allocation       142.72%      2.82s  353.98m
============================================================================
# cachelib-backed allocator stats:
fast allocs: 19989, slow allocs: 11,
allocated bytes: 1010000, total storage bytes: 1988592
*/
BENCHMARK(global_allocator_map_allocation) {
  facebook::cachelib::benchmark::testGlobalAllocatorMapAllocation();
}
BENCHMARK_RELATIVE(simple_custom_allocator_map_allocation) {
  facebook::cachelib::benchmark::testSimpleCustomAllocatorMapAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_fallback_map_allocation) {
  facebook::cachelib::benchmark::testMonotonicResourceFallbackMapAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_cachelib_map_allocation) {
  facebook::cachelib::benchmark::testMonotonicResourceCachelibMapAllocation();
}
BENCHMARK_DRAW_LINE();

/*
============================================================================
cachelib/benchmarks/objcache/Allocator.cpp      relative  time/iter  iters/s
============================================================================
global_allocator_umap_allocation                              2.23s  449.19m
simple_custom_allocator_umap_allocation          106.06%      2.10s  476.41m
monotonic_resource_fallback_umap_allocation       86.59%      2.57s  388.96m
monotonic_resource_cachelib_umap_allocation      152.62%      1.46s  685.57m
============================================================================
# cachelib-backed allocator stats:
fast allocs: 20001, slow allocs: 11,
allocated bytes: 973744, total storage bytes: 2177472
*/
BENCHMARK(global_allocator_umap_allocation) {
  facebook::cachelib::benchmark::testGlobalAllocatorUMapAllocation();
}
BENCHMARK_RELATIVE(simple_custom_allocator_umap_allocation) {
  facebook::cachelib::benchmark::testSimpleCustomAllocatorUMapAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_fallback_umap_allocation) {
  facebook::cachelib::benchmark::testMonotonicResourceFallbackUMapAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_cachelib_umap_allocation) {
  facebook::cachelib::benchmark::testMonotonicResourceCachelibUMapAllocation();
}
BENCHMARK_DRAW_LINE();

/*
============================================================================
cachelib/benchmarks/objcache/Allocator.cpp      relative  time/iter  iters/s
============================================================================
global_allocator_f14map_allocation                            1.59s  628.21m
simple_custom_allocator_f14map_allocation        110.63%      1.44s  694.95m
monotonic_resource_fallback_f14map_allocation     86.87%      1.83s  545.69m
monotonic_resource_cachelib_f14map_allocation    124.63%      1.28s  782.92m
============================================================================
# cachelib-backed allocator stats:
fast allocs: 10001, slow allocs: 14,
allocated bytes: 1325664, total storage bytes: 2659980
*/
BENCHMARK(global_allocator_f14map_allocation) {
  facebook::cachelib::benchmark::testGlobalAllocatorF14MapAllocation();
}
BENCHMARK_RELATIVE(simple_custom_allocator_f14map_allocation) {
  facebook::cachelib::benchmark::testSimpleCustomAllocatorF14MapAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_fallback_f14map_allocation) {
  facebook::cachelib::benchmark::
      testMonotonicResourceFallbackF14MapAllocation();
}
BENCHMARK_RELATIVE(monotonic_resource_cachelib_f14map_allocation) {
  facebook::cachelib::benchmark::
      testMonotonicResourceCachelibF14MapAllocation();
}
BENCHMARK_DRAW_LINE();

int main() {
  facebook::cachelib::benchmark::setupCache();
  folly::runBenchmarks();
}
