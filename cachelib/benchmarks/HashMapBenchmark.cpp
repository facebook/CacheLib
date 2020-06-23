#include <unistd.h>

#include <fstream>
#include <iostream>
#include <unordered_map>

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/Portability.h>
#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <tsl/sparse_map.h>

#include "cachelib/navy/block_cache/BTree.h"

using namespace facebook::cachelib::navy;
using namespace facebook::cachelib::navy::details;

struct FOLLY_PACK_ATTR Record {
  uint32_t address{0};
  uint16_t size{0};
  uint8_t currentHits{0};
  uint8_t totalHits{0};

  Record& operator++() {
    address += 3;
    ++currentHits;
    ++totalHits;
    return *this;
  }
};
static_assert(8 == sizeof(Record), "Record size is 8 bytes");

template <typename Key, typename Value>
using BTreeMap = BTree<Key, Value, BTreeTraits<30, 60, 90>>;

template <typename Key, typename Value>
using SparseMap = tsl::sparse_map<Key,
                                  Value,
                                  folly::f14::DefaultHasher<Key>,
                                  std::equal_to<Key>,
                                  std::allocator<std::pair<Key, Value>>,
                                  tsl::sh::power_of_two_growth_policy<2>,
                                  tsl::sh::exception_safety::basic,
                                  tsl::sh::sparsity::high>;

constexpr size_t kKeySpace = 1'000'000;
constexpr size_t kNumKeys = 800'000;

template <class K>
K makeKey() {
  return folly::to<K>(folly::Random::rand32(kKeySpace));
}

template <class K>
const std::vector<K>& makeKeys() {
  // static to reuse storage across iterations.
  static std::vector<K> keys(kNumKeys);
  for (auto& key : keys) {
    key = makeKey<K>();
  }
  return keys;
}

size_t getPageBytes() {
  std::ifstream file("/proc/self/statm");

  size_t pages;
  file >> pages; // Ignore first
  file >> pages;

  return pages * getpagesize();
}

template <class Map>
Map makeMap(size_t& size) {
  using K = typename Map::key_type;
  size_t startBytes = getPageBytes();
  Map m;
  for (size_t i = 0; i < kKeySpace; ++i) {
    ++m[makeKey<K>()];
  }
  size_t endBytes = getPageBytes();
  size = endBytes > startBytes ? endBytes - startBytes : 0;
  return m;
}

template <class K, class V>
BTreeMap<K, V> makeBTreeMap(size_t& size) {
  size_t startBytes = getPageBytes();
  BTreeMap<K, V> bt;
  for (size_t i = 0; i < kKeySpace; ++i) {
    auto k = makeKey<K>();
    V v = 0;
    bt.lookup(k, v);
    bt.insert(k, v + 1);
  }
  size_t endBytes = getPageBytes();
  size = endBytes > startBytes ? endBytes - startBytes : 0;
  return bt;
}

template <class Map>
void mapLookupBench(size_t iters, const Map& map) {
  using K = typename Map::key_type;
  int s = 0;
  for (;;) {
    folly::BenchmarkSuspender setup;
    auto& keys = makeKeys<K>();
    setup.dismiss();

    folly::makeUnpredictable(map);
    for (auto key : keys) {
      if (iters-- == 0) {
        folly::doNotOptimizeAway(s);
        return;
      }
      folly::makeUnpredictable(key);
      auto found = map.find(key);
      if (found != map.end()) {
        ++s;
        folly::doNotOptimizeAway(s);
      }
    }
  }
  folly::doNotOptimizeAway(s);
}

template <class BT>
void btreeLookupBench(size_t iters, const BT& bt) {
  using K = typename BT::Key;
  using V = typename BT::Value;

  int s = 0;
  for (;;) {
    folly::BenchmarkSuspender setup;
    auto& keys = makeKeys<K>();
    setup.dismiss();

    folly::makeUnpredictable(bt);
    for (auto key : keys) {
      if (iters-- == 0) {
        folly::doNotOptimizeAway(s);
        return;
      }
      V value;
      folly::makeUnpredictable(key);
      if (bt.lookup(key, value)) {
        ++s;
        folly::doNotOptimizeAway(s);
      }
    }
  }
  folly::doNotOptimizeAway(s);
}

size_t pageBytesBTree32 = 0;
size_t pageBytesStd32 = 0;
size_t pageBytesF14_32 = 0;
size_t pageBytesTsl32 = 0;

size_t pageBytesBTree64 = 0;
size_t pageBytesStd64 = 0;
size_t pageBytesF14_64 = 0;
size_t pageBytesTsl64 = 0;
size_t pageBytesTslRec = 0;

auto btreeMap_u32 = makeBTreeMap<uint32_t, uint32_t>(pageBytesBTree32);
auto stdMap_u32 =
    makeMap<std::unordered_map<uint32_t, uint32_t>>(pageBytesStd32);
auto f14Map_u32 =
    makeMap<folly::F14ValueMap<uint32_t, uint32_t>>(pageBytesF14_32);
auto tslMap_u32 = makeMap<SparseMap<uint32_t, uint32_t>>(pageBytesTsl32);

auto btreeMap_u64 = makeBTreeMap<uint32_t, uint64_t>(pageBytesBTree64);
auto stdMap_u64 =
    makeMap<std::unordered_map<uint32_t, uint64_t>>(pageBytesStd64);
auto f14Map_u64 =
    makeMap<folly::F14ValueMap<uint32_t, uint64_t>>(pageBytesF14_64);
auto tslMap_u64 = makeMap<SparseMap<uint32_t, uint64_t>>(pageBytesTsl64);
auto tslMap_record = makeMap<SparseMap<uint32_t, Record>>(pageBytesTslRec);

BENCHMARK_PARAM(btreeLookupBench, btreeMap_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, stdMap_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, f14Map_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, tslMap_u32)

BENCHMARK_DRAW_LINE();

BENCHMARK_PARAM(btreeLookupBench, btreeMap_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, stdMap_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, f14Map_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, tslMap_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, tslMap_record)

#if 0
============================================================================
cachelib/benchmarks/HashMapBenchmark.cpp        relative  time/iter  iters/s
============================================================================
btreeLookupBench(btreeMap_u32)                             635.42ns    1.57M
mapLookupBench(stdMap_u32)                       495.48%   128.24ns    7.80M
mapLookupBench(f14Map_u32)                      1637.18%    38.81ns   25.77M
mapLookupBench(tslMap_u32)                      1056.81%    60.13ns   16.63M
----------------------------------------------------------------------------
btreeLookupBench(btreeMap_u64)                             660.51ns    1.51M
mapLookupBench(stdMap_u64)                       445.00%   148.43ns    6.74M
mapLookupBench(f14Map_u64)                      1147.49%    57.56ns   17.37M
mapLookupBench(tslMap_u64)                       960.95%    68.73ns   14.55M
mapLookupBench(tslMap_record)                   1638.63%    40.31ns   24.81M
============================================================================
Memory footprint for 800,000 entries:
Map            Page Bytes       Accurate Bytes
----------------------------------------------------------------------------
btreeMap_u32:   8,560,640       6,640,000
f14Map_u32  :  12,800,000       8,388,640
stdMap_u32  :  24,285,184
tslMap_u32  :   5,574,656
----------------------------------------------------------------------------
btreeMap_u64:   10,186,752       10,506,624
f14Map_u64  :   21,139,456       16,777,248
stdMap_u64  :   30,203,904
tslMap_u64  :   11,034,624
tslMap_record:   6,217,728
====================================END=====================================
#endif

int main(int /* argc */, char** /* argv */) {
  folly::runBenchmarks();

  // memory footprint
  auto accBytesBTree32 = btreeMap_u32.getMemoryStats().totalMemory();
  auto accBytesF14_32 =
      f14Map_u32.getAllocatedMemorySize() + sizeof(f14Map_u32);

  auto accBytesBTree64 = btreeMap_u64.getMemoryStats().totalMemory();
  auto accBytesF14_64 =
      f14Map_u64.getAllocatedMemorySize() + sizeof(f14Map_u64);

  auto btInfo32 = folly::sformat("btreeMap_u32:   {:,}       {:,}",
                                 pageBytesBTree32, accBytesBTree32);
  auto f14Info32 = folly::sformat("f14Map_u32  :  {:,}       {:,}",
                                  pageBytesF14_32, accBytesF14_32);
  auto stdInfo32 = folly::sformat("stdMap_u32  :  {:,}", pageBytesStd32);
  auto tslInfo32 = folly::sformat("tslMap_u32  :   {:,}", pageBytesTsl32);

  auto btInfo64 = folly::sformat("btreeMap_u64:   {:,}       {:,}",
                                 pageBytesBTree64, accBytesBTree64);
  auto f14Info64 = folly::sformat("f14Map_u64  :   {:,}       {:,}",
                                  pageBytesF14_64, accBytesF14_64);
  auto stdInfo64 = folly::sformat("stdMap_u64  :   {:,}", pageBytesStd64);
  auto tslInfo64 = folly::sformat("tslMap_u64  :   {:,}", pageBytesTsl64);
  auto tslInfoRec = folly::sformat("tslMap_record:   {:,}", pageBytesTslRec);

  std::cout << folly::sformat("Memory footprint for {:,} entries:", kNumKeys)
            << std::endl;
  std::cout << "Map            Page Bytes       Accurate Bytes" << std::endl;
  std::cout << folly::sformat("{:-^*}", 76, "---") << std::endl;

  std::cout << btInfo32 << std::endl;
  std::cout << f14Info32 << std::endl;
  std::cout << stdInfo32 << std::endl;
  std::cout << tslInfo32 << std::endl;

  std::cout << folly::sformat("{:-^*}", 76, "---") << std::endl;
  std::cout << btInfo64 << std::endl;
  std::cout << f14Info64 << std::endl;
  std::cout << stdInfo64 << std::endl;
  std::cout << tslInfo64 << std::endl;
  std::cout << tslInfoRec << std::endl;

  std::cout << folly::sformat("{:=^*}", 76, "END") << std::endl;
  return 0;
}
