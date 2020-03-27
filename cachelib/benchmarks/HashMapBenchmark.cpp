#include <iostream>
#include <unordered_map>

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/container/F14Map.h>

#include "cachelib/navy/block_cache/BTree.h"

using namespace facebook::cachelib::navy;
using namespace facebook::cachelib::navy::details;

template <typename Key, typename Value>
using BTreeMap = BTree<Key, Value, BTreeTraits<30, 60, 90>>;

constexpr size_t kNumEntries = 1'000'000;
constexpr size_t kNumKeys = 800'000;

template <class K>
K makeKey() {
  return folly::to<K>(folly::Random::rand32(kNumEntries));
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

template <class Map>
Map makeMap() {
  using K = typename Map::key_type;
  Map m;
  for (size_t i = 0; i < kNumEntries; ++i) {
    ++m[makeKey<K>()];
  }
  return m;
}

template <class K, class V>
BTreeMap<K, V> makeBTreeMap() {
  BTreeMap<K, V> bt;
  for (size_t i = 0; i < kNumEntries; ++i) {
    auto k = makeKey<K>();
    V v = 0;
    bt.lookup(k, v);
    bt.insert(k, v + 1);
  }
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

    for (auto& key : keys) {
      if (iters-- == 0) {
        folly::doNotOptimizeAway(s);
        return;
      }
      auto found = map.find(key);
      if (found != map.end()) {
        ++s;
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

    for (auto& key : keys) {
      if (iters-- == 0) {
        folly::doNotOptimizeAway(s);
        return;
      }
      V value;
      if (bt.lookup(key, value)) {
        ++s;
      }
    }
  }
  folly::doNotOptimizeAway(s);
}

auto btreeMap_u32 = makeBTreeMap<uint32_t, uint32_t>();
auto stdMap_u32 = makeMap<std::unordered_map<uint32_t, uint32_t>>();
auto f14Map_u32 = makeMap<folly::F14ValueMap<uint32_t, uint32_t>>();

BENCHMARK_PARAM(btreeLookupBench, btreeMap_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, stdMap_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, f14Map_u32)

BENCHMARK_DRAW_LINE();

#if 0
============================================================================
cachelib/benchmarks/HashMapBenchmark.cpp        relative  time/iter  iters/s
============================================================================
btreeLookupBench(btreeMap_u32)                             346.08ns    2.89M
mapLookupBench(stdMap_u32)                       541.99%    63.85ns   15.66M
mapLookupBench(f14Map_u32)                      1633.63%    21.18ns   47.20M
----------------------------------------------------------------------------
============================================================================
Memory footprint for 1,000,000 entries
----------------------------------------------------------------------------
btreeMap_u32:  6,636,160 Bytes
f14Map_u32  :  8,388,640 Bytes
====================================END=====================================
#endif

int main(int /* argc */, char** /* argv */) {
  folly::runBenchmarks();

  // memory footprint
  auto bt = btreeMap_u32.getMemoryStats().totalMemory();
  auto f14 = f14Map_u32.getAllocatedMemorySize() + sizeof(f14Map_u32);
  std::cout << folly::sformat("Memory footprint for {:,} entries", kNumEntries)
            << std::endl;
  std::cout << folly::sformat("{:-^*}", 76, "---") << std::endl;
  std::cout << folly::sformat("btreeMap_u32:  {:,} Bytes", bt) << std::endl;
  std::cout << folly::sformat("f14Map_u32  :  {:,} Bytes", f14) << std::endl;
  std::cout << folly::sformat("{:=^*}", 76, "END") << std::endl;
  return 0;
}
