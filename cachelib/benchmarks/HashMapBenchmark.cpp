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

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/Portability.h>
#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <folly/init/Init.h>
#include <tsl/sparse_map.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <unordered_map>

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

size_t pageBytesStd32 = 0;
size_t pageBytesF14_32 = 0;
size_t pageBytesTsl32 = 0;

size_t pageBytesStd64 = 0;
size_t pageBytesF14_64 = 0;
size_t pageBytesTsl64 = 0;
size_t pageBytesTslRec = 0;

auto stdMap_u32 =
    makeMap<std::unordered_map<uint32_t, uint32_t>>(pageBytesStd32);
auto f14Map_u32 =
    makeMap<folly::F14ValueMap<uint32_t, uint32_t>>(pageBytesF14_32);
auto tslMap_u32 = makeMap<SparseMap<uint32_t, uint32_t>>(pageBytesTsl32);

auto stdMap_u64 =
    makeMap<std::unordered_map<uint32_t, uint64_t>>(pageBytesStd64);
auto f14Map_u64 =
    makeMap<folly::F14ValueMap<uint32_t, uint64_t>>(pageBytesF14_64);
auto tslMap_u64 = makeMap<SparseMap<uint32_t, uint64_t>>(pageBytesTsl64);
auto tslMap_record = makeMap<SparseMap<uint32_t, Record>>(pageBytesTslRec);

BENCHMARK_PARAM(mapLookupBench, stdMap_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, f14Map_u32)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, tslMap_u32)

BENCHMARK_DRAW_LINE();

BENCHMARK_PARAM(mapLookupBench, stdMap_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, f14Map_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, tslMap_u64)
BENCHMARK_RELATIVE_PARAM(mapLookupBench, tslMap_record)

#if 0
============================================================================
cachelib/benchmarks/HashMapBenchmark.cpp        relative  time/iter  iters/s
============================================================================
mapLookupBench(stdMap_u32)                                  81.32ns   12.30M
mapLookupBench(f14Map_u32)                       258.04%    31.51ns   31.73M
mapLookupBench(tslMap_u32)                       192.79%    42.18ns   23.71M
----------------------------------------------------------------------------
mapLookupBench(stdMap_u64)                                 127.21ns    7.86M
mapLookupBench(f14Map_u64)                       248.96%    51.10ns   19.57M
mapLookupBench(tslMap_u64)                       242.28%    52.51ns   19.05M
============================================================================
Memory footprint for 800,000 entries:
Map            Page Bytes       Accurate Bytes
----------------------------------------------------------------------------
f14Map_u32  :  14,966,784       8,388,640
stdMap_u32  :  22,888,448
tslMap_u32  :   8,003,584
----------------------------------------------------------------------------
f14Map_u64  :   21,270,528       16,777,248
stdMap_u64  :   29,724,672
tslMap_u64  :   13,242,368
====================================END=====================================
#endif

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();

  // memory footprint
  auto accBytesF14_32 =
      f14Map_u32.getAllocatedMemorySize() + sizeof(f14Map_u32);

  auto accBytesF14_64 =
      f14Map_u64.getAllocatedMemorySize() + sizeof(f14Map_u64);

  auto f14Info32 = folly::sformat("f14Map_u32  :  {:,}       {:,}",
                                  pageBytesF14_32, accBytesF14_32);
  auto stdInfo32 = folly::sformat("stdMap_u32  :  {:,}", pageBytesStd32);
  auto tslInfo32 = folly::sformat("tslMap_u32  :   {:,}", pageBytesTsl32);

  auto f14Info64 = folly::sformat("f14Map_u64  :   {:,}       {:,}",
                                  pageBytesF14_64, accBytesF14_64);
  auto stdInfo64 = folly::sformat("stdMap_u64  :   {:,}", pageBytesStd64);
  auto tslInfo64 = folly::sformat("tslMap_u64  :   {:,}", pageBytesTsl64);
  auto tslInfoRec = folly::sformat("tslMap_record:   {:,}", pageBytesTslRec);

  std::cout << folly::sformat("Memory footprint for {:,} entries:", kNumKeys)
            << std::endl;
  std::cout << "Map            Page Bytes       Accurate Bytes" << std::endl;
  std::cout << folly::sformat("{:-^*}", 76, "---") << std::endl;

  std::cout << f14Info32 << std::endl;
  std::cout << stdInfo32 << std::endl;
  std::cout << tslInfo32 << std::endl;

  std::cout << folly::sformat("{:-^*}", 76, "---") << std::endl;
  std::cout << f14Info64 << std::endl;
  std::cout << stdInfo64 << std::endl;
  std::cout << tslInfo64 << std::endl;
  std::cout << tslInfoRec << std::endl;

  std::cout << folly::sformat("{:=^*}", 76, "END") << std::endl;
  return 0;
}
