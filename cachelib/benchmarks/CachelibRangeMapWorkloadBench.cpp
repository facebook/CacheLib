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

// Benchmark for measuring RMW workload with cachelib::Map
//
// Benchmark Results on a Devvm
// ============================================================================
// cachelib/benchmarks/CachelibRangeMapWorkloadBench.cpprelative  time/iter
// iters/s
// ============================================================================
// cachelib_range_map                                          49.55ms    20.18
// std_map_on_cachelib                               10.40%   476.42ms     2.10
// frozen_map_on_cachelib                            15.93%   311.07ms     3.21
// std_map_on_folly_evcting_cache_map               405.06%    12.23ms    81.74
// ============================================================================

#include <folly/Benchmark.h>
#include <folly/container/EvictingCacheMap.h>
#include <folly/init/Init.h>

#include <random>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <thrift/lib/cpp2/frozen/FrozenUtil.h>

#include "cachelib/benchmarks/gen-cpp2/DataTypeBench_layouts.h"
#include "cachelib/benchmarks/gen-cpp2/DataTypeBench_types.h"
#pragma GCC diagnostic pop

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/datatype/RangeMap.h"

DEFINE_int32(num_keys, 100, "number of keys used to populate the maps");
DEFINE_int32(num_ops, 100 * 1000, "number of operations");
DEFINE_double(write_rate, 0.05, "rate of writes");

namespace facebook {
namespace cachelib {
struct Value {
  static constexpr size_t kValueSize = 8;
  std::array<uint8_t, kValueSize> _;
};

using CachelibRangeMap = RangeMap<uint32_t, Value, LruAllocator>;
using StdMap = datatypebench::StdMap;

constexpr folly::StringPiece kClMap = "cachelib_map";
constexpr folly::StringPiece kStdMap = "std_unordered_map";
constexpr folly::StringPiece kFrozenStdMap = "frozen_unordered_map";
const std::string kFollyCacheStdMap = "folly_cache_std_unordered_map";

std::unique_ptr<LruAllocator> cache;
PoolId poolId;

using FollyCache = folly::EvictingCacheMap<std::string, StdMap>;
std::unique_ptr<FollyCache> follyCache;

void insertFrozenMap(StdMap& m, folly::StringPiece name) {
  std::string frozenContent;
  apache::thrift::frozen::freezeToString(m, frozenContent);
  auto item = cache->allocate(poolId, name, frozenContent.size());
  XDCHECK(item);
  std::memcpy(item->getMemory(), frozenContent.data(), frozenContent.size());
  cache->insertOrReplace(item);
}

void setup() {
  LruAllocator::Config config;
  config.setCacheSize(200 * 1024 * 1024); // 200 MB
  config.enableCompactCache();

  // 16 million buckets, 1 million locks
  LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                          20 /* locks power */};
  config.setAccessConfig(accessConfig);
  config.configureChainedItems(accessConfig);

  cache = std::make_unique<LruAllocator>(config);

  poolId = cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);

  // insert CachelibRangeMap into cache
  {
    auto m = CachelibRangeMap::create(*cache, poolId, kClMap);
    cache->insert(m.viewWriteHandle());
  }

  // insert StdMap
  {
    StdMap m;
    auto iobuf = Serializer::serializeToIOBuf(m);
    auto res = util::insertIOBufInCache(*cache, poolId, kStdMap, *iobuf);
    XDCHECK(res);
  }

  // insert frozen StdMap into cache
  {
    StdMap m;
    insertFrozenMap(m, kFrozenStdMap);
  }

  // set up folly::EvictingCacheMap
  follyCache = std::make_unique<FollyCache>(1000 /* max number of items */);
  {
    StdMap m;
    follyCache->set(kFollyCacheStdMap, std::move(m));
  }
}

void benchCachelibRangeMap() {
  auto getCachelibRangeMap = [] {
    auto it = cache->findImpl(kClMap, AccessMode::kRead);
    XDCHECK(it);
    return CachelibRangeMap::fromWriteHandle(*cache, std::move(it));
  };
  std::mt19937 gen{1};
  std::discrete_distribution<> rwDist({1 - FLAGS_write_rate, FLAGS_write_rate});

  Value val;
  for (int i = 0; i < FLAGS_num_ops; ++i) {
    auto m = getCachelibRangeMap();
    int key = i % FLAGS_num_keys;

    if (rwDist(gen) == 0) {
      m.lookup(key);
    } else {
      m.insertOrReplace(key, val);
    }
  }
}

void benchStdMap() {
  auto getMap = [] {
    auto it = cache->find(kStdMap);
    XDCHECK(it);
    Deserializer deserializer{
        it->template getMemoryAs<uint8_t>(),
        it->template getMemoryAs<uint8_t>() + it->getSize()};
    return deserializer.deserialize<StdMap>();
  };
  std::mt19937 gen{1};
  std::discrete_distribution<> rwDist({1 - FLAGS_write_rate, FLAGS_write_rate});

  std::string val{Value::kValueSize, 'c'};
  for (int i = 0; i < FLAGS_num_ops; ++i) {
    auto s = getMap();

    int key = i % FLAGS_num_keys;

    if (rwDist(gen) == 0) {
      s.m()->find(key);
    } else {
      s.m()[key] = val;
      auto iobuf = Serializer::serializeToIOBuf(s);
      auto res = util::insertIOBufInCache(*cache, poolId, kStdMap, *iobuf);
      XDCHECK(res);
    }
  }
}

void benchFrozenMap() {
  auto getMap = [&] {
    auto it = cache->find(kFrozenStdMap);
    XDCHECK(it);
    folly::StringPiece range{
        it->template getMemoryAs<const char>(),
        it->template getMemoryAs<const char>() + it->getSize()};
    return apache::thrift::frozen::mapFrozen<StdMap>(range);
  };
  std::mt19937 gen{1};
  std::discrete_distribution<> rwDist({1 - FLAGS_write_rate, FLAGS_write_rate});

  std::string val{Value::kValueSize, 'c'};
  for (int i = 0; i < FLAGS_num_ops; ++i) {
    auto s = getMap();

    int key = i % FLAGS_num_keys;

    if (rwDist(gen) == 0) {
      s.m().find(key);
    } else {
      auto mutableS = s.thaw();
      mutableS.m()[key] = val;
      insertFrozenMap(mutableS, kFrozenStdMap);
    }
  }
}

void benchFollyCacheStdMap() {
  std::mt19937 gen{1};
  std::discrete_distribution<> rwDist({1 - FLAGS_write_rate, FLAGS_write_rate});

  std::string val{Value::kValueSize, 'c'};
  for (int i = 0; i < FLAGS_num_ops; ++i) {
    auto& s = follyCache->get(kFollyCacheStdMap);

    int key = i % FLAGS_num_keys;

    if (rwDist(gen) == 0) {
      s.m()->find(key);
    } else {
      s.m()[key] = val;
    }
  }
}
} // namespace cachelib
} // namespace facebook

namespace cl = facebook::cachelib;

BENCHMARK(cachelib_range_map) { cl::benchCachelibRangeMap(); }
BENCHMARK_RELATIVE(std_map_on_cachelib) { cl::benchStdMap(); }
BENCHMARK_RELATIVE(frozen_map_on_cachelib) { cl::benchFrozenMap(); }
BENCHMARK_RELATIVE(std_map_on_folly_evcting_cache_map) {
  cl::benchFollyCacheStdMap();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  cl::setup();
  folly::runBenchmarks();
}
