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
#include "cachelib/datatype/Map.h"

DEFINE_int32(num_keys, 100, "number of keys used to populate the maps");
DEFINE_int32(num_ops, 100 * 1000, "number of operations");
DEFINE_double(write_rate, 0.05, "rate of writes");

namespace facebook {
namespace cachelib {
struct Value {
  static constexpr size_t kValueSize = 8;
  std::array<uint8_t, kValueSize> _;
};

using CachelibMap = Map<uint32_t, Value, LruAllocator>;
using StdUnorderedMap = datatypebench::StdUnorderedMap;

constexpr folly::StringPiece kClMap = "cachelib_map";
constexpr folly::StringPiece kStdUnorderedMap = "std_unordered_map";
constexpr folly::StringPiece kFrozenStdUnorderedMap = "frozen_unordered_map";
const std::string kFollyCacheStdUnorderedMap = "folly_cache_std_unordered_map";

std::unique_ptr<LruAllocator> cache;
PoolId poolId;

using FollyCache = folly::EvictingCacheMap<std::string, StdUnorderedMap>;
std::unique_ptr<FollyCache> follyCache;

void insertFrozenMap(StdUnorderedMap& m, folly::StringPiece name) {
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

  // insert CachelibMap into cache
  {
    auto m = CachelibMap::create(*cache, poolId, kClMap);
    cache->insert(m.viewWriteHandle());
  }

  // insert StdUnorderedMap
  {
    StdUnorderedMap m;
    auto iobuf = Serializer::serializeToIOBuf(m);
    auto res =
        util::insertIOBufInCache(*cache, poolId, kStdUnorderedMap, *iobuf);
    XDCHECK(res);
  }

  // insert frozen StdUnorderedMap into cache
  {
    StdUnorderedMap m;
    insertFrozenMap(m, kFrozenStdUnorderedMap);
  }

  // set up folly::EvictingCacheMap
  follyCache = std::make_unique<FollyCache>(1000 /* max number of items */);
  {
    StdUnorderedMap m;
    follyCache->set(kFollyCacheStdUnorderedMap, std::move(m));
  }
}

void benchCachelibMap() {
  auto getCachelibMap = [] {
    auto it = cache->findImpl(kClMap, AccessMode::kRead);
    XDCHECK(it);
    return CachelibMap::fromWriteHandle(*cache, std::move(it));
  };
  std::mt19937 gen{1};
  std::discrete_distribution<> rwDist({1 - FLAGS_write_rate, FLAGS_write_rate});

  Value val;
  for (int i = 0; i < FLAGS_num_ops; ++i) {
    auto m = getCachelibMap();
    int key = i % FLAGS_num_keys;

    if (rwDist(gen) == 0) {
      m.find(key);
    } else {
      m.insertOrReplace(key, val);
    }
  }
}

void benchStdMap() {
  auto getMap = [] {
    auto it = cache->find(kStdUnorderedMap);
    XDCHECK(it);
    Deserializer deserializer{
        it->template getMemoryAs<uint8_t>(),
        it->template getMemoryAs<uint8_t>() + it->getSize()};
    return deserializer.deserialize<StdUnorderedMap>();
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
      auto res =
          util::insertIOBufInCache(*cache, poolId, kStdUnorderedMap, *iobuf);
      XDCHECK(res);
    }
  }
}

void benchFrozenMap() {
  auto getMap = [&] {
    auto it = cache->find(kFrozenStdUnorderedMap);
    XDCHECK(it);
    folly::StringPiece range{
        it->template getMemoryAs<const char>(),
        it->template getMemoryAs<const char>() + it->getSize()};
    return apache::thrift::frozen::mapFrozen<StdUnorderedMap>(range);
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
      insertFrozenMap(mutableS, kFrozenStdUnorderedMap);
    }
  }
}

void benchFollyCacheStdMap() {
  std::mt19937 gen{1};
  std::discrete_distribution<> rwDist({1 - FLAGS_write_rate, FLAGS_write_rate});

  std::string val{Value::kValueSize, 'c'};
  for (int i = 0; i < FLAGS_num_ops; ++i) {
    auto& s = follyCache->get(kFollyCacheStdUnorderedMap);

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

BENCHMARK(cachelib_map) { cl::benchCachelibMap(); }
BENCHMARK_RELATIVE(std_unordered_map_on_cachelib) { cl::benchStdMap(); }
BENCHMARK_RELATIVE(frozen_unordered_map_on_cachelib) { cl::benchFrozenMap(); }
BENCHMARK_RELATIVE(std_unordered_mao_on_folly_evcting_cache_map) {
  cl::benchFollyCacheStdMap();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  cl::setup();
  folly::runBenchmarks();
}
