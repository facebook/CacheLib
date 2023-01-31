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

// Benchmark for measuring individual operations of cachelib::Map
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <array>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/datatype/Map.h"

DEFINE_int32(num_keys, 50 * 1000, "number of keys used to populate the maps");
DEFINE_int32(num_read_ops, 1000 * 1000, "number of read operations");

namespace facebook {
namespace cachelib {
using Value = std::array<uint8_t, 32>;
using CachelibMap = Map<int32_t, Value, LruAllocator>;
using StdUnorderedMap = std::unordered_map<int32_t, Value>;
using CachelibHashTable = detail::HashTable<int32_t>;
using CachelibBM = detail::BufferManager<LruAllocator>;

std::unique_ptr<LruAllocator> cache;
PoolId poolId;

std::unique_ptr<StdUnorderedMap> lookupStdMap;

class HashTableDeleter {
 public:
  void operator()(CachelibHashTable* ht) {
    delete[] reinterpret_cast<uint8_t*>(ht);
  }
};
std::unique_ptr<CachelibHashTable, HashTableDeleter> clHashtable;

std::vector<detail::BufferAddr> bufferAddrs;

void setupClMap() {
  // Prepopulate a map
  auto m = CachelibMap::create(*cache, poolId, "lookup_benchmark_map");
  XDCHECK(!m.isNullWriteHandle());
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    Value v{};
    const auto res = m.insert(i, v);
    XDCHECK(res);
  }
  cache->insert(m.viewWriteHandle());
}

void setupUnorderedStdMap() {
  lookupStdMap = std::make_unique<StdUnorderedMap>();
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    (*lookupStdMap)[i] = Value{};
  }
}

void setupClHashtable() {
  // Gives 50% more room to the hash table
  const uint32_t htSize = CachelibHashTable::computeStorageSize(
      static_cast<size_t>(FLAGS_num_keys * 1.5));
  uint8_t* buffer = new uint8_t[htSize];
  clHashtable.reset(new (buffer) CachelibHashTable(
      static_cast<size_t>(FLAGS_num_keys * 1.5)));
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    clHashtable->insertOrReplace(i, {0, 0});
  }
}

void setupClBufferManager() {
  auto parent =
      util::allocateAccessible(*cache, poolId, "lookup_buffer_manager", 0);
  CachelibBM bm{*cache, parent, 1000};
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    auto addr = bm.allocate(sizeof(Value));
    if (!addr) {
      bm.expand(sizeof(Value));
      addr = bm.allocate(sizeof(Value));
    }
    bufferAddrs.push_back(addr);
  }
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

  setupUnorderedStdMap();
  setupClMap();
  setupClHashtable();
  setupClBufferManager();
}

void insertionStdUnorderedMap() {
  StdUnorderedMap m;
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    m[i] = Value{};
  }
}

void insertionCachelibBufferManagerOnly() {
  auto parent = cache->allocate(poolId, "this_is_my_buffer_mgr", 0);
  CachelibBM bm{*cache, parent, 1000};
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    auto addr = bm.allocate(sizeof(Value));
    if (!addr) {
      bm.expand(sizeof(Value));
      addr = bm.allocate(sizeof(Value));
    }
    XDCHECK(addr);
  }
}

void insertionCachelibMap() {
  auto m = CachelibMap::create(*cache, poolId, "this_is_my_map");
  XDCHECK(!m.isNullWriteHandle());
  for (int32_t i = 0; i < FLAGS_num_keys; ++i) {
    const auto res = m.insert(i, Value{});
    XDCHECK(res);
  }
}

void lookupStdUnorderedMap() {
  StdUnorderedMap& m = *lookupStdMap;
  for (int32_t i = 0; i < FLAGS_num_read_ops; ++i) {
    const int32_t key = i % FLAGS_num_keys;
    const auto itr = m.find(key);
    XDCHECK(itr != m.end());
  }
}

void lookupCachelibBufferManagerOnly() {
  auto parent = cache->findImpl("lookup_buffer_manager", AccessMode::kRead);
  XDCHECK(parent != nullptr);

  CachelibBM bm{*cache, parent};
  for (int32_t i = 0; i < FLAGS_num_read_ops; ++i) {
    const int32_t key = i % FLAGS_num_keys;
    auto* v = bm.template get<void*>(bufferAddrs[key]);
    XDCHECK(v != nullptr);
  }
}

void lookupCachelibHashtableOnly() {
  auto& ht = *clHashtable;
  for (int32_t i = 0; i < FLAGS_num_read_ops; ++i) {
    const int32_t key = i % FLAGS_num_keys;
    auto* v = ht.find(key);
    XDCHECK(v != nullptr);
  }
}

void lookupCachelibMap() {
  auto handle = cache->findImpl("lookup_benchmark_map", AccessMode::kRead);
  XDCHECK(handle != nullptr);

  CachelibMap m = CachelibMap::fromWriteHandle(*cache, std::move(handle));
  for (int32_t i = 0; i < FLAGS_num_read_ops; ++i) {
    const int32_t key = i % FLAGS_num_keys;
    auto* v = m.find(key);
    XDCHECK(v != nullptr);
  }
}
} // namespace cachelib
} // namespace facebook

namespace cl = facebook::cachelib;

// Insertion Benchmarks
BENCHMARK(insertion_std_unordered_map) { cl::insertionStdUnorderedMap(); }
BENCHMARK_RELATIVE(insertion_cachelib_buffer_manager_only) {
  cl::insertionCachelibBufferManagerOnly();
}
BENCHMARK_RELATIVE(insertion_cachelib_map) { cl::insertionCachelibMap(); }

// Lookup Benchmarks
BENCHMARK(lookup_std_unordered_map) { cl::lookupStdUnorderedMap(); }
BENCHMARK_RELATIVE(lookup_cachelib_buffer_manager_only) {
  cl::lookupCachelibBufferManagerOnly();
}
BENCHMARK_RELATIVE(lookup_cachelib_hashtable_only) {
  cl::lookupCachelibHashtableOnly();
}
BENCHMARK_RELATIVE(lookup_cachelib_map) { cl::lookupCachelibMap(); }

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  cl::setup();
  folly::runBenchmarks();
}
