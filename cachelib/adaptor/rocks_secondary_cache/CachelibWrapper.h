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

#pragma once
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/facebook/admin/CacheAdmin.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/types.h"
#include "rocksdb/version.h"

namespace facebook {
namespace rocks_secondary_cache {
// Options structure for configuring a Cachelib SecondaryCache instance
struct RocksCachelibOptions {
  // A name for the use case
  std::string cacheName;

  // Path to the cache file
  std::string fileName;

  // Maximum size of the cache file
  size_t size;

  // Minimum IO granularity. Typically the device block size
  size_t blockSize = 4096;

  // Size of a cache region. A region is the granularity for garbage
  // collection
  size_t regionSize = 16 * 1024 * 1024;

  // Admission control policy - random or dynamic_random. The latter allows
  // writes to be rate limited in order to prolong flash life, and the
  // rejection rate is dynamically adjusted to ratelimit writes.
  std::string admPolicy = "random";

  // For random admission policy, probability of admission
  double admProbability = 1.0;

  // Maximum write rate for dynamic_random policy in bytes/s
  uint64_t maxWriteRate = 128 << 20;

  // Target daily write rate for dynamic_random policy in bytes/s. This would
  // typically be <= maxWriteRate.
  uint64_t admissionWriteRate = 128 << 20;

  // Size of the volatile portion of the cache. Typically a few 10s to a
  // couple of 100s of MBs
  size_t volatileSize = 256 * 1024 * 1024;

  // Base 2 exponent for number of hash table buckets
  uint32_t bktPower = 12;

  // Base 2 exponent for number of locks
  uint32_t lockPower = 12;

  // If true, enable Cachelib FB303 stats
  bool fb303Stats = false;

  // An oncall name for FB303 stats
  std::string oncallName;
};

using FbCache =
    cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
using FbCacheConfig = typename FbCache::Config;
using NvmCacheConfig = typename FbCache::NvmCacheConfig;
using FbCacheKey = typename FbCache::Key;
using FbCacheReadHandle = typename FbCache::ReadHandle;
using FbCacheItem = typename FbCache::Item;

// The RocksCachelibWrapper is a concrete implementation of
// rocksdb::SecondaryCache. It can be allocated using
// NewRocksCachelibWrapper() and the resulting pointer
// can be passed in rocksdb::LRUCacheOptions to
// rocksdb::NewLRUCache().
//
// Users can also cast a pointer to it and call methods on
// it directly, especially custom methods that may be added
// in the future.  For example -
// std::unique_ptr<rocksdb::SecondaryCache> cache =
//      NewRocksCachelibWrapper(opts);
// static_cast<RocksCachelibWrapper*>(cache.get())->Erase(key);
class RocksCachelibWrapper : public rocksdb::SecondaryCache {
 public:
  RocksCachelibWrapper(std::unique_ptr<FbCache>&& cache,
                       std::unique_ptr<cachelib::CacheAdmin>&& admin,
                       cachelib::PoolId pool)
      : cache_(std::move(cache).release()),
        admin_(std::move(admin)),
        pool_(pool) {}
  ~RocksCachelibWrapper() override;

  const char* Name() const override { return "RocksCachelibWrapper"; }

  rocksdb::Status Insert(
      const rocksdb::Slice& key,
      void* value,
      const rocksdb::Cache::CacheItemHelper* helper) override;

  std::unique_ptr<rocksdb::SecondaryCacheResultHandle> Lookup(
      const rocksdb::Slice& key,
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 10)
      const rocksdb::Cache::CacheItemHelper* helper,
      rocksdb::Cache::CreateContext* create_context,
#else
      const rocksdb::Cache::CreateCallback& create_cb,
#endif
      bool wait,
      bool advise_erase,
      bool& is_in_sec_cache) override;

  bool SupportForceErase() const override { return false; }

  void Erase(const rocksdb::Slice& key) override;

  void WaitAll(
      std::vector<rocksdb::SecondaryCacheResultHandle*> handles) override;

  // TODO
  std::string GetPrintableOptions() const override { return ""; }

  // Calling Close() persists the cachelib state to the file and frees the
  // cachelib object. After calling Close(), subsequent lookups will fail,
  // and subsequent inserts will be silently ignored. Close() is not thread
  // safe, i.e only one thread can call it at a time. It doesn't require
  // ongoing lookups and inserts by other threads to be quiesced.
  void Close();

  // If the admPolicy in RocksCachelibOptions was set to "dynamic_random",
  // then this function can be called to update the max write rate for that
  // policy.
  bool UpdateMaxWriteRateForDynamicRandom(uint64_t maxRate);

 private:
  std::atomic<FbCache*> cache_;
  std::unique_ptr<cachelib::CacheAdmin> admin_;
  cachelib::PoolId pool_;
};

// Allocate a new Cache instance with a rocksdb::TieredCache wrapper around it
extern std::unique_ptr<rocksdb::SecondaryCache> NewRocksCachelibWrapper(
    const RocksCachelibOptions& opts);
} // namespace rocks_secondary_cache
} // namespace facebook
