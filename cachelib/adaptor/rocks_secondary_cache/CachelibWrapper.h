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
#include "rocksdb/secondary_cache.h"
#include "rocksdb/types.h"
#include "rocksdb/version.h"

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;
} // namespace ROCKSDB_NAMESPACE

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
// ROCKSDB_NAMESPACE::SecondaryCache. It can be allocated using
// NewRocksCachelibWrapper() and the resulting pointer
// can be passed in ROCKSDB_NAMESPACE::LRUCacheOptions to
// ROCKSDB_NAMESPACE::NewLRUCache().
//
// Users can also cast a pointer to it and call methods on
// it directly, especially custom methods that may be added
// in the future.  For example -
// std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCache> cache =
//      NewRocksCachelibWrapper(opts);
// static_cast<RocksCachelibWrapper*>(cache.get())->Erase(key);
class RocksCachelibWrapper : public ROCKSDB_NAMESPACE::SecondaryCache {
 public:
  RocksCachelibWrapper(const RocksCachelibOptions& options) :
    options_(options) { }
  ~RocksCachelibWrapper() override;

  static const char* kClassName() const { return "RocksCachelibWrapper"; }
  const char* Name() const override { return kClassName(); }

  ROCKSDB_NAMESPACE::Status Insert(
      const ROCKSDB_NAMESPACE::Slice& key,
      void* value,
      const ROCKSDB_NAMESPACE::Cache::CacheItemHelper* helper) override;

  std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCacheResultHandle> Lookup(
      const ROCKSDB_NAMESPACE::Slice& key,
      const ROCKSDB_NAMESPACE::Cache::CreateCallback& create_cb,
      bool wait
#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
      ,
      bool /*advise_erase*/
#endif
      ,
      bool& is_in_sec_cache) override;

#if ROCKSDB_MAJOR > 7 || (ROCKSDB_MAJOR == 7 && ROCKSDB_MINOR >= 7)
  bool SupportForceErase() const override { return false; }
#endif

  void Erase(const ROCKSDB_NAMESPACE::Slice& key) override;

  void WaitAll(
      std::vector<ROCKSDB_NAMESPACE::SecondaryCacheResultHandle*> handles) override;

  // TODO
  std::string GetPrintableOptions() const override { return ""; }

  // Calling Close() persists the cachelib state to the file and frees the
  // cachelib object. After calling Close(), subsequent lookups will fail,
  // and subsequent inserts will be silently ignored. Close() is not thread
  // safe, i.e only one thread can call it at a time. It doesn't require
  // ongoing lookups and inserts by other threads to be quiesced.
  void Close();

  ROCKSDB_NAMESPACE::Status PrepareOptions(const ConfigOptions& /*options*/) override;
  
 private:
  RocksCachelibOptions options_;
  std::atomic<FbCache*> cache_;
  cachelib::PoolId pool_;
};

// Allocate a new Cache instance with a ROCKSDB_NAMESPACE::TieredCache wrapper around it
extern std::unique_ptr<ROCKSDB_NAMESPACE::SecondaryCache> NewRocksCachelibWrapper(
    const RocksCachelibOptions& opts);
#ifndef ROCKSDB_LITE
extern "C" {
int register_CachelibObjects(ROCKSDB_NAMESPACE::ObjectLibrary& library, const std::string&);
} // extern "C"

#endif // ROCKSDB_LITE
} // namespace rocks_secondary_cache
} // namespace facebook
