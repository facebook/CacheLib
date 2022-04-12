/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/ScopeGuard.h>

#include <any>
#include <atomic>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/Time.h"
#include "cachelib/experimental/objcache2/ObjectCacheBase.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

struct ObjectCacheConfig {
  // Above this many entries, L1 will start evicting
  size_t l1EntriesLimit{};

  // This controls how many buckets are present in L1's hashtable
  uint32_t l1HashTablePower{10};

  // This controls how many locks are present in L1's hashtable
  uint32_t l1LockPower{10};

  // Number of shards to improve insert/remove concurrency
  size_t l1NumShards{1};

  // default alloc size for l1 cache, only single alloc size is supported now
  // this is an internal per-item overhead, used to approximately control cache
  // limit temporarily before size awareness is available.
  uint32_t l1AllocSize{1024};

  // the l1 cache size, if it is default, we will calculate base on
  // l1EntriesLimit
  size_t l1CacheSize{0};

  // cache name
  std::string cacheName;

  // disable place holder, which is used to control the total number of entries
  bool placeHolderDisabled{false};
};

class ObjectCache : public ObjectCacheBase {
 private:
  // make constructor private, but constructable by std::make_unique
  struct InternalConstructor {};

  template <typename T>
  void init(ObjectCacheConfig config);

 public:
  explicit ObjectCache(InternalConstructor, const ObjectCacheConfig& config)
      : l1NumShards_{config.l1NumShards},
        l1EntriesLimit_(config.l1EntriesLimit) {}

  template <typename T>
  static std::unique_ptr<ObjectCache> create(ObjectCacheConfig config);

  ~ObjectCache();

  template <typename T>
  std::shared_ptr<const T> find(folly::StringPiece key);

  template <typename T>
  std::shared_ptr<T> findToWrite(folly::StringPiece key);

  // Insert the object into the cache with given key, if the key exists in the
  // cache, it will be replaced with new obejct.
  //
  // @param key          the key.
  // @param object       unique pointer for the object to be inserted.
  // @param ttlSecs      object expiring seconds.
  // @param replacedPtr  a pointer to a shared_ptr, if it is not nullptr it will
  // be assigned to the replaced object.
  //
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @return a pair of allocation status and shared_ptr of newly inserted
  // object.
  template <typename T>
  std::pair<bool, std::shared_ptr<T>> insertOrReplace(
      folly::StringPiece key,
      std::unique_ptr<T> object,
      uint32_t ttlSecs = 0,
      std::shared_ptr<T>* replacedPtr = nullptr);

  void remove(folly::StringPiece key);

  void getObjectCacheCounters(
      std::function<void(folly::StringPiece, uint64_t)> visitor) const override;

  uint64_t getNumEntries() const {
    return l1Cache_->getGlobalCacheStats().numItems;
  }

  LruAllocator& getL1Cache() { return *l1Cache_; }

 protected:
  // serialize cache allocator config for exporting to Scuba
  std::map<std::string, std::string> serializeConfigParams() const override;

 private:
  template <typename T>
  LruAllocator::WriteHandle allocateFromL1(folly::StringPiece key,
                                           uint32_t ttl,
                                           uint32_t creationTime);

  // Number of shards (LRUs) to lessen the contention on L1 cache
  const size_t l1NumShards_{};

  // Above this many entries, L1 will start evicting
  const size_t l1EntriesLimit_{};

  // They take up space so we can control exact number of items in cache
  std::vector<LruAllocator::WriteHandle> placeholders_;

  TLCounter evictions_{};
  TLCounter lookups_;
  TLCounter succL1Lookups_;
  TLCounter inserts_;
  TLCounter insertErrors_;
  TLCounter replaces_;
  TLCounter removes_;
};
} // namespace objcache2
} // namespace cachelib
} // namespace facebook
#include "cachelib/experimental/objcache2/ObjectCache-inl.h"
