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

namespace test {
template <typename AllocatorT>
class ObjectCacheTest;
}

struct ObjectCacheConfig {
  // Above this many entries, L1 will start evicting
  size_t l1EntriesLimit{};

  // This controls how many buckets are present in L1's hashtable
  uint32_t l1HashTablePower{10};

  // This controls how many locks are present in L1's hashtable
  uint32_t l1LockPower{10};

  // Number of shards to improve insert/remove concurrency
  size_t l1NumShards{1};

  // The cache name
  std::string cacheName;

  // The maximum key size in bytes. Default to 255 bytes which is the maximum
  // key size cachelib supports.
  uint8_t maxKeySizeBytes{255};

  // If this is enabled, user has to pass the object size upon insertion
  bool objectSizeTrackingEnabled{false};
};

template <typename T>
struct FOLLY_PACK_ATTR ObjectCacheItem {
  T* objectPtr;
  size_t objectSize;
};

template <typename AllocatorT>
class ObjectCache : public ObjectCacheBase<AllocatorT> {
 private:
  // make constructor private, but constructable by std::make_unique
  struct InternalConstructor {};

  template <typename T>
  void init(ObjectCacheConfig config);

 public:
  enum class AllocStatus { kSuccess, kAllocError, kKeyAlreadyExists };

  explicit ObjectCache(InternalConstructor, const ObjectCacheConfig& config)
      : l1NumShards_{config.l1NumShards},
        l1EntriesLimit_(config.l1EntriesLimit),
        objectSizeTrackingEnabled_(config.objectSizeTrackingEnabled) {}

  template <typename T>
  static std::unique_ptr<ObjectCache<AllocatorT>> create(
      ObjectCacheConfig config);

  ~ObjectCache();

  // Look up an object in read-only access.
  // @param key   the key to the object.
  //
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @return shared pointer to a const version of the object
  template <typename T>
  std::shared_ptr<const T> find(folly::StringPiece key);

  // Look up an object in mutable access
  // @param key   the key to the object
  //
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @return shared pointer to a mutable version of the object
  template <typename T>
  std::shared_ptr<T> findToWrite(folly::StringPiece key);

  // Insert the object into the cache with given key. If the key exists in the
  // cache, it will be replaced with new obejct.
  //
  // @param key          the key to the object.
  // @param object       unique pointer for the object to be inserted.
  // @param objectSize   size of the object to be inserted.
  //                     if objectSizeTracking is enabled, a non-zero value must
  //                     be passed.
  // @param ttlSecs      object expiring seconds.
  // @param replacedPtr  a pointer to a shared_ptr, if it is not nullptr it will
  //                     be assigned to the replaced object.
  //
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @throw std::invalid_argument if objectSizeTracking is enabled but
  //        objectSize is 0.
  // @return a pair of allocation status and shared_ptr of newly inserted
  //         object.
  template <typename T>
  std::pair<AllocStatus, std::shared_ptr<T>> insertOrReplace(
      folly::StringPiece key,
      std::unique_ptr<T> object,
      size_t objectSize = 0,
      uint32_t ttlSecs = 0,
      std::shared_ptr<T>* replacedPtr = nullptr);

  // Insert the object into the cache with given key. If the key exists in the
  // cache, the new object won't be inserted.
  //
  // @param key          the key to the object.
  // @param object       unique pointer for the object to be inserted.
  // @param objectSize   size of the object to be inserted.
  //                     if objectSizeTracking is enabled, a non-zero value must
  //                     be passed.
  // @param ttlSecs      object expiring seconds.
  //
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @throw std::invalid_argument if objectSizeTracking is enabled but
  //        objectSize is 0.
  // @return a pair of allocation status and shared_ptr of newly inserted
  //         object. Note that even if object is not inserted, it will still
  //         be converted to a shared_ptr and returned.
  template <typename T>
  std::pair<AllocStatus, std::shared_ptr<T>> insert(folly::StringPiece key,
                                                    std::unique_ptr<T> object,
                                                    size_t objectSize = 0,
                                                    uint32_t ttlSecs = 0);

  // Remove an object from cache by its key. No-op if object doesn't exist.
  // @param key   the key to the object.
  void remove(folly::StringPiece key);

  // Get all the stats related to object-cache
  // @param visitor   callback that will be invoked with
  //                  {stat-name, value} for each stat
  void getObjectCacheCounters(
      std::function<void(folly::StringPiece, uint64_t)> visitor) const override;

  // Return the number of objects in cache
  uint64_t getNumEntries() const {
    return this->l1Cache_->getAccessContainerNumKeys();
  }

  // Get direct access to the interal CacheAllocator.
  // This is only used in tests.
  AllocatorT& getL1Cache() { return *this->l1Cache_; }

  // Get the default l1 allocation size in bytes.
  template <typename T>
  static uint32_t getL1AllocSize(uint8_t maxKeySizeBytes);

  // Get the total size of all cached objects in bytes.
  size_t getTotalObjectSize() const {
    return totalObjectSizeBytes_.load(std::memory_order_relaxed);
  }

 protected:
  // Serialize cache allocator config for exporting to Scuba
  std::map<std::string, std::string> serializeConfigParams() const override;

 private:
  // minimum alloc size in bytes for l1 cache.
  static constexpr uint32_t kL1AllocSizeMin = 64;

  // Allocate an item handle from the interal cache allocator. This item's
  // storage is used to cache pointer to objects in object-cache.
  template <typename T>
  typename AllocatorT::WriteHandle allocateFromL1(folly::StringPiece key,
                                                  uint32_t ttl,
                                                  uint32_t creationTime);

  // Number of shards (LRUs) to lessen the contention on L1 cache
  size_t l1NumShards_{};

  // Above this many entries, L1 will start evicting
  const size_t l1EntriesLimit_{};

  // Whether tracking the size of each object inside object-cache
  const bool objectSizeTrackingEnabled_{};

  // They take up space so we can control exact number of items in cache
  std::vector<typename AllocatorT::WriteHandle> placeholders_;

  TLCounter evictions_{};
  TLCounter lookups_;
  TLCounter succL1Lookups_;
  TLCounter inserts_;
  TLCounter insertErrors_;
  TLCounter replaces_;
  TLCounter removes_;
  std::atomic<size_t> totalObjectSizeBytes_{0};

  friend class test::ObjectCacheTest<AllocatorT>;
};
} // namespace objcache2
} // namespace cachelib
} // namespace facebook
#include "cachelib/experimental/objcache2/ObjectCache-inl.h"
