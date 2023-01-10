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

#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>

#include <any>
#include <atomic>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/EventInterface.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/common/Time.h"
#include "cachelib/experimental/objcache2/ObjectCacheBase.h"
#include "cachelib/experimental/objcache2/ObjectCacheConfig.h"
#include "cachelib/experimental/objcache2/ObjectCacheSizeController.h"
#include "cachelib/experimental/objcache2/persistence/Persistence.h"
#include "cachelib/experimental/objcache2/persistence/gen-cpp2/persistent_data_types.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

namespace test {
template <typename AllocatorT>
class ObjectCacheTest;
}

struct FOLLY_PACK_ATTR ObjectCacheItem {
  uintptr_t objectPtr;
  size_t objectSize;
};

enum class ObjectCacheDestructorContext {
  // evicted from cache
  kEvicted,
  // removed by user calling remove()/insertOrReplace() or due to expired
  kRemoved,
  // unknown cases
  kUnknown,
};

struct ObjectCacheDestructorData {
  ObjectCacheDestructorData(ObjectCacheDestructorContext ctx,
                            uintptr_t ptr,
                            const KAllocation::Key& k,
                            uint32_t expiryTime)
      : context(ctx), objectPtr(ptr), key(k), expiryTime(expiryTime) {}

  // release the evicted/removed/expired object memory
  template <typename T>
  void deleteObject() {
    delete reinterpret_cast<T*>(objectPtr);
  }

  // remove or eviction
  ObjectCacheDestructorContext context;

  // pointer of the evicted/removed/expired object
  uintptr_t objectPtr;

  // the key corresponding to the evicted/removed/expired object
  const KAllocation::Key& key;

  // the expiry time of the object
  uint32_t expiryTime;
};

template <typename AllocatorT>
class ObjectCache : public ObjectCacheBase<AllocatorT> {
 private:
  // make constructor private, but constructable by std::make_unique
  struct InternalConstructor {};

 public:
  using ItemDestructor = std::function<void(ObjectCacheDestructorData)>;
  using Key = KAllocation::Key;
  using Config = ObjectCacheConfig<ObjectCache<AllocatorT>>;
  using Item = ObjectCacheItem;
  using Serializer = ObjectSerializer<ObjectCache<AllocatorT>>;
  using Deserializer = ObjectDeserializer<ObjectCache<AllocatorT>>;
  using SerializeCb = std::function<std::unique_ptr<folly::IOBuf>(Serializer)>;
  using DeserializeCb = std::function<bool(Deserializer)>;
  using Persistor = Persistor<ObjectCache<AllocatorT>>;
  using Restorer = Restorer<ObjectCache<AllocatorT>>;

  enum class AllocStatus { kSuccess, kAllocError, kKeyAlreadyExists };

  explicit ObjectCache(InternalConstructor, const Config& config)
      : config_(config.validate()) {}

  // Create an ObjectCache to store objects of one or more types
  //    - ItemDestructor must be set from ObjectCacheConfig
  //    - Inside ItemDestructor, `ctx.deleteObject<T>()` must be called to
  //      delete the objects (also see example in ObjectCacheConfig)
  static std::unique_ptr<ObjectCache<AllocatorT>> create(Config config);

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

  // Persist all non-expired objects in the cache if cache persistence is
  // enabled.
  // No-op if cache persistence is not enabled.
  // @return false if no persistence happened
  bool persist();

  // Recover non-expired objects to the cache if cache persistence is
  // enabled.
  // No-op if cache persistence is not enabled.
  // @return false if no recovery happened
  bool recover();

  // Get all the stats related to object-cache
  // @param visitor   callback that will be invoked with
  //                  {stat-name, value} for each stat
  void getObjectCacheCounters(
      const util::CounterVisitor& visitor) const override;

  // Return the number of objects in cache
  uint64_t getNumEntries() const {
    return this->l1Cache_->getAccessContainerNumKeys();
  }

  // Get direct access to the interal CacheAllocator.
  // This is only used in tests.
  AllocatorT& getL1Cache() { return *this->l1Cache_; }

  // Get the default l1 allocation size in bytes.
  static uint32_t getL1AllocSize(uint8_t maxKeySizeBytes);

  // Get the total size of all cached objects in bytes.
  size_t getTotalObjectSize() const {
    return totalObjectSizeBytes_.load(std::memory_order_relaxed);
  }

  // Get the current L1 entries number limit.
  size_t getCurrentEntriesLimit() const {
    return sizeController_ == nullptr
               ? config_.l1EntriesLimit
               : sizeController_->getCurrentEntriesLimit();
  }

 protected:
  // Serialize cache allocator config for exporting to Scuba
  std::map<std::string, std::string> serializeConfigParams() const override;

 private:
  // Minimum alloc size in bytes for l1 cache.
  static constexpr uint32_t kL1AllocSizeMin = 64;

  // Generate the key for the ith placeholder.
  static std::string getPlaceHolderKey(size_t i) {
    return fmt::format("_cl_ph_{}", i);
  }

  void init();

  // Allocate an item handle from the interal cache allocator. This item's
  // storage is used to cache pointer to objects in object-cache.
  typename AllocatorT::WriteHandle allocateFromL1(folly::StringPiece key,
                                                  uint32_t ttl,
                                                  uint32_t creationTime);

  // Allocate the placeholder and add it to the placeholder vector.
  //
  // @return true if the allocation is successful
  bool allocatePlaceholder(std::string key);

  // Start size controller
  //
  // @param interval   the period this worker fires
  // @param config     throttling config
  // @return true if size controller has been successfully started
  bool startSizeController(std::chrono::milliseconds interval,
                           const util::Throttler::Config& config);

  // Stop size controller
  //
  // @return true if size controller has been successfully stopped
  bool stopSizeController(std::chrono::seconds timeout = std::chrono::seconds{
                              0});

  // Config passed to the cache.
  Config config_{};

  // Number of shards (LRUs) to lessen the contention on L1 cache
  size_t l1NumShards_{};

  // They take up space so we can control exact number of items in cache
  std::vector<typename AllocatorT::WriteHandle> placeholders_;

  // A periodic worker that controls the total object size to be limited by
  // cache size limit
  std::unique_ptr<ObjectCacheSizeController<AllocatorT>> sizeController_;

  // Actual object size in total
  std::atomic<size_t> totalObjectSizeBytes_{0};

  TLCounter evictions_{};
  TLCounter lookups_;
  TLCounter succL1Lookups_;
  TLCounter inserts_;
  TLCounter insertErrors_;
  TLCounter replaces_;
  TLCounter removes_;

  friend class test::ObjectCacheTest<AllocatorT>;

  template <typename AllocatorT2>
  friend class ObjectCacheSizeController;

  friend Persistor;
};
} // namespace objcache2
} // namespace cachelib
} // namespace facebook
#include "cachelib/experimental/objcache2/ObjectCache-inl.h"
