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

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/object_cache/ObjectCacheBase.h"
#include "cachelib/object_cache/ObjectCacheConfig.h"
#include "cachelib/object_cache/ObjectCacheSizeController.h"
#include "cachelib/object_cache/ObjectCacheSizeDistTracker.h"
#include "cachelib/object_cache/persistence/Persistence.h"
#include "cachelib/object_cache/util/ThreadMemoryTracker.h"

namespace facebook::cachelib::objcache2 {
namespace test {
template <typename AllocatorT>
class ObjectCacheTest;
}

struct ObjectCacheItem {
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
                            uint32_t expiryTime,
                            uint32_t creationTime,
                            uint32_t lastAccessTime)
      : context(ctx),
        objectPtr(ptr),
        key(k),
        expiryTime(expiryTime),
        creationTime(creationTime),
        lastAccessTime(lastAccessTime) {}

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

  // the creation time of the object
  uint32_t creationTime;

  // the last time this object was accessed
  uint32_t lastAccessTime;
};

template <typename AllocatorT>
class ObjectCache : public ObjectCacheBase<AllocatorT> {
 private:
  // make constructor private, but constructable by std::make_unique
  struct InternalConstructor {};

  template <typename T>
  class Deleter {
   public:
    using ReadHandle = typename AllocatorT::ReadHandle;
    using WriteHandle = typename AllocatorT::WriteHandle;
    using Handle = std::variant<ReadHandle, WriteHandle>;

    explicit Deleter(typename AllocatorT::ReadHandle&& hdl)
        : hdl_(std::move(hdl)) {}
    explicit Deleter(typename AllocatorT::WriteHandle&& hdl)
        : hdl_(std::move(hdl)) {}

    void operator()(T*) {
      // Just release the handle.
      // Cache destroys object when all handles released.
      std::holds_alternative<ReadHandle>(hdl_)
          ? std::get<ReadHandle>(hdl_).reset()
          : std::get<WriteHandle>(hdl_).reset();
    }

    WriteHandle& getWriteHandleRef() {
      if (std::holds_alternative<ReadHandle>(hdl_)) {
        hdl_ = std::move(std::get<ReadHandle>(hdl_)).toWriteHandle();
      }
      return std::get<WriteHandle>(hdl_);
    }

    ReadHandle& getReadHandleRef() {
      return std::holds_alternative<ReadHandle>(hdl_)
                 ? std::get<ReadHandle>(hdl_)
                 : std::get<WriteHandle>(hdl_);
    }

   private:
    Handle hdl_;
  };

 public:
  using ItemDestructor = std::function<void(ObjectCacheDestructorData)>;
  using RemoveCb = std::function<void(ObjectCacheDestructorData)>;
  using Key = KAllocation::Key;
  using Config = ObjectCacheConfig<ObjectCache<AllocatorT>>;
  using EvictionPolicyConfig = typename AllocatorT::MMType::Config;
  using Item = ObjectCacheItem;
  using Serializer = ObjectSerializer<ObjectCache<AllocatorT>>;
  using Deserializer = ObjectDeserializer<ObjectCache<AllocatorT>>;
  using SerializeCb = std::function<std::unique_ptr<folly::IOBuf>(Serializer)>;
  using DeserializeCb = std::function<bool(Deserializer)>;
  using Persistor = Persistor<ObjectCache<AllocatorT>>;
  using Restorer = Restorer<ObjectCache<AllocatorT>>;
  using EvictionIterator = typename AllocatorT::EvictionIterator;
  using AccessIterator = typename AllocatorT::AccessIterator;
  using NvmCache = typename AllocatorT::NvmCacheT;
  using NvmCacheConfig = typename AllocatorT::NvmCacheT::Config;
  using WriteHandle = typename AllocatorT::WriteHandle;
  using CacheItem = typename AllocatorT::Item;

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

  // Return whether an object exists in cache without looking up the device.
  StorageMedium existFast(folly::StringPiece key);

  // Look up an object in mutable access
  // @param key   the key to the object
  //
  // @throw cachelib::exception::RefcountOverflow if the item we are replacing
  //        is already out of refcounts.
  // @return shared pointer to a mutable version of the object
  template <typename T>
  std::shared_ptr<T> findToWrite(folly::StringPiece key);

  // Quickly peek an object in mutable access
  // Unlike findToWrite, this API doesn't update access frequency or stat. It
  // also ignores the nvm cache and only does RAM lookup.
  // It can be used when caller wants to quickly check item's existence to
  // mutate the object without changing the item's ranking or stats
  // @param key   the key to the object
  //
  // @return shared pointer to a mutable version of the object
  template <typename T>
  std::shared_ptr<T> peekToWrite(folly::StringPiece key);

  // Insert the object into the cache with given key. If the key exists in the
  // cache, it will be replaced with new obejct.
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
  // @return a tuple of allocation status, shared_ptr of newly inserted
  //         object and shared_ptr of old object that has been replaced (nullptr
  //         if no replacement happened)
  template <typename T>
  std::tuple<AllocStatus, std::shared_ptr<T>, std::shared_ptr<T>>
  insertOrReplace(folly::StringPiece key,
                  std::unique_ptr<T> object,
                  size_t objectSize = 0,
                  uint32_t ttlSecs = 0);

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
  //
  // @return false if the key is not found in object-cache
  bool remove(folly::StringPiece key);

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

  // Return whether a key is valid. The length of the key needs to be in (0,
  // kKeyMaxLenSmall) to be valid.
  bool isKeyValid(Key key) const;

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

  // Get an iterator to iterate over all items in object-cache.
  AccessIterator begin() { return this->l1Cache_->begin(); }

  AccessIterator end() { return this->l1Cache_->end(); }

  // Get the default l1 allocation size in bytes.
  static uint32_t getL1AllocSize(uint32_t maxKeySizeBytes);

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

  // Get the expiry timestamp of the object
  // @param  object   object shared pointer returned from ObjectCache APIs
  //
  // @return the expiry timestamp in seconds of the object
  //         0 if object is nullptr
  template <typename T>
  uint32_t getExpiryTimeSec(const std::shared_ptr<T>& object) const {
    if (object == nullptr) {
      return 0;
    }
    return getReadHandleRefInternal<T>(object)->getExpiryTime();
  }

  // Get the configured TTL of the object
  // @param  object   object shared pointer returned from ObjectCache APIs
  //
  // @return the configured TTL in seconds of the object
  //         0 if object is nullptr
  template <typename T>
  static std::chrono::seconds getConfiguredTtl(
      const std::shared_ptr<T>& object) {
    if (object == nullptr) {
      return std::chrono::seconds{0};
    }
    return getReadHandleRefInternal<T>(object)->getConfiguredTTL();
  }

  // Get the last access timestamp of the object
  // @param  object   object shared pointer returned from ObjectCache APIs
  //
  // @return the last accessed timestamp in seconds of the object
  //         0 if object is nullptr
  template <typename T>
  uint32_t getLastAccessTimeSec(std::shared_ptr<T>& object) {
    if (object == nullptr) {
      return 0;
    }
    return getReadHandleRefInternal<T>(object)->getLastAccessTime();
  }

  // Get the creation timestamp of the object
  // @param  object   object shared pointer returned from ObjectCache APIs
  //
  // @return the create timestamp in seconds of the object
  //         0 if object is nullptr
  template <typename T>
  uint32_t getCreationTimeSec(std::shared_ptr<T>& object) {
    if (object == nullptr) {
      return 0;
    }
    return getReadHandleRefInternal<T>(object)->getCreationTime();
  }

  // Update the expiry timestamp of an object
  //
  // @param  object         object shared pointer returned from ObjectCache APIs
  // @param  expiryTimeSecs the expiryTime in seconds to update
  //
  // @return boolean indicating whether expiry time was successfully updated
  template <typename T>
  bool updateExpiryTimeSec(std::shared_ptr<T>& object,
                           uint32_t expiryTimeSecs) {
    if (object == nullptr) {
      return false;
    }
    return getWriteHandleRefInternal<T>(object)->updateExpiryTime(
        expiryTimeSecs);
  }

  // Update expiry time to @ttl seconds from now.
  //
  // @param  object    object shared pointer returned from ObjectCache APIs
  // @param  ttl       TTL in seconds (from now)
  //
  // @return boolean indicating whether TTL was successfully extended
  template <typename T>
  bool extendTtl(std::shared_ptr<T>& object, std::chrono::seconds ttl) {
    if (object == nullptr) {
      return false;
    }
    return getWriteHandleRefInternal<T>(object)->extendTTL(ttl);
  }

  // Mutate object and update the object size
  // When size-awareness is enabled, users must call this API to mutate the
  // object. Otherwise, we won't be able to track the updated object size
  //
  // @param  object       shared pointer of the object to be mutated (must be
  //                      fetched from ObjectCache APIs)
  // @param  mutateCb     callback containing the mutation logic
  template <typename T>
  void mutateObject(const std::shared_ptr<T>& object,
                    std::function<void()> mutateCb);

  // Get the size of the object
  //
  // @param  object       object shared pointer returned from ObjectCache APIs
  //
  // @return the object size if size-awareness is enabled
  //         0 otherwise
  template <typename T>
  size_t getObjectSize(const std::shared_ptr<T>& object) const {
    if (!object) {
      return 0;
    }
    return reinterpret_cast<const ObjectCacheItem*>(
               getReadHandleRefInternal<T>(object)->getMemory())
        ->objectSize;
  }

  size_t getObjectSize(AccessIterator& itr) const {
    if (!itr.asHandle()) {
      return 0;
    }
    return reinterpret_cast<const ObjectCacheItem*>(itr.asHandle()->getMemory())
        ->objectSize;
  }

  // Update the object size without updating the object itself.
  // This is useful when object has been changed, although it's not changed
  // by mutateObject.
  //
  // @param  object       shared pointer of the object whose size is being
  //                      updated (must be fetched from ObjectCache APIs)
  // @param  newSize      new object size
  //
  // @return boolean indicating whether the object size was successfully updated
  template <typename T>
  bool updateObjectSize(const std::shared_ptr<T>& object, size_t newSize);

  // Stop all workers
  //
  // @return true if all workers have been successfully stopped
  bool stopAllWorkers(std::chrono::seconds timeout = std::chrono::seconds{0}) {
    bool success = true;
    success &=
        util::stopPeriodicWorker(kSizeControllerName, sizeController_, timeout);
    success &= util::stopPeriodicWorker(kSizeDistTrackerName, sizeDistTracker_,
                                        timeout);
    // Nullproof. This function can be called in the destructor before init()
    // completes successfully.
    if (this->l1Cache_) {
      success &= this->l1Cache_->stopWorkers(timeout);
    }
    return success;
  }

  // No-op for workers that are already running. Typically user uses this in
  // conjunction with `config.setDelayCacheWorkersStart()` to avoid
  // initialization ordering issues with user callback for cachelib's workers.
  void startCacheWorkers();

 protected:
  // Serialize cache allocator config for exporting to Scuba
  std::map<std::string, std::string> serializeConfigParams() const override;

 private:
  // Minimum alloc size in bytes for l1 cache.
  static constexpr uint32_t kL1AllocSizeMin = 64;
  static constexpr const char* kPlaceholderKey = "_cl_ph";

  // Names of periodic workers
  static constexpr folly::StringPiece kSizeControllerName{"SizeController"};
  static constexpr folly::StringPiece kSizeDistTrackerName{"SizeDistTracker"};

  void init();

  void initWorkers();

  // Allocate an item handle from the interal cache allocator. This item's
  // storage is used to cache pointer to objects in object-cache.
  typename AllocatorT::WriteHandle allocateFromL1(folly::StringPiece key,
                                                  uint32_t ttl,
                                                  uint32_t creationTime);

  // Allocate the placeholder and add it to the placeholder vector.
  //
  // @return true if the allocation is successful
  bool allocatePlaceholder();

  // Returns the total number of placeholders
  size_t getNumPlaceholders() const { return placeholders_.size(); }

  // Get a ReadHandle reference from the object shared_ptr
  template <typename T>
  static typename AllocatorT::ReadHandle& getReadHandleRefInternal(
      const std::shared_ptr<T>& object) {
    auto* deleter = std::get_deleter<Deleter<T>>(object);
    XDCHECK(deleter != nullptr);
    auto& hdl = deleter->getReadHandleRef();
    XDCHECK(hdl != nullptr);
    return hdl;
  }

  // Get a WriteHandle reference from the object shared_ptr
  template <typename T>
  static typename AllocatorT::WriteHandle& getWriteHandleRefInternal(
      const std::shared_ptr<T>& object) {
    auto* deleter = std::get_deleter<Deleter<T>>(object);
    XDCHECK(deleter != nullptr);
    auto& hdl = deleter->getWriteHandleRef();
    XDCHECK(hdl != nullptr);
    return hdl;
  }

  EvictionIterator getEvictionIterator(PoolId pid) const noexcept {
    auto& mmContainer = this->l1Cache_->getMMContainer(pid, 0 /* classId */);
    return mmContainer.getEvictionIterator();
  }

  // Config passed to the cache.
  Config config_{};

  // They take up space so we can control exact number of items in cache
  std::vector<typename AllocatorT::WriteHandle> placeholders_;

  // A periodic worker that controls the total object size to be limited by
  // cache size limit
  std::unique_ptr<ObjectCacheSizeController<AllocatorT>> sizeController_;

  // A periodic worker that tracks the object size distribution stats.
  std::unique_ptr<ObjectCacheSizeDistTracker<ObjectCache<AllocatorT>>>
      sizeDistTracker_;

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

template <typename AllocatorT>
void ObjectCache<AllocatorT>::init() {
  // Compute variables required to size the cache and placeholders
  DCHECK_GE(config_.l1EntriesLimit, config_.l1NumShards);
  auto l1AllocSize = getL1AllocSize(config_.maxKeySizeBytes);
  const size_t allocsPerSlab = Slab::kSize / l1AllocSize;
  const size_t allocsPerShard =
      util::getDivCeiling(config_.l1EntriesLimit, config_.l1NumShards);
  const size_t slabsPerShard =
      util::getDivCeiling(allocsPerShard, allocsPerSlab);
  const size_t perPoolSize = slabsPerShard * Slab::kSize;
  const size_t l1SizeRequired = perPoolSize * config_.l1NumShards;
  auto cacheSize = l1SizeRequired + Slab::kSize;

  typename AllocatorT::Config l1Config;
  l1Config.setCacheName(config_.cacheName)
      .setCacheSize(cacheSize)
      .setAccessConfig({config_.l1HashTablePower, config_.l1LockPower})
      .setDefaultAllocSizes({l1AllocSize})
      .enableItemReaperInBackground(config_.reaperInterval)
      .setEventTracker(std::move(config_.legacyEventTracker))
      .setEvictionSearchLimit(config_.evictionSearchLimit);
  if (config_.itemDestructor) {
    l1Config.setItemDestructor(
        [this](typename AllocatorT::DestructorData data) {
          ObjectCacheDestructorContext ctx;
          if (data.context == DestructorContext::kEvictedFromRAM) {
            evictions_.inc();
            ctx = ObjectCacheDestructorContext::kEvicted;
          } else if (data.context == DestructorContext::kRemovedFromRAM) {
            ctx = ObjectCacheDestructorContext::kRemoved;
          } else { // should not enter here
            ctx = ObjectCacheDestructorContext::kUnknown;
          }

          auto& item = data.item;

          auto itemPtr = reinterpret_cast<ObjectCacheItem*>(item.getMemory());

          SCOPE_EXIT {
            if (config_.objectSizeTrackingEnabled) {
              // update total object size
              totalObjectSizeBytes_.fetch_sub(itemPtr->objectSize,
                                              std::memory_order_relaxed);
            }
            // execute user defined item destructor
            config_.itemDestructor(ObjectCacheDestructorData(
                ctx, itemPtr->objectPtr, item.getKey(), item.getExpiryTime(),
                item.getCreationTime(), item.getLastAccessTime()));
          };
        });
  } else {
    l1Config.setRemoveCallback([this](typename AllocatorT::RemoveCbData data) {
      ObjectCacheDestructorContext ctx;
      if (data.context == RemoveContext::kEviction) {
        evictions_.inc();
        ctx = ObjectCacheDestructorContext::kEvicted;
      } else if (data.context == RemoveContext::kNormal) {
        ctx = ObjectCacheDestructorContext::kRemoved;
      } else { // should not enter here
        ctx = ObjectCacheDestructorContext::kUnknown;
      }

      auto& item = data.item;

      auto itemPtr = reinterpret_cast<ObjectCacheItem*>(item.getMemory());

      SCOPE_EXIT {
        if (config_.objectSizeTrackingEnabled) {
          // update total object size
          totalObjectSizeBytes_.fetch_sub(itemPtr->objectSize,
                                          std::memory_order_relaxed);
        }
        // execute user defined item destructor
        config_.removeCb(ObjectCacheDestructorData(
            ctx, itemPtr->objectPtr, item.getKey(), item.getExpiryTime(),
            item.getCreationTime(), item.getLastAccessTime()));
      };
    });
  }

  if (config_.delayCacheWorkersStart) {
    l1Config.setDelayCacheWorkersStart();
  }

  if (config_.nvmConfig.has_value()) {
    // Update the callback to track size
    config_.nvmConfig->makeObjCb =
        [this, makeObjCb = std::move(config_.nvmConfig->makeObjCb)](
            auto& nvmItem, auto& item, auto chain) {
          auto ret = makeObjCb(nvmItem, item, chain);
          if (ret) {
            totalObjectSizeBytes_.fetch_add(
                item.template getMemoryAs<ObjectCacheItem>()->objectSize);
          }
          return ret;
        };
    l1Config.nvmConfig.assign(std::move(config_.nvmConfig.value()));
  }

  if (config_.maxKeySizeBytes > KAllocation::kKeyMaxLenSmall) {
    l1Config.setAllowLargeKeys(true);
  }

  // Apply aggregate pool stats configuration if enabled
  if (config_.isAggregatePoolStatsEnabled()) {
    this->aggregatePoolStats_ = true;
  }

  this->l1Cache_ = std::make_unique<AllocatorT>(l1Config);
  // add a pool per shard
  for (size_t i = 0; i < config_.l1NumShards; i++) {
    std::string shardName =
        config_.l1ShardName.empty() ? "pool" : config_.l1ShardName;
    // Add the shard index as the suffix of pool name if needed
    if (config_.l1NumShards > 1) {
      shardName += folly::sformat("_{}", i);
    }

    if (config_.provisionPool) {
      PoolId pid = this->l1Cache_->addPool(
          shardName, perPoolSize, {} /* allocSizes */,
          config_.evictionPolicyConfig /* MMConfig*/,
          nullptr /* rebalanceStrategy */, nullptr /* resizeStrategy*/,
          true /* ensureProvisionable */);
      this->l1Cache_->provisionPool(pid,
                                    {static_cast<unsigned int>(slabsPerShard)});
    } else {
      this->l1Cache_->addPool(shardName, perPoolSize, {} /* allocSizes */,
                              config_.evictionPolicyConfig);
    }
  }

  // Allocate placeholder items such that the cache will fit no more than
  // "l1EntriesLimit" objects. In doing so, placeholders are distributed
  // evenly to each shard/pool, i.e., l1EntriesLimit / l1NumShards
  const size_t l1PlaceHoldersPerShard =
      slabsPerShard * allocsPerSlab - allocsPerShard;
  XDCHECK_GE(slabsPerShard * allocsPerSlab, allocsPerShard);
  XDCHECK_LT(l1PlaceHoldersPerShard, allocsPerSlab);

  // allocsPerShard is celing of the division by numShards, meaning
  // additional number (i.e., extraLimit) of placesholders need to be created
  const size_t extraLimit =
      allocsPerShard * config_.l1NumShards - config_.l1EntriesLimit;
  XDCHECK_GE(allocsPerShard * config_.l1NumShards, config_.l1EntriesLimit);
  const size_t l1PlaceHolders =
      l1PlaceHoldersPerShard * config_.l1NumShards + extraLimit;
  for (size_t i = 0; i < l1PlaceHolders; i++) {
    if (!allocatePlaceholder()) {
      throw std::runtime_error(
          fmt::format("Couldn't allocate placeholder {}", i));
    }
  }

  if (!config_.delayCacheWorkersStart) {
    initWorkers();
  }
}

template <typename AllocatorT>
void ObjectCache<AllocatorT>::startCacheWorkers() {
  if (config_.delayCacheWorkersStart) {
    this->l1Cache_->startCacheWorkers();
    initWorkers();
  }
}

template <typename AllocatorT>
void ObjectCache<AllocatorT>::initWorkers() {
  if ((config_.objectSizeTrackingEnabled &&
       config_.sizeControllerIntervalMs != 0)) {
    util::startPeriodicWorker(
        kSizeControllerName, sizeController_,
        std::chrono::milliseconds{config_.sizeControllerIntervalMs}, *this,
        config_.sizeControllerThrottlerConfig);
  }

  if (config_.objectSizeTrackingEnabled &&
      config_.objectSizeDistributionTrackingEnabled) {
    util::startPeriodicWorker(
        kSizeDistTrackerName, sizeDistTracker_,
        std::chrono::seconds{60} /*default interval to be 60s*/, *this);
  }
}

template <typename AllocatorT>
std::unique_ptr<ObjectCache<AllocatorT>> ObjectCache<AllocatorT>::create(
    Config config) {
  auto obj =
      std::make_unique<ObjectCache>(InternalConstructor(), std::move(config));
  obj->init();
  return obj;
}

template <typename AllocatorT>
StorageMedium ObjectCache<AllocatorT>::existFast(folly::StringPiece key) {
  return this->l1Cache_->existFast(key);
}

template <typename AllocatorT>
template <typename T>
std::shared_ptr<const T> ObjectCache<AllocatorT>::find(folly::StringPiece key) {
  lookups_.inc();
  auto found = this->l1Cache_->find(key);
  if (!found) {
    return nullptr;
  }
  succL1Lookups_.inc();

  auto ptr = found->template getMemoryAs<ObjectCacheItem>()->objectPtr;
  // Use custom deleter
  auto deleter = Deleter<const T>(std::move(found));
  return std::shared_ptr<const T>(reinterpret_cast<const T*>(ptr),
                                  std::move(deleter));
}

template <typename AllocatorT>
template <typename T>
std::shared_ptr<T> ObjectCache<AllocatorT>::findToWrite(
    folly::StringPiece key) {
  lookups_.inc();
  auto found = this->l1Cache_->findToWrite(key);
  if (!found) {
    return nullptr;
  }
  succL1Lookups_.inc();

  auto ptr = found->template getMemoryAs<ObjectCacheItem>()->objectPtr;
  // Use custom deleter
  auto deleter = Deleter<T>(std::move(found));
  return std::shared_ptr<T>(reinterpret_cast<T*>(ptr), std::move(deleter));
}

template <typename AllocatorT>
template <typename T>
std::shared_ptr<T> ObjectCache<AllocatorT>::peekToWrite(
    folly::StringPiece key) {
  auto found = this->l1Cache_->peek(key);
  if (!found) {
    return nullptr;
  }

  auto ptr = found->template getMemoryAs<ObjectCacheItem>()->objectPtr;
  // Use custom deleter
  auto deleter = Deleter<T>(std::move(found));
  return std::shared_ptr<T>(reinterpret_cast<T*>(ptr), std::move(deleter));
}

template <typename AllocatorT>
template <typename T>
std::tuple<typename ObjectCache<AllocatorT>::AllocStatus,
           std::shared_ptr<T>,
           std::shared_ptr<T>>
ObjectCache<AllocatorT>::insertOrReplace(folly::StringPiece key,
                                         std::unique_ptr<T> object,
                                         size_t objectSize,
                                         uint32_t ttlSecs) {
  if (config_.objectSizeTrackingEnabled && objectSize == 0) {
    throw std::invalid_argument(
        "Object size tracking is enabled but object size is set to be 0.");
  }

  if (!config_.objectSizeTrackingEnabled && objectSize != 0) {
    XLOGF_EVERY_MS(
        WARN, 60'000,
        "Object size tracking is not enabled but object size is set to be {}.",
        objectSize);
  }

  inserts_.inc();

  auto handle =
      allocateFromL1(key, ttlSecs, 0 /* use current time as creationTime */);
  if (!handle) {
    insertErrors_.inc();
    return {AllocStatus::kAllocError, std::shared_ptr<T>(std::move(object)),
            nullptr};
  }
  // We don't release the object here because insertOrReplace could throw when
  // the replaced item is out of refcount; in this case, the object isn't
  // inserted to the cache and releasing the object will cause memory leak.
  T* ptr = object.get();
  *handle->template getMemoryAs<ObjectCacheItem>() =
      ObjectCacheItem{reinterpret_cast<uintptr_t>(ptr), objectSize};

  // Update total object size. This should be done before inserting into L1
  // to avoid any race condition with the size controller at start up
  if (config_.objectSizeTrackingEnabled) {
    totalObjectSizeBytes_.fetch_add(objectSize, std::memory_order_relaxed);
  }

  auto replaced = this->l1Cache_->insertOrReplace(handle);

  std::shared_ptr<T> replacedPtr = nullptr;
  if (replaced) {
    replaces_.inc();
    auto itemPtr = reinterpret_cast<ObjectCacheItem*>(replaced->getMemory());
    // Just release the handle. Cache destorys object when all handles
    // released.
    auto deleter = [h = std::move(replaced)](T*) {};
    replacedPtr = std::shared_ptr<T>(reinterpret_cast<T*>(itemPtr->objectPtr),
                                     std::move(deleter));
  }

  // Release the object as it has been successfully inserted to the cache.
  object.release();

  // Use custom deleter
  auto deleter = Deleter<T>(std::move(handle));
  return {AllocStatus::kSuccess, std::shared_ptr<T>(ptr, std::move(deleter)),
          replacedPtr};
}

template <typename AllocatorT>
template <typename T>
std::pair<typename ObjectCache<AllocatorT>::AllocStatus, std::shared_ptr<T>>
ObjectCache<AllocatorT>::insert(folly::StringPiece key,
                                std::unique_ptr<T> object,
                                size_t objectSize,
                                uint32_t ttlSecs) {
  if (config_.objectSizeTrackingEnabled && objectSize == 0) {
    throw std::invalid_argument(
        "Object size tracking is enabled but object size is set to be 0.");
  }

  if (!config_.objectSizeTrackingEnabled && objectSize != 0) {
    throw std::invalid_argument(
        "Object size tracking is not enabled but object size is set. Are you "
        "trying to set TTL?");
  }

  inserts_.inc();

  auto handle =
      allocateFromL1(key, ttlSecs, 0 /* use current time as creationTime */);
  if (!handle) {
    insertErrors_.inc();
    return {AllocStatus::kAllocError, std::shared_ptr<T>(std::move(object))};
  }
  T* ptr = object.get();
  *handle->template getMemoryAs<ObjectCacheItem>() =
      ObjectCacheItem{reinterpret_cast<uintptr_t>(ptr), objectSize};

  auto success = this->l1Cache_->insert(handle);
  if (!success) {
    return {AllocStatus::kKeyAlreadyExists,
            std::shared_ptr<T>(std::move(object))};
  }

  // update total object size
  if (config_.objectSizeTrackingEnabled) {
    totalObjectSizeBytes_.fetch_add(objectSize, std::memory_order_relaxed);
  }
  // Release the handle now since we have inserted the handle into the cache,
  // and from now the Cache will be responsible for destroying the object
  // when it's evicted/removed.
  object.release();

  // Use custom deleter
  auto deleter = Deleter<T>(std::move(handle));
  return {AllocStatus::kSuccess, std::shared_ptr<T>(ptr, std::move(deleter))};
}

template <typename AllocatorT>
typename AllocatorT::WriteHandle ObjectCache<AllocatorT>::allocateFromL1(
    folly::StringPiece key, uint32_t ttl, uint32_t creationTime) {
  PoolId poolId = 0;
  if (config_.l1NumShards > 1) {
    auto hash = cachelib::MurmurHash2{}(key.data(), key.size());
    poolId = static_cast<PoolId>(hash % config_.l1NumShards);
  }
  return this->l1Cache_->allocate(poolId, key, sizeof(ObjectCacheItem), ttl,
                                  creationTime);
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::allocatePlaceholder() {
  // rotate pools so that the number of placeholders for each pool is balanced
  auto poolId = static_cast<PoolId>(getNumPlaceholders() % config_.l1NumShards);
  auto hdl = this->l1Cache_->allocate(poolId, kPlaceholderKey,
                                      sizeof(ObjectCacheItem), 0 /* no ttl */,
                                      0 /* use current time as creationTime */);
  if (!hdl) {
    return false;
  }
  placeholders_.push_back(std::move(hdl));
  return true;
}

template <typename AllocatorT>
uint32_t ObjectCache<AllocatorT>::getL1AllocSize(uint32_t maxKeySizeBytes) {
  auto requiredSizeBytes = AllocatorT::Item::getRequiredSize(
      maxKeySizeBytes, sizeof(ObjectCacheItem));
  if (requiredSizeBytes <= kL1AllocSizeMin) {
    return kL1AllocSizeMin;
  }
  return util::getAlignedSize(static_cast<uint32_t>(requiredSizeBytes),
                              8 /* alloc size must be aligned to 8 bytes */);
}

template <typename AllocatorT>
ObjectCache<AllocatorT>::~ObjectCache() {
  stopAllWorkers();

  if (this->l1Cache_) {
    for (auto itr = this->l1Cache_->begin(); itr != this->l1Cache_->end();
         ++itr) {
      this->l1Cache_->remove(itr.asHandle());
    }
  }
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::remove(folly::StringPiece key) {
  removes_.inc();
  return this->l1Cache_->remove(key) == AllocatorT::RemoveRes::kSuccess;
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::isKeyValid(Key key) const {
  return this->l1Cache_->isKeyValid(key);
}

template <typename AllocatorT>
void ObjectCache<AllocatorT>::getObjectCacheCounters(
    const util::CounterVisitor& visitor) const {
  visitor("objcache.lookups", lookups_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.lookups.l1_hits", succL1Lookups_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.inserts", inserts_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.inserts.errors", insertErrors_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.replaces", replaces_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.removes", removes_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.evictions", evictions_.get(),
          util::CounterVisitor::CounterType::RATE);
  visitor("objcache.object_size_bytes", getTotalObjectSize());
  if (sizeController_) {
    sizeController_->getCounters(visitor);
  }

  if (sizeDistTracker_) {
    sizeDistTracker_->getCounters(visitor);
  }
}

template <typename AllocatorT>
std::map<std::string, std::string>
ObjectCache<AllocatorT>::serializeConfigParams() const {
  auto config = this->l1Cache_->serializeConfigParams();
  config["l1EntriesLimit"] = std::to_string(config_.l1EntriesLimit);
  config["l1NumShards"] = std::to_string(config_.l1NumShards);
  config["aggregatePoolStats"] = config_.aggregatePoolStats ? "true" : "false";
  if (config_.objectSizeTrackingEnabled &&
      config_.sizeControllerIntervalMs > 0) {
    config["totalObjectSizeLimit"] =
        std::to_string(config_.totalObjectSizeLimit);
    config["sizeControllerIntervalMs"] =
        std::to_string(config_.sizeControllerIntervalMs);
  }
  return config;
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::persist() {
  if (config_.persistBaseFilePath.empty() || !config_.serializeCb) {
    return false;
  }

  // Stop all the other workers before persist
  if (!stopAllWorkers()) {
    return false;
  }

  Persistor persistor(config_.persistThreadCount, config_.persistBaseFilePath,
                      config_.serializeCb, *this);
  return persistor.run();
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::recover() {
  if (config_.persistBaseFilePath.empty() || !config_.deserializeCb) {
    return false;
  }
  Restorer restorer(config_.persistBaseFilePath, config_.deserializeCb, *this);
  return restorer.run();
}

template <typename AllocatorT>
template <typename T>
void ObjectCache<AllocatorT>::mutateObject(const std::shared_ptr<T>& object,
                                           std::function<void()> mutateCb) {
  if (!object) {
    return;
  }

  cachelib::objcache2::ThreadMemoryTracker tMemTracker;
  size_t memUsageBefore = tMemTracker.getMemUsageBytes();
  mutateCb();
  size_t memUsageAfter = tMemTracker.getMemUsageBytes();

  auto& hdl = getWriteHandleRefInternal<T>(object);
  size_t memUsageDiff = 0;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Watomic-alignment"
  if (memUsageAfter > memUsageBefore) { // updated to a larger value
    memUsageDiff = memUsageAfter - memUsageBefore;
    // do atomic update on objectSize
    ObjectCacheItem* o = reinterpret_cast<ObjectCacheItem*>(hdl->getMemory());
    __atomic_fetch_add(&(o->objectSize), memUsageDiff, __ATOMIC_SEQ_CST);
    totalObjectSizeBytes_.fetch_add(memUsageDiff, std::memory_order_relaxed);
  } else if (memUsageAfter < memUsageBefore) { // updated to a smaller value
    memUsageDiff = memUsageBefore - memUsageAfter;
    // do atomic update on objectSize
    ObjectCacheItem* o = reinterpret_cast<ObjectCacheItem*>(hdl->getMemory());
    __atomic_fetch_sub(&(o->objectSize), memUsageDiff, __ATOMIC_SEQ_CST);
    totalObjectSizeBytes_.fetch_sub(memUsageDiff, std::memory_order_relaxed);
  }
#pragma clang diagnostic pop
}

template <typename AllocatorT>
template <typename T>
bool ObjectCache<AllocatorT>::updateObjectSize(const std::shared_ptr<T>& object,
                                               size_t newSize) {
  if (!object) {
    return false;
  }
  if (!config_.objectSizeTrackingEnabled) {
    XLOG_EVERY_MS(
        WARN, 60'000,
        "Object size tracking is not enabled but object size being updated.");
    return false;
  }
  if (newSize == 0) {
    XLOG_EVERY_MS(
        WARN, 60'000,
        "Object size tracking is enabled but object size is updated to be 0.");
    return false;
  }

  // do atomic update on objectSize
  const auto oldSize = __sync_lock_test_and_set(
      &(reinterpret_cast<ObjectCacheItem*>(
            getWriteHandleRefInternal<T>(object)->getMemory())
            ->objectSize),
      newSize);
  if (newSize > oldSize) {
    totalObjectSizeBytes_.fetch_add(newSize - oldSize,
                                    std::memory_order_relaxed);
  } else if (newSize < oldSize) {
    totalObjectSizeBytes_.fetch_sub(oldSize - newSize,
                                    std::memory_order_relaxed);
  }
  return true;
}
} // namespace facebook::cachelib::objcache2
