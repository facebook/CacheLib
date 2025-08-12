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

#include <chrono>
#include <string>

#include "cachelib/allocator/KAllocation.h"
#include "cachelib/allocator/nvmcache/NvmItem.h"
#include "cachelib/common/EventInterface.h"
#include "cachelib/common/Throttler.h"
#include "cachelib/common/Utils.h"
#include "cachelib/object_cache/ObjectCacheSizeController.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

template <typename ObjectCache>
struct ObjectCacheConfig {
  using Key = KAllocation::Key;
  using EventTrackerSharedPtr = std::shared_ptr<EventInterface<Key>>;
  using ItemDestructor = typename ObjectCache::ItemDestructor;
  using RemoveCb = typename ObjectCache::RemoveCb;
  using SerializeCb = typename ObjectCache::SerializeCb;
  using DeserializeCb = typename ObjectCache::DeserializeCb;
  using EvictionPolicyConfig = typename ObjectCache::EvictionPolicyConfig;
  using NvmCacheConfig = typename ObjectCache::NvmCacheConfig;
  using ToBlobCb = std::function<std::unique_ptr<folly::IOBuf>(uintptr_t)>;
  using ToPtrCb = std::function<uintptr_t(folly::StringPiece)>;
  using GetFreeMemCb =
      typename ObjectCacheSizeController<ObjectCache>::GetFreeMemCb;
  using GetRSSMemCb =
      typename ObjectCacheSizeController<ObjectCache>::GetRSSMemCb;

  // Set cache name as a string
  ObjectCacheConfig& setCacheName(const std::string& _cacheName);

  // Set the cache capacity in terms of the number of objects and
  // the total object size.
  // the entries limit specifies the object number limit to be held in
  // cache. If the limit is exceeded, objects will be evicted.
  // The total object size limit needs to be set only to enable "size-aware"
  // object cache which tracks the object size internally and limits
  // the total memory size consumed by those objects.
  // When enabling the "size-aware" object cache, the size controller interval
  // should also be set to positive number which determines the interval
  // at which the size controller is invoked
  ObjectCacheConfig& setCacheCapacity(size_t _l1EntriesLimit,
                                      size_t _totalObjectSizeLimit = 0,
                                      int _sizeControllerIntervalMs = 0);

  // Set the memory mode to use from Object Size, FreeMemory and RSS.
  ObjectCacheConfig& setObjectSizeControllerMode(ObjCacheSizeControlMode _mode,
                                                 uint64_t _upperLimitBytes,
                                                 uint64_t _lowerLimitBytes);

  // Set a custom function to compute the free memory bytes.
  // If not set, the default function util::getMemAvailable() will be used.
  ObjectCacheConfig& setFreeMemCb(GetFreeMemCb _getFreeMemBytes);

  // Set a custom function to compute the RSS bytes.
  // If not set, the default function util::getRSSBytes() will be used.
  ObjectCacheConfig& setRSSMemCb(GetRSSMemCb _getRSSBytes);

  // Set the number of internal cache pools to be used for sharding.
  // This determines the number of concurrent inserts/removes. Default is 1
  ObjectCacheConfig& setNumShards(size_t _l1NumShards);

  // Set the shard name.
  // Once set, if l1NumShards == 1, l1ShardName will be the name;
  //           if l1NumShards > 1, we will use l1ShardName_0, l1ShardName_1,
  //           etc. as the name.
  // If not set, we will use pool_0, poo1_1, etc. as the default name.
  ObjectCacheConfig& setShardName(const std::string& _l1ShardName);

  // Set the maximum size of the key. The default is 255
  ObjectCacheConfig& setMaxKeySizeBytes(uint32_t _maxKeySizeBytes);

  // Set the access config for cachelib's access container
  ObjectCacheConfig& setAccessConfig(uint32_t _l1HashTablePower,
                                     uint32_t _l1LockPower);

  ObjectCacheConfig& setSizeControllerThrottlerConfig(
      util::Throttler::Config config);

  // Enable event tracker. This will log all relevant cache events.
  ObjectCacheConfig& setEventTracker(EventTrackerSharedPtr&& ptr);

  // You MUST set this callback to release the removed/evicted/expired objects
  // memory; otherwise, memory leak will happen.
  // 1) store a single type Foo
  // config.setItemDestructor([&](ObjectCacheDestructorData data) {
  //         data.deleteObject<Foo>();
  //     }
  // });
  //
  // 2) store multiple types
  // one way to do that is to encode the type in the key.
  // Example:
  // enum class user_defined_ObjectType {Foo1, Foo2, Foo3 };
  //
  // config.setItemDestructor([&](ObjectCacheDestructorData data) {
  //     switch (user_defined_getType(data.key)) {
  //       case user_defined_ObjectType::Foo1:
  //         data.deleteObject<Foo1>();
  //         break;
  //       case user_defined_ObjectType::Foo2:
  //         data.deleteObject<Foo2>();
  //         break;
  //       case user_defined_ObjectType::Foo3:
  //         data.deleteObject<Foo3>();
  //         break;
  //       ...
  //     }
  // });
  ObjectCacheConfig& setItemDestructor(ItemDestructor destructor);

  ObjectCacheConfig& setRemoveCb(RemoveCb cb);

  // Run in a multi-thread mode, eviction order is not guaranteed to persist.
  // @param threadCount          number of threads to work on persistence
  //                             concurrently
  // @param baseFilePath         metadata will be saved in "baseFilePath";
  //                             objects will be saved in "baseFilePath_i", i in
  //                             [0, threadCount)
  // @param serializeCallback    callback to serialize an object
  // @param deserializeCallback  callback to deserialize an object
  ObjectCacheConfig& enablePersistence(uint32_t threadCount,
                                       const std::string& basefilePath,
                                       SerializeCb serializeCallback,
                                       DeserializeCb deserializeCallback);

  // Run in a single-thread mode to persist the eviction order.
  // Please note: TinyLFU eviction policy is not supported.
  // @param baseFilePath         metadata will be saved in "baseFilePath";
  //                             objects will be saved in "baseFilePath_0"
  // @param serializeCallback    callback to serialize an object
  // @param deserializeCallback  callback to deserialize an object
  ObjectCacheConfig& enablePersistenceWithEvictionOrder(
      const std::string& basefilePath,
      SerializeCb serializeCallback,
      DeserializeCb deserializeCallback);

  // Enable tracking Jemalloc external fragmentation.
  ObjectCacheConfig& enableFragmentationTracking();

  ObjectCacheConfig& setItemReaperInterval(std::chrono::milliseconds interval);

  ObjectCacheConfig& setEvictionPolicyConfig(
      EvictionPolicyConfig _evictionPolicyConfig);

  ObjectCacheConfig& setEvictionSearchLimit(uint32_t _evictionSearchLimit);

  // We will delay worker start until user explicitly calls
  // ObjectCache::startCacheWorkers()
  ObjectCacheConfig& setDelayCacheWorkersStart();

  /**
   * Enable NVM cache.
   */
  ObjectCacheConfig& enableNvm(NvmCacheConfig config);
  /**
   * @param blobCb  Given the raw pointer of the object in DRAM, create a
   * unique_ptr<IOBuf> to a buffer that can be copied to NVM. The IOBuf does not
   * need to hold the underlying buffer if the buffer just wraps around the
   * original DRAM data. Return nullptr if the conversion fails.
   * @param ptrCb  Given the StringPiece that wraps around the NvmItem payload,
   * return a raw pointer of the object. Return nullptr if the conversion fails.
   */
  ObjectCacheConfig& overrideNvmCbs(ToBlobCb blobCb, ToPtrCb ptrCb);

  /**
   * Enable pool provisioning. See the doc for provisionPool.
   * Setting this to true would make cachelib take the entire space it is
   * configured to upon startup to avoid certain race conditions (S498497).
   * Keep it false if your object cache has an arbitrarilty large number of
   * entries.
   */
  ObjectCacheConfig& enablePoolProvisioning();

  // With size controller disabled, above this many entries, L1 will start
  // evicting.
  // With size controller enabled, this is only a hint used for initialization.
  size_t l1EntriesLimit{0};

  // This controls how many buckets are present in L1's hashtable
  uint32_t l1HashTablePower{10};

  // This controls how many locks are present in L1's hashtable
  uint32_t l1LockPower{10};

  // Number of shards to improve insert/remove concurrency
  size_t l1NumShards{1};

  // Name of the shard.
  std::string l1ShardName;

  // The cache name
  std::string cacheName;

  // The maximum key size in bytes. Default to 255 bytes which is the maximum
  // small key size cachelib supports.
  uint32_t maxKeySizeBytes{255};

  // If this is enabled, user has to pass the object size upon insertion
  bool objectSizeTrackingEnabled{false};

  // If this is enabled, we will track Jemalloc external fragmentation and add
  // the fragmentation bytes on top of total object size to bound the cache
  bool fragmentationTrackingEnabled{false};

  // If this is enabled, we will track the object size distribution and export
  // the stats to ods.
  bool objectSizeDistributionTrackingEnabled{false};

  // Period to fire size controller in milliseconds. 0 means size controller is
  // disabled.
  int sizeControllerIntervalMs{0};

  // With size controller enabled, if total object size is above this limit,
  // the cache will start evicting
  size_t totalObjectSizeLimit{0};

  // Throttler config of size controller
  util::Throttler::Config sizeControllerThrottlerConfig{};

  // Callback for initializing the eventTracker on CacheAllocator construction
  EventTrackerSharedPtr eventTracker{nullptr};

  // ItemDestructor which is invoked for each item that is evicted
  // or explicitly from cache
  ItemDestructor itemDestructor{};

  RemoveCb removeCb{};

  // time to sleep between each reaping period.
  std::chrono::milliseconds reaperInterval{5000};

  // The flag indicating whether cache persistence is enabled
  bool persistenceEnabled{false};

  // The thread number for cache persistence.
  // It sets the threads to run a persistor upon shut down and a restorer upon
  // restart. 0 means cache persistence is not enabled.
  uint32_t persistThreadCount{0};

  // The base file path to save the persistent data for cache persistence.
  // - Metadata will be saved in "baseFilePath";
  // - Objects will be saved in "baseFilePath_i" where i is in
  //   [0, persistThreadCount)
  // Empty means cache persistence is not enabled.
  std::string persistBaseFilePath{};

  // Serialize callback for cache persistence
  SerializeCb serializeCb{};

  // Deserialize callback for cache persistence
  DeserializeCb deserializeCb{};

  // Config of the eviction policy
  EvictionPolicyConfig evictionPolicyConfig{};

  // The maximum number of tries to search for an object to evict
  // 0 means it's infinite
  uint32_t evictionSearchLimit{50};

  // The memory mode to use from Object Size, FreeMemory and RSS
  // Object Size has no memory tracking but ensures the number of objects
  // and its total size are within the limit.
  // FreeMemory makes sure that the system has the specified amounts
  // of free memory at all times on top of Object Size mode guarantees.
  // RSS mode makes sure that the RSS of the process is within the limit
  // on top of Object Size mode guarantees.
  ObjCacheSizeControlMode memoryMode{ObjCacheSizeControlMode::ObjectSize};

  // The number of entries to add or remove per iteration.
  // The limits means different things for different memory modes.
  // For Object Size mode,
  //   - upperLimitBytes: max total object size limit (shrink cache)
  //   - lowerLimitBytes: min total object size limit (expand cache)
  // For FreeMemory mode,
  //   - upperLimitBytes: max free memory limit (expand cache)
  //   - lowerLimitBytes: min free memory limit (shrink cache)
  // For RSS mode,
  //   - upperLimitBytes: max RSS limit (shrink cache)
  //   - lowerLimitBytes: min RSS limit (expand cache)
  uint64_t upperLimitBytes{0};
  uint64_t lowerLimitBytes{0};
  GetFreeMemCb getFreeMemBytes = util::getMemAvailable;
  GetRSSMemCb getRSSMemBytes = util::getRSSBytes;

  // If true, we will delay worker start until user explicitly calls
  // ObjectCache::startCacheWorkers()
  bool delayCacheWorkersStart{false};

  std::optional<typename ObjectCache::NvmCacheConfig> nvmConfig{};

  // If true, we'll provision pools proactively upon creation.
  bool provisionPool{false};

  const ObjectCacheConfig& validate() const;
};

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setCacheName(
    const std::string& _cacheName) {
  cacheName = _cacheName;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setCacheCapacity(
    size_t _l1EntriesLimit,
    size_t _totalObjectSizeLimit,
    int _sizeControllerIntervalMs) {
  l1EntriesLimit = _l1EntriesLimit;
  totalObjectSizeLimit = _totalObjectSizeLimit;
  sizeControllerIntervalMs = _sizeControllerIntervalMs;

  if (_totalObjectSizeLimit && _sizeControllerIntervalMs) {
    // object size tracking is enabled as well
    objectSizeTrackingEnabled = true;
  } else if (_sizeControllerIntervalMs || _totalObjectSizeLimit) {
    throw std::invalid_argument(
        "Both of sizeControllerIntervalMs and totalObjectSizeLimit should be "
        "provided to enable the size controller");
  }
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setObjectSizeControllerMode(
    ObjCacheSizeControlMode _memoryMode,
    uint64_t _upperLimitBytes,
    uint64_t _lowerLimitBytes) {
  if (!objectSizeTrackingEnabled) {
    throw std::invalid_argument(
        "Enable object size tracking before setting size controller mode.");
  }
  memoryMode = _memoryMode;
  upperLimitBytes = _upperLimitBytes;
  lowerLimitBytes = _lowerLimitBytes;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setFreeMemCb(
    GetFreeMemCb _getFreeMemBytes) {
  getFreeMemBytes = _getFreeMemBytes;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setRSSMemCb(
    GetRSSMemCb _getRSSBytes) {
  getRSSMemBytes = _getRSSBytes;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setNumShards(size_t _l1NumShards) {
  l1NumShards = _l1NumShards;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setShardName(
    const std::string& _l1ShardName) {
  l1ShardName = _l1ShardName;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setMaxKeySizeBytes(
    uint32_t _maxKeySizeBytes) {
  XDCHECK_GT(_maxKeySizeBytes, 0U) << "Must allow keys larger than 0 bytes";
  maxKeySizeBytes = _maxKeySizeBytes;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setAccessConfig(
    uint32_t _l1HashTablePower, uint32_t _l1LockPower) {
  l1HashTablePower = _l1HashTablePower;
  l1LockPower = _l1LockPower;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setSizeControllerThrottlerConfig(
    util::Throttler::Config config) {
  sizeControllerThrottlerConfig = config;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::enableFragmentationTracking() {
  fragmentationTrackingEnabled = true;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setEventTracker(
    EventTrackerSharedPtr&& ptr) {
  eventTracker = std::move(ptr);
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setItemDestructor(
    ItemDestructor destructor) {
  itemDestructor = std::move(destructor);
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setRemoveCb(RemoveCb cb) {
  removeCb = std::move(cb);
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::enablePersistence(
    uint32_t threadCount,
    const std::string& basefilePath,
    SerializeCb serializeCallback,
    DeserializeCb deserializeCallback) {
  if (persistenceEnabled) {
    throw std::invalid_argument("cache persistence is already enabled");
  }

  if (threadCount == 0) {
    throw std::invalid_argument(
        "A non-zero thread count must be set to enable cache persistence");
  }

  if (basefilePath.empty()) {
    throw std::invalid_argument(
        "A valid file path must be providede to enable cache persistence");
  }

  if (!serializeCallback || !deserializeCallback) {
    throw std::invalid_argument(
        "Serialize and deserialize callback must be set to enable cache "
        "persistence");
  }
  persistenceEnabled = true;
  persistThreadCount = threadCount;
  persistBaseFilePath = basefilePath;
  serializeCb = std::move(serializeCallback);
  deserializeCb = std::move(deserializeCallback);
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::enablePersistenceWithEvictionOrder(
    const std::string& basefilePath,
    SerializeCb serializeCallback,
    DeserializeCb deserializeCallback) {
  if (persistenceEnabled) {
    throw std::invalid_argument("cache persistence is already enabled");
  }

  if (basefilePath.empty()) {
    throw std::invalid_argument(
        "A valid file path must be providede to enable cache persistence");
  }

  if (!serializeCallback || !deserializeCallback) {
    throw std::invalid_argument(
        "Serialize and deserialize callback must be set to enable cache "
        "persistence");
  }
  persistenceEnabled = true;
  persistThreadCount = 1;
  persistBaseFilePath = basefilePath;
  serializeCb = std::move(serializeCallback);
  deserializeCb = std::move(deserializeCallback);
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setItemReaperInterval(
    std::chrono::milliseconds _reaperInterval) {
  reaperInterval = _reaperInterval;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setEvictionPolicyConfig(
    EvictionPolicyConfig _evictionPolicyConfig) {
  evictionPolicyConfig = _evictionPolicyConfig;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setEvictionSearchLimit(
    uint32_t _evictionSearchLimit) {
  evictionSearchLimit = _evictionSearchLimit;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::setDelayCacheWorkersStart() {
  delayCacheWorkersStart = true;
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::enableNvm(NvmCacheConfig config) {
  nvmConfig = std::move(config);
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::overrideNvmCbs(ToBlobCb blobCb,
                                                           ToPtrCb ptrCb) {
  if (!nvmConfig || nvmConfig->makeBlobCb || nvmConfig->makeObjCb) {
    throw std::invalid_argument(
        "Do not set makeBlobCb or makeObjCb in nvmConfig before calling "
        "overriceNvmCbs.");
  }

  nvmConfig->makeBlobCb =
      [blobCb = std::move(blobCb)](
          const typename T::CacheItem& item,
          folly::Range<typename T::NvmCache::ChainedItemIter>) {
        uintptr_t ptr =
            item.template getMemoryAs<typename T::Item>()->objectPtr;
        auto blob = blobCb(ptr);
        std::vector<BufferedBlob> blobs;
        if (blob == nullptr) {
          return blobs;
        }
        blobs.emplace_back(BufferedBlob{static_cast<uint32_t>(item.getSize()),
                                        std::move(blob)});
        return blobs;
      };

  nvmConfig->makeObjCb =
      [ptrCb = std::move(ptrCb)](
          const NvmItem& nvmItem, typename T::CacheItem& it,
          folly::Range<typename T::NvmCache::WritableChainedItemIter>) {
        // Create ptr. Do not consider chained item.
        auto pBlob = nvmItem.getBlob(0);
        uintptr_t ptr = ptrCb(pBlob.data);
        // TODO: Is there a better way to do this?
        if (ptr == reinterpret_cast<uintptr_t>(nullptr)) {
          return false;
        }
        *it.template getMemoryAs<typename T::Item>() =
            typename T::Item{ptr, pBlob.data.size()};

        return true;
      };
  return *this;
}

template <typename T>
ObjectCacheConfig<T>& ObjectCacheConfig<T>::enablePoolProvisioning() {
  provisionPool = true;
  return *this;
}

template <typename T>
const ObjectCacheConfig<T>& ObjectCacheConfig<T>::validate() const {
  // checking missing params
  if (cacheName.empty()) {
    throw std::invalid_argument("cache name is not provided");
  }

  if (!l1EntriesLimit) {
    throw std::invalid_argument("l1EntriesLimit is not provided");
  }

  if ((!itemDestructor && !removeCb) || (itemDestructor && removeCb)) {
    throw std::invalid_argument(
        "Only one of ItemDestructor or RemoveCb can be set. Not both, nor "
        "neither.");
  }

  if (objectSizeTrackingEnabled) {
    if ((sizeControllerIntervalMs && !totalObjectSizeLimit) ||
        (!sizeControllerIntervalMs && totalObjectSizeLimit)) {
      throw std::invalid_argument(
          "Only one of sizeControllerIntervalMs and totalObjectSizeLimit is "
          "set");
    }
  }

  if (fragmentationTrackingEnabled && !objectSizeTrackingEnabled) {
    throw std::invalid_argument(
        "Object size tracking has to be enabled to have fragmentation "
        "tracking");
  }

  if (objectSizeDistributionTrackingEnabled && !objectSizeTrackingEnabled) {
    throw std::invalid_argument(
        "Object size tracking has to be enabled to track object size "
        "distribution");
  }

  return *this;
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
