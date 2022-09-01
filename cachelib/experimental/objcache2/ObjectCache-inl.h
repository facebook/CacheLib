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

#include <stdexcept>
namespace facebook {
namespace cachelib {
namespace objcache2 {

template <typename AllocatorT>
void ObjectCache<AllocatorT>::init() {
  // compute the approximate cache size using the l1EntriesLimit and
  // l1AllocSize
  XCHECK(config_.l1EntriesLimit);
  auto l1AllocSize = getL1AllocSize(config_.maxKeySizeBytes);
  const size_t l1SizeRequired = config_.l1EntriesLimit * l1AllocSize;
  const size_t l1SizeRequiredSlabGranularity =
      (l1SizeRequired / Slab::kSize + (l1SizeRequired % Slab::kSize != 0)) *
      Slab::kSize;
  auto cacheSize = l1SizeRequiredSlabGranularity + Slab::kSize;

  typename AllocatorT::Config l1Config;
  l1Config.setCacheName(config_.cacheName)
      .setCacheSize(cacheSize)
      .setAccessConfig({config_.l1HashTablePower, config_.l1LockPower})
      .setDefaultAllocSizes({l1AllocSize});

  // TODO T121696070: Add validate() API in ObjectCacheConfig
  XCHECK(config_.itemDestructor);
  l1Config.setItemDestructor([this](typename AllocatorT::DestructorData ctx) {
    if (ctx.context == DestructorContext::kEvictedFromRAM) {
      evictions_.inc();
    }

    auto& item = ctx.item;

    auto itemPtr = reinterpret_cast<ObjectCacheItem*>(item.getMemory());

    SCOPE_EXIT {
      if (config_.objectSizeTrackingEnabled) {
        // update total object size
        totalObjectSizeBytes_.fetch_sub(itemPtr->objectSize,
                                        std::memory_order_relaxed);
      }
      // execute user defined item destructor
      config_.itemDestructor(
          ObjectCacheDestructorData(itemPtr->objectPtr, item.getKey()));
    };
  });
  l1Config.setEventTracker(std::move(config_.eventTracker));

  this->l1Cache_ = std::make_unique<AllocatorT>(l1Config);
  size_t perPoolSize =
      this->l1Cache_->getCacheMemoryStats().cacheSize / config_.l1NumShards;
  // pool size can't be smaller than slab size
  perPoolSize = std::max(perPoolSize, Slab::kSize);
  // num of pool need to be modified properly as well
  l1NumShards_ =
      std::min(config_.l1NumShards,
               this->l1Cache_->getCacheMemoryStats().cacheSize / perPoolSize);
  for (size_t i = 0; i < l1NumShards_; i++) {
    this->l1Cache_->addPool(fmt::format("pool_{}", i), perPoolSize);
  }

  // the placeholder is used to make sure each pool
  // won't store objects more than l1EntriesLimit / l1NumShards
  const size_t l1PlaceHolders =
      ((perPoolSize / l1AllocSize) - (config_.l1EntriesLimit / l1NumShards_)) *
      l1NumShards_;
  for (size_t i = 0; i < l1PlaceHolders; i++) {
    // Allocate placeholder items such that the cache will fit exactly
    // "l1EntriesLimit" objects
    auto key = getPlaceHolderKey(i);
    bool success = allocatePlaceholder(key);
    if (!success) {
      throw std::runtime_error(fmt::format("Couldn't allocate {}", key));
    }
  }

  if (config_.objectSizeTrackingEnabled &&
      config_.sizeControllerIntervalMs != 0) {
    startSizeController(
        std::chrono::milliseconds{config_.sizeControllerIntervalMs},
        config_.sizeControllerThrottlerConfig);
  }
}

template <typename AllocatorT>
std::unique_ptr<ObjectCache<AllocatorT>> ObjectCache<AllocatorT>::create(
    ObjectCacheConfig config) {
  auto obj =
      std::make_unique<ObjectCache>(InternalConstructor(), std::move(config));
  obj->init();
  return obj;
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
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](const T*) {};
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
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](T*) {};
  return std::shared_ptr<T>(reinterpret_cast<T*>(ptr), std::move(deleter));
}

template <typename AllocatorT>
template <typename T>
std::pair<typename ObjectCache<AllocatorT>::AllocStatus, std::shared_ptr<T>>
ObjectCache<AllocatorT>::insertOrReplace(folly::StringPiece key,
                                         std::unique_ptr<T> object,
                                         size_t objectSize,
                                         uint32_t ttlSecs,
                                         std::shared_ptr<T>* replacedPtr) {
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
  // We don't release the object here because insertOrReplace could throw when
  // the replaced item is out of refcount; in this case, the object isn't
  // inserted to the cache and releasing the object will cause memory leak.
  T* ptr = object.get();
  *handle->template getMemoryAs<ObjectCacheItem>() =
      ObjectCacheItem{reinterpret_cast<uintptr_t>(ptr), objectSize};

  auto replaced = this->l1Cache_->insertOrReplace(handle);

  if (replaced) {
    replaces_.inc();
    if (replacedPtr) {
      auto itemPtr = reinterpret_cast<ObjectCacheItem*>(replaced->getMemory());
      // Just release the handle. Cache destorys object when all handles
      // released.
      auto deleter = [h = std::move(replaced)](T*) {};
      *replacedPtr = std::shared_ptr<T>(
          reinterpret_cast<T*>(itemPtr->objectPtr), std::move(deleter));
    }
  }

  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(handle)](T*) {};

  // update total object size
  if (config_.objectSizeTrackingEnabled) {
    totalObjectSizeBytes_.fetch_add(objectSize, std::memory_order_relaxed);
  }

  // Release the object as it has been successfully inserted to the cache.
  object.release();
  return {AllocStatus::kSuccess, std::shared_ptr<T>(ptr, std::move(deleter))};
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
  if (success) {
    // update total object size
    if (config_.objectSizeTrackingEnabled) {
      totalObjectSizeBytes_.fetch_add(objectSize, std::memory_order_relaxed);
    }
    // Release the handle now since we have inserted the handle into the cache,
    // and from now the Cache will be responsible for destroying the object
    // when it's evicted/removed.
    object.release();
  }

  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(handle)](T*) {};
  return {success ? AllocStatus::kSuccess : AllocStatus::kKeyAlreadyExists,
          std::shared_ptr<T>(ptr, std::move(deleter))};
}

template <typename AllocatorT>
typename AllocatorT::WriteHandle ObjectCache<AllocatorT>::allocateFromL1(
    folly::StringPiece key, uint32_t ttl, uint32_t creationTime) {
  PoolId poolId = 0;
  if (l1NumShards_ > 1) {
    auto hash = cachelib::MurmurHash2{}(key.data(), key.size());
    poolId = static_cast<PoolId>(hash % l1NumShards_);
  }
  return this->l1Cache_->allocate(poolId, key, sizeof(ObjectCacheItem), ttl,
                                  creationTime);
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::allocatePlaceholder(std::string key) {
  auto hdl = allocateFromL1(key, 0 /* no ttl */,
                                     0 /* use current time as creationTime
                                     */);
  if (!hdl) {
    return false;
  }
  placeholders_.push_back(std::move(hdl));
  return true;
}

template <typename AllocatorT>
uint32_t ObjectCache<AllocatorT>::getL1AllocSize(uint8_t maxKeySizeBytes) {
  auto requiredSizeBytes = maxKeySizeBytes + sizeof(ObjectCacheItem) +
                           sizeof(typename AllocatorT::Item);
  if (requiredSizeBytes <= kL1AllocSizeMin) {
    return kL1AllocSizeMin;
  }
  return util::getAlignedSize(static_cast<uint32_t>(requiredSizeBytes),
                              8 /* alloc size must be aligned to 8 bytes */);
}

template <typename AllocatorT>
ObjectCache<AllocatorT>::~ObjectCache() {
  if (config_.objectSizeTrackingEnabled) {
    stopSizeController();
  }

  for (auto itr = this->l1Cache_->begin(); itr != this->l1Cache_->end();
       ++itr) {
    this->l1Cache_->remove(itr.asHandle());
  }
}

template <typename AllocatorT>
void ObjectCache<AllocatorT>::remove(folly::StringPiece key) {
  removes_.inc();
  this->l1Cache_->remove(key);
}

template <typename AllocatorT>
void ObjectCache<AllocatorT>::getObjectCacheCounters(
    std::function<void(folly::StringPiece, uint64_t)> visitor) const {
  visitor("objcache.lookups", lookups_.get());
  visitor("objcache.lookups.l1_hits", succL1Lookups_.get());
  visitor("objcache.inserts", inserts_.get());
  visitor("objcache.inserts.errors", insertErrors_.get());
  visitor("objcache.replaces", replaces_.get());
  visitor("objcache.removes", removes_.get());
  visitor("objcache.evictions", evictions_.get());
  visitor("objcache.object_size_bytes", getTotalObjectSize());
}

template <typename AllocatorT>
std::map<std::string, std::string>
ObjectCache<AllocatorT>::serializeConfigParams() const {
  auto config = this->l1Cache_->serializeConfigParams();
  config["l1EntriesLimit"] = std::to_string(config_.l1EntriesLimit);
  if (config_.objectSizeTrackingEnabled &&
      config_.sizeControllerIntervalMs > 0) {
    config["l1CacheSizeLimit"] = std::to_string(config_.cacheSizeLimit);
    config["sizeControllerIntervalMs"] =
        std::to_string(config_.sizeControllerIntervalMs);
  }
  return config;
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::startSizeController(
    std::chrono::milliseconds interval, const util::Throttler::Config& config) {
  if (!stopSizeController()) {
    XLOG(ERR) << "Size controller is already running. Cannot start it again.";
    return false;
  }

  sizeController_ =
      std::make_unique<ObjectCacheSizeController<AllocatorT>>(*this, config);
  bool ret = sizeController_->start(interval, "ObjectCache-SizeController");
  if (ret) {
    XLOG(DBG) << "Started ObjectCache SizeController";
  } else {
    XLOGF(
        ERR,
        "Couldn't start ObjectCache SizeController, interval: {} milliseconds",
        interval.count());
  }
  return ret;
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::stopSizeController(std::chrono::seconds timeout) {
  if (!sizeController_) {
    return true;
  }

  bool ret = sizeController_->stop(timeout);
  if (ret) {
    XLOG(DBG) << "Stopped ObjectCache SizeController";
  } else {
    XLOGF(ERR, "Couldn't stop ObjectCache SizeController, timeout: {} seconds",
          timeout.count());
  }
  sizeController_.reset();
  return ret;
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
