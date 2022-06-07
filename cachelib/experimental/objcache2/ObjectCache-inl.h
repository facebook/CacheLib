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
template <typename T>
void ObjectCache<AllocatorT>::init(ObjectCacheConfig config) {
  // compute the approximate cache size using the l1EntriesLimit and
  // l1AllocSize
  XCHECK(config.l1EntriesLimit);
  auto l1AllocSize = getL1AllocSize<T>(config.maxKeySizeBytes);
  const size_t l1SizeRequired = config.l1EntriesLimit * l1AllocSize;
  const size_t l1SizeRequiredSlabGranularity =
      (l1SizeRequired / Slab::kSize + (l1SizeRequired % Slab::kSize != 0)) *
      Slab::kSize;
  auto cacheSize = l1SizeRequiredSlabGranularity + Slab::kSize;

  typename AllocatorT::Config l1Config;
  l1Config.setCacheName(config.cacheName)
      .setCacheSize(cacheSize)
      .setAccessConfig({config.l1HashTablePower, config.l1LockPower})
      .setDefaultAllocSizes({l1AllocSize});
  l1Config.setItemDestructor([this](typename AllocatorT::DestructorData ctx) {
    if (ctx.context == DestructorContext::kEvictedFromRAM) {
      evictions_.inc();
    }

    auto& item = ctx.item;

    auto itemPtr = reinterpret_cast<ObjectCacheItem<T>*>(item.getMemory());

    SCOPE_EXIT {
      if (objectSizeTrackingEnabled_) {
        // update total object size
        totalObjectSizeBytes_.fetch_sub(itemPtr->objectSize,
                                        std::memory_order_relaxed);
      }
      // Explicitly invoke destructor because cachelib does not
      // free memory and neither does it call destructor by default.
      // TODO: support user-defined destructor, currently we can only support
      // identical object type.
      delete itemPtr->objectPtr;
    };
  });

  this->l1Cache_ = std::make_unique<AllocatorT>(l1Config);
  size_t perPoolSize =
      this->l1Cache_->getCacheMemoryStats().cacheSize / l1NumShards_;
  // pool size can't be smaller than slab size
  perPoolSize = std::max(perPoolSize, Slab::kSize);
  // num of pool need to be modified properly as well
  l1NumShards_ =
      std::min(l1NumShards_,
               this->l1Cache_->getCacheMemoryStats().cacheSize / perPoolSize);
  for (size_t i = 0; i < l1NumShards_; i++) {
    this->l1Cache_->addPool(fmt::format("pool_{}", i), perPoolSize);
  }

  // the placeholder is used to make sure each pool
  // won't store objects more than l1EntriesLimit / l1NumShards
  const size_t l1PlaceHolders =
      ((perPoolSize / l1AllocSize) - (config.l1EntriesLimit / l1NumShards_)) *
      l1NumShards_;
  for (size_t i = 0; i < l1PlaceHolders; i++) {
    // Allocate placeholder items such that the cache will fit exactly
    // "l1EntriesLimit" objects
    auto key = fmt::format("_cl_ph_{}", i);
    auto hdl = allocateFromL1<T>(key, 0 /* no ttl */,
                                     0 /* use current time as creationTime
                                     */);
    if (!hdl) {
      throw std::runtime_error(fmt::format("Couldn't allocate {}", key));
    }
    placeholders_.push_back(std::move(hdl));
  }
}

template <typename AllocatorT>
template <typename T>
std::unique_ptr<ObjectCache<AllocatorT>> ObjectCache<AllocatorT>::create(
    ObjectCacheConfig config) {
  auto obj = std::make_unique<ObjectCache>(InternalConstructor(), config);
  obj->template init<T>(std::move(config));
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

  auto ptr = found->template getMemoryAs<ObjectCacheItem<const T>>()->objectPtr;
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](const T*) {};
  return std::shared_ptr<const T>(ptr, std::move(deleter));
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

  auto ptr = found->template getMemoryAs<ObjectCacheItem<T>>()->objectPtr;
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](T*) {};
  return std::shared_ptr<T>(ptr, std::move(deleter));
}

template <typename AllocatorT>
template <typename T>
std::pair<typename ObjectCache<AllocatorT>::AllocStatus, std::shared_ptr<T>>
ObjectCache<AllocatorT>::insertOrReplace(folly::StringPiece key,
                                         std::unique_ptr<T> object,
                                         size_t objectSize,
                                         uint32_t ttlSecs,
                                         std::shared_ptr<T>* replacedPtr) {
  if (objectSizeTrackingEnabled_ && objectSize == 0) {
    throw std::invalid_argument(
        "Object size tracking is enabled but object size is set to be 0.");
  }

  inserts_.inc();

  auto handle =
      allocateFromL1<T>(key, ttlSecs, 0 /* use current time as creationTime */);
  if (!handle) {
    insertErrors_.inc();
    return {AllocStatus::kAllocError, std::shared_ptr<T>(std::move(object))};
  }
  // We don't release the object here because insertOrReplace could throw when
  // the replaced item is out of refcount; in this case, the object isn't
  // inserted to the cache and releasing the object will cause memory leak.
  T* ptr = object.get();
  *handle->template getMemoryAs<ObjectCacheItem<T>>() =
      ObjectCacheItem<T>{ptr, objectSize};

  auto replaced = this->l1Cache_->insertOrReplace(handle);

  if (replaced) {
    replaces_.inc();
    if (replacedPtr) {
      auto itemPtr =
          reinterpret_cast<ObjectCacheItem<T>*>(replaced->getMemory());
      // Just release the handle. Cache destorys object when all handles
      // released.
      auto deleter = [h = std::move(replaced)](T*) {};
      *replacedPtr = std::shared_ptr<T>(itemPtr->objectPtr, std::move(deleter));
    }
  }

  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(handle)](T*) {};

  // update total object size
  if (objectSizeTrackingEnabled_) {
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
  if (objectSizeTrackingEnabled_ && objectSize == 0) {
    throw std::invalid_argument(
        "Object size tracking is enabled but object size is set to be 0.");
  }

  inserts_.inc();

  auto handle =
      allocateFromL1<T>(key, ttlSecs, 0 /* use current time as creationTime */);
  if (!handle) {
    insertErrors_.inc();
    return {AllocStatus::kAllocError, std::shared_ptr<T>(std::move(object))};
  }
  T* ptr = object.get();
  *handle->template getMemoryAs<T*>() = ptr;

  auto success = this->l1Cache_->insert(handle);
  if (success) {
    // update total object size
    if (objectSizeTrackingEnabled_) {
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
template <typename T>
typename AllocatorT::WriteHandle ObjectCache<AllocatorT>::allocateFromL1(
    folly::StringPiece key, uint32_t ttl, uint32_t creationTime) {
  PoolId poolId = 0;
  if (l1NumShards_ > 1) {
    auto hash = cachelib::MurmurHash2{}(key.data(), key.size());
    poolId = static_cast<PoolId>(hash % l1NumShards_);
  }
  return this->l1Cache_->allocate(poolId, key, sizeof(ObjectCacheItem<T>), ttl,
                                  creationTime);
}

template <typename AllocatorT>
template <typename T>
uint32_t ObjectCache<AllocatorT>::getL1AllocSize(uint8_t maxKeySizeBytes) {
  auto requiredSizeBytes = maxKeySizeBytes + sizeof(ObjectCacheItem<T>) +
                           sizeof(typename AllocatorT::Item);
  if (requiredSizeBytes <= kL1AllocSizeMin) {
    return kL1AllocSizeMin;
  }
  return util::getAlignedSize(static_cast<uint32_t>(requiredSizeBytes),
                              8 /* alloc size must be aligned to 8 bytes */);
}

template <typename AllocatorT>
ObjectCache<AllocatorT>::~ObjectCache() {
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
  config["l1EntriesLimit"] = std::to_string(l1EntriesLimit_);
  return config;
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
