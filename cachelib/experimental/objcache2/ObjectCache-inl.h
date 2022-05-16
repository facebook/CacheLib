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

namespace facebook {
namespace cachelib {
namespace objcache2 {

template <typename CacheTrait>
template <typename T>
void ObjectCache<CacheTrait>::init(ObjectCacheConfig config) {
  auto cacheSize = config.l1CacheSize;
  if (cacheSize == 0) {
    // compute the approximate cache size using the l1EntriesLimit and
    // kL1AllocSize
    XCHECK(config.l1EntriesLimit);
    const size_t l1SizeRequired = config.l1EntriesLimit * kL1AllocSize;
    const size_t l1SizeRequiredSlabGranularity =
        (l1SizeRequired / Slab::kSize + (l1SizeRequired % Slab::kSize != 0)) *
        Slab::kSize;
    cacheSize = l1SizeRequiredSlabGranularity + Slab::kSize;
  }

  typename CacheTrait::Config l1Config;
  l1Config.setCacheName(config.cacheName)
      .setCacheSize(cacheSize)
      .setAccessConfig({config.l1HashTablePower, config.l1LockPower})
      .setDefaultAllocSizes({kL1AllocSize});
  l1Config.setItemDestructor([this](typename CacheTrait::DestructorData ctx) {
    if (ctx.context == DestructorContext::kEvictedFromRAM) {
      evictions_.inc();
    }

    auto& item = ctx.item;
    auto ptr = *reinterpret_cast<T**>(item.getMemory());

    SCOPE_EXIT {
      // Explicitly invoke destructor because cachelib does not
      // free memory and neither does it call destructor by default.
      // TODO: support user-defined destructor, currently we can only support
      // identical object type.
      delete ptr;
    };
  });

  this->l1Cache_ = std::make_unique<CacheTrait>(l1Config);
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
      ((perPoolSize / kL1AllocSize) - (config.l1EntriesLimit / l1NumShards_)) *
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

template <typename CacheTrait>
template <typename T>
std::unique_ptr<ObjectCache<CacheTrait>> ObjectCache<CacheTrait>::create(
    ObjectCacheConfig config) {
  auto obj = std::make_unique<ObjectCache>(InternalConstructor(), config);
  obj->template init<T>(std::move(config));
  return obj;
}

template <typename CacheTrait>
template <typename T>
std::shared_ptr<const T> ObjectCache<CacheTrait>::find(folly::StringPiece key) {
  lookups_.inc();
  auto found = this->l1Cache_->find(key);
  if (!found) {
    return nullptr;
  }
  succL1Lookups_.inc();

  auto ptrPtr = found->template getMemoryAs<const T*>();
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](const T*) {};
  return std::shared_ptr<const T>(*ptrPtr, std::move(deleter));
}

template <typename CacheTrait>
template <typename T>
std::shared_ptr<T> ObjectCache<CacheTrait>::findToWrite(
    folly::StringPiece key) {
  lookups_.inc();
  auto found = this->l1Cache_->findToWrite(key);
  if (!found) {
    return nullptr;
  }
  succL1Lookups_.inc();

  auto ptrPtr = found->template getMemoryAs<T*>();
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](T*) {};
  return std::shared_ptr<T>(*ptrPtr, std::move(deleter));
}

template <typename CacheTrait>
template <typename T>
std::pair<typename ObjectCache<CacheTrait>::AllocStatus, std::shared_ptr<T>>
ObjectCache<CacheTrait>::insertOrReplace(folly::StringPiece key,
                                         std::unique_ptr<T> object,
                                         uint32_t ttlSecs,
                                         std::shared_ptr<T>* replacedPtr) {
  inserts_.inc();

  auto handle =
      allocateFromL1<T>(key, ttlSecs, 0 /* use current time as creationTime */);
  if (!handle) {
    insertErrors_.inc();
    return {AllocStatus::kAllocError, std::shared_ptr<T>(std::move(object))};
  }
  T* ptr = object.release();
  *handle->template getMemoryAs<T*>() = ptr;

  auto replaced = this->l1Cache_->insertOrReplace(handle);

  if (replaced) {
    replaces_.inc();
    if (replacedPtr) {
      auto ptrPtr = reinterpret_cast<T**>(replaced->getMemory());
      // Just release the handle. Cache destorys object when all handles
      // released.
      auto deleter = [h = std::move(replaced)](T*) {};
      *replacedPtr = std::shared_ptr<T>(*ptrPtr, std::move(deleter));
    }
  }

  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(handle)](T*) {};
  return {AllocStatus::kSuccess, std::shared_ptr<T>(ptr, std::move(deleter))};
}

template <typename CacheTrait>
template <typename T>
std::pair<typename ObjectCache<CacheTrait>::AllocStatus, std::shared_ptr<T>>
ObjectCache<CacheTrait>::insert(folly::StringPiece key,
                                std::unique_ptr<T> object,
                                uint32_t ttlSecs) {
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

template <typename CacheTrait>
template <typename T>
typename CacheTrait::WriteHandle ObjectCache<CacheTrait>::allocateFromL1(
    folly::StringPiece key, uint32_t ttl, uint32_t creationTime) {
  PoolId poolId = 0;
  if (l1NumShards_ > 1) {
    auto hash = cachelib::MurmurHash2{}(key.data(), key.size());
    poolId = static_cast<PoolId>(hash % l1NumShards_);
  }
  return this->l1Cache_->allocate(poolId, key, sizeof(T*), ttl, creationTime);
}

template <typename CacheTrait>
ObjectCache<CacheTrait>::~ObjectCache() {
  for (auto itr = this->l1Cache_->begin(); itr != this->l1Cache_->end();
       ++itr) {
    this->l1Cache_->remove(itr.asHandle());
  }
}

template <typename CacheTrait>
void ObjectCache<CacheTrait>::remove(folly::StringPiece key) {
  removes_.inc();
  this->l1Cache_->remove(key);
}

template <typename CacheTrait>
void ObjectCache<CacheTrait>::getObjectCacheCounters(
    std::function<void(folly::StringPiece, uint64_t)> visitor) const {
  visitor("objcache.lookups", lookups_.get());
  visitor("objcache.lookups.l1_hits", succL1Lookups_.get());
  visitor("objcache.inserts", inserts_.get());
  visitor("objcache.inserts.errors", insertErrors_.get());
  visitor("objcache.replaces", replaces_.get());
  visitor("objcache.removes", removes_.get());
  visitor("objcache.evictions", evictions_.get());
}

template <typename CacheTrait>
std::map<std::string, std::string>
ObjectCache<CacheTrait>::serializeConfigParams() const {
  auto config = this->l1Cache_->serializeConfigParams();
  config["l1EntriesLimit"] = std::to_string(l1EntriesLimit_);
  return config;
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
