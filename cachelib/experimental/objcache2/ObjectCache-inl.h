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

template <typename T>
void ObjectCache::init(ObjectCacheConfig config) {
  auto cacheSize = config.l1CacheSize;
  if (cacheSize == 0) {
    // compute the approximate cache size using the l1EntriesLimit and
    // l1AllocSize
    XCHECK(config.l1EntriesLimit);
    const size_t l1SizeRequired = config.l1EntriesLimit * config.l1AllocSize;
    const size_t l1SizeRequiredSlabGranularity =
        (l1SizeRequired / Slab::kSize + (l1SizeRequired % Slab::kSize != 0)) *
        Slab::kSize;
    cacheSize = l1SizeRequiredSlabGranularity + Slab::kSize;
  }

  LruAllocator::Config l1Config;
  l1Config.setCacheName(config.cacheName)
      .setCacheSize(cacheSize)
      .setAccessConfig({config.l1HashTablePower, config.l1LockPower})
      .setDefaultAllocSizes({config.l1AllocSize});
  l1Config.setItemDestructor([this](LruAllocator::DestructorData ctx) {
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

  l1Cache_ = std::make_unique<LruAllocator>(l1Config);
  size_t perPoolSize = l1Cache_->getCacheMemoryStats().cacheSize / l1NumShards_;
  for (size_t i = 0; i < l1NumShards_; i++) {
    l1Cache_->addPool(fmt::format("pool_{}", i), perPoolSize);
  }

  if (!config.placeHolderDisabled) {
    // the placeholder is used to make sure each pool
    // won't store objects more than l1EntriesLimit / l1NumShards
    const size_t l1PlaceHolders = ((perPoolSize / config.l1AllocSize) -
                                   (config.l1EntriesLimit / l1NumShards_)) *
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
}

template <typename T>
std::unique_ptr<ObjectCache> ObjectCache::create(ObjectCacheConfig config) {
  auto obj = std::make_unique<ObjectCache>(InternalConstructor(), config);
  obj->init<T>(std::move(config));
  return obj;
}

template <typename T>
std::shared_ptr<const T> ObjectCache::find(folly::StringPiece key) {
  lookups_.inc();
  auto found = l1Cache_->find(key);
  if (!found) {
    return nullptr;
  }
  succL1Lookups_.inc();

  auto ptrPtr = found->template getMemoryAs<const T*>();
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](const T*) {};
  return std::shared_ptr<const T>(*ptrPtr, std::move(deleter));
}

template <typename T>
std::shared_ptr<T> ObjectCache::findToWrite(folly::StringPiece key) {
  lookups_.inc();
  auto found = l1Cache_->findToWrite(key);
  if (!found) {
    return nullptr;
  }
  succL1Lookups_.inc();

  auto ptrPtr = found->template getMemoryAs<T*>();
  // Just release the handle. Cache destorys object when all handles released.
  auto deleter = [h = std::move(found)](T*) {};
  return std::shared_ptr<T>(*ptrPtr, std::move(deleter));
}

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
std::pair<bool, std::shared_ptr<T>> ObjectCache::insertOrReplace(
    folly::StringPiece key,
    std::unique_ptr<T> object,
    uint32_t ttlSecs,
    std::shared_ptr<T>* replacedPtr) {
  inserts_.inc();

  auto handle =
      allocateFromL1<T>(key, ttlSecs, 0 /* use current time as creationTime */);
  if (!handle) {
    insertErrors_.inc();
    return {false, std::shared_ptr<T>(std::move(object))};
  }
  T* ptr = object.release();
  *handle->template getMemoryAs<T*>() = ptr;

  auto replaced = l1Cache_->insertOrReplace(handle);

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
  return {true, std::shared_ptr<T>(ptr, std::move(deleter))};
}

template <typename T>
LruAllocator::WriteHandle ObjectCache::allocateFromL1(folly::StringPiece key,
                                                      uint32_t ttl,
                                                      uint32_t creationTime) {
  PoolId poolId = 0;
  if (l1NumShards_ > 1) {
    auto hash = cachelib::MurmurHash2{}(key.data(), key.size());
    poolId = static_cast<PoolId>(hash % l1NumShards_);
  }
  return l1Cache_->allocate(poolId, key, sizeof(T*), ttl, creationTime);
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
