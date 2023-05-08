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

namespace facebook {
namespace cachelib {
namespace objcache2 {

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
      .setEventTracker(std::move(config_.eventTracker))
      .setEvictionSearchLimit(config_.evictionSearchLimit)
      .setItemDestructor([this](typename AllocatorT::DestructorData data) {
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

  this->l1Cache_ = std::make_unique<AllocatorT>(l1Config);
  // add a pool per shard
  for (size_t i = 0; i < config_.l1NumShards; i++) {
    std::string shardName =
        config_.l1ShardName.empty() ? "pool" : config_.l1ShardName;
    // Add the shard index as the suffix of pool name if needed
    if (config_.l1NumShards > 1) {
      shardName += folly::sformat("_{}", i);
    }
    this->l1Cache_->addPool(shardName, perPoolSize, {} /* allocSizes */,
                            config_.evictionPolicyConfig);
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

  if (config_.objectSizeTrackingEnabled &&
      config_.sizeControllerIntervalMs != 0) {
    startSizeController(
        std::chrono::milliseconds{config_.sizeControllerIntervalMs},
        config_.sizeControllerThrottlerConfig);
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
    throw std::invalid_argument(
        "Object size tracking is not enabled but object size is set. Are you "
        "trying to set TTL?");
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

  // update total object size
  if (config_.objectSizeTrackingEnabled) {
    totalObjectSizeBytes_.fetch_add(objectSize, std::memory_order_relaxed);
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

  // Use custom deleter
  auto deleter = Deleter<T>(std::move(handle));
  return {success ? AllocStatus::kSuccess : AllocStatus::kKeyAlreadyExists,
          std::shared_ptr<T>(ptr, std::move(deleter))};
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
  stopSizeController();

  for (auto itr = this->l1Cache_->begin(); itr != this->l1Cache_->end();
       ++itr) {
    this->l1Cache_->remove(itr.asHandle());
  }
}

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::remove(folly::StringPiece key) {
  removes_.inc();
  return this->l1Cache_->remove(key) == AllocatorT::RemoveRes::kSuccess;
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
}

template <typename AllocatorT>
std::map<std::string, std::string>
ObjectCache<AllocatorT>::serializeConfigParams() const {
  auto config = this->l1Cache_->serializeConfigParams();
  config["l1EntriesLimit"] = std::to_string(config_.l1EntriesLimit);
  config["l1NumShards"] = std::to_string(config_.l1NumShards);
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

template <typename AllocatorT>
bool ObjectCache<AllocatorT>::persist() {
  if (config_.persistBaseFilePath.empty() || !config_.serializeCb) {
    return false;
  }

  // Stop all the other workers before persist
  if (!stopSizeController()) {
    return false;
  }

  if (!this->l1Cache_->stopWorkers()) {
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
                                           std::function<void()> mutateCb,
                                           const std::string& mutateCtx) {
  if (!object) {
    return;
  }

  cachelib::objcache2::ThreadMemoryTracker tMemTracker;
  size_t memUsageBefore = tMemTracker.getMemUsageBytes();
  mutateCb();
  size_t memUsageAfter = tMemTracker.getMemUsageBytes();

  auto& hdl = getWriteHandleRefInternal<T>(object);
  size_t memUsageDiff = 0;
  size_t oldObjectSize = 0;
  if (memUsageAfter > memUsageBefore) { // updated to a larger value
    memUsageDiff = memUsageAfter - memUsageBefore;
    // do atomic update on objectSize
    oldObjectSize = __sync_fetch_and_add(
        &(reinterpret_cast<ObjectCacheItem*>(hdl->getMemory())->objectSize),
        memUsageDiff);
    totalObjectSizeBytes_.fetch_add(memUsageDiff, std::memory_order_relaxed);
  } else if (memUsageAfter < memUsageBefore) { // updated to a smaller value
    memUsageDiff = memUsageBefore - memUsageAfter;
    // do atomic update on objectSize
    oldObjectSize = __sync_fetch_and_sub(
        &(reinterpret_cast<ObjectCacheItem*>(hdl->getMemory())->objectSize),
        memUsageDiff);
    totalObjectSizeBytes_.fetch_sub(memUsageDiff, std::memory_order_relaxed);
  }

  // TODO T149177357: for debugging purpose, remove the log later
  XLOGF_EVERY_MS(
      INFO, 60'000,
      "[Object-Cache mutate][{}] type: {}, memUsageBefore: {}, memUsageAfter: "
      "{}, memUsageDiff:{}, oldObjectSize: {}, curObjectSize: {}, "
      "curTotalObjectSize: {}",
      mutateCtx, typeid(T).name(), memUsageBefore, memUsageAfter, memUsageDiff,
      oldObjectSize, getObjectSize(object), getTotalObjectSize());
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook
