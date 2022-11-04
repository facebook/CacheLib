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
#include <scoped_allocator>

#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/Persistence.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "cachelib/experimental/objcache/gen-cpp2/ObjectCachePersistence_types.h"
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {
namespace detail {
// Accessor to expose unmarkNascent() from a item handle
template <typename ItemHandle2>
void objcacheUnmarkNascent(const ItemHandle2& hdl) {
  hdl.unmarkNascent();
}
} // namespace detail

namespace objcache {
// Figure out how many bytes we need at the minimum for an object in cache
template <typename T>
inline uint32_t getTypeSize() {
  return static_cast<uint32_t>(sizeof(T));
}

// Convert an item to a type, starting from after the allocator's metadata
template <typename T, typename AllocatorResource>
inline T* getType(void* mem) {
  return reinterpret_cast<T*>(
      AllocatorResource::getReservedStorage(mem, std::alignment_of<T>()));
}

// Object handle for an object in cache. This handle owns the resource backing
// an object cached in cachelib cache. User should only access an object via
// an object handle, or a shared_ptr converted from object handle.
template <typename T, typename CacheDescriptor, typename AllocatorResource>
class CacheObjectHandle {
 public:
  using ItemHandle = typename CacheDescriptor::ItemHandle;

  CacheObjectHandle() = default;
  ~CacheObjectHandle() = default;

  explicit CacheObjectHandle(ItemHandle handle) : handle_{std::move(handle)} {
    if (handle_) {
      if (*reinterpret_cast<uint16_t*>(handle_->getMemory()) != 0xbeef) {
        throw std::invalid_argument(folly::sformat(
            "This item does not have an allocator associated. Key: {}",
            handle_->getKey()));
      }

      ptr_ = getType<T, AllocatorResource>(handle_->getMemory());
    }
  }

  CacheObjectHandle(const CacheObjectHandle&) = delete;
  CacheObjectHandle& operator=(const CacheObjectHandle&) = delete;

  CacheObjectHandle(CacheObjectHandle&& other) {
    ptr_ = other.ptr_;
    handle_ = std::move(other.handle_);
    other.ptr_ = nullptr;
  }
  CacheObjectHandle& operator=(CacheObjectHandle&& other) {
    if (this != &other) {
      new (this) CacheObjectHandle(std::move(other));
    }
    return *this;
  }

  explicit operator bool() const noexcept { return get() != nullptr; }
  T* operator->() const noexcept { return get(); }
  T& operator*() const noexcept { return *get(); }
  T* get() const noexcept { return ptr_; }

  // View item handle. This is useful if user wants to work with the
  // underlying CacheAllocator directly with an ItemHandle
  const ItemHandle& viewItemHandle() const noexcept { return handle_; }

  ItemHandle& viewItemHandle() { return handle_; }

  // Moves ownership into a regular item handle.
  ItemHandle releaseItemHandle() && {
    // Reset "pointer" since we're converting this into a regular item handle
    ptr_ = nullptr;
    return std::move(handle_);
  }

  // Moves ownership into a shared_ptr. This is irreversible.
  // TODO: in the future, we should consider if we want to allow user to
  //       convert shared_ptr back into an ObjectHandle via get_deleter().
  //       https://en.cppreference.com/w/cpp/memory/shared_ptr/get_deleter
  std::shared_ptr<T> toSharedPtr() && {
    CacheObjectHandle objHandle{std::move(*this)};
    auto* t = objHandle.get();
    return std::shared_ptr<T>(
        t, [objHandle = std::move(objHandle)](T*) mutable {
          std::move(objHandle).releaseItemHandle().reset();
        });
  }

 private:
  // Handle to ensure the object remains valid
  ItemHandle handle_{};
  T* ptr_{};
};

template <typename ObjectCache>
class ObjectCacheCompactor {
 public:
  explicit ObjectCacheCompactor(
      ObjectCache& objcache,
      std::unique_ptr<typename ObjectCache::CompactionSyncObj> syncObj)
      : objcache_{objcache}, syncObj_{std::move(syncObj)} {}

  template <typename T>
  void compact(folly::StringPiece key, void* unalignedMem) const {
    auto poolId = objcache_.getCacheAlloc().getAllocInfo(unalignedMem).poolId;
    auto* obj =
        getType<T, typename ObjectCache::AllocatorResource>(unalignedMem);

    auto newObj = objcache_.template createCompact<T>(poolId, key, *obj);
    objcache_.insertOrReplace(newObj);
  }

 private:
  // Marking this mutable as we will use non-const method in object cache but
  // the compactor object itself should be kept as const.
  ObjectCache& objcache_;
  std::unique_ptr<typename ObjectCache::CompactionSyncObj> syncObj_;
};

template <typename ObjectCache>
class ObjectCacheConfig {
 public:
  using CacheAllocatorConfig = typename ObjectCache::CacheAlloc::Config;

  ObjectCacheConfig& setCacheAllocatorConfig(CacheAllocatorConfig config) {
    cacheAllocatorConfig_ = config;
    return *this;
  }
  CacheAllocatorConfig& getCacheAllocatorConfig() {
    // User can mutate certain fields in cache allocator config
    return cacheAllocatorConfig_;
  }
  const CacheAllocatorConfig& getCacheAllocatorConfig() const {
    return cacheAllocatorConfig_;
  }

  // If objects have non-trivial destructor semantics, then it is recommended
  // user should create destructor callback, so cachelib will destroy objects
  // properly when they are evicted/removed from cache. For example, if the
  // objects contain any objects allocated on heap, or has any side effects
  // in its destructor logic, then they should have a destructor callback.
  //
  // Example of specifying a destructor callback
  //  auto dcb = [](folly::StringPiece key, void* unalignedMem) {
  //    if (key.startsWith("map_type_")) {
  //      getType<MyMapType>(unalignedMem)->~MyMapType();
  //    } else if (key.startsWith("my_other_type_")) {
  //      getType<MyOtherType>(unalignedMem)->~MyOtherType();
  //    }
  //  };
  //  config.setDestructorCallback(dcb);
  using DestructorCallback = typename ObjectCache::DestructorCallback;
  ObjectCacheConfig& setDestructorCallback(DestructorCallback destructorCb) {
    destructorCb_ = std::move(destructorCb);
    return *this;
  }
  const DestructorCallback& getDestructorCallback() const {
    return destructorCb_;
  }

  // Compaction callback is an asynchrnous mechancism to copy the objects
  // and compact them in memory footprint throughout the process. This is
  // useful if user frequently mutates the cache objects. If the objects
  // are rarely mutated (mostly a read workload), then there's no need to
  // enable compaction. Compaction also comes with a cpu cost, as we have
  // to walk the cache and check each object for compaction threshold, and
  // compact eligible objects. The faster we compact, the more expensive
  // it will be for CPU. The compaction speed is tunable. Also note that
  // cachelib will never use more than one full core for compaction.
  //
  // Example of specifying a compaction callback
  //  auto ccb = [](folly::StringPiece key,
  //                void* unalignedMem,
  //                const Compactor& compactor) {
  //    if (key.startsWith("map_type_")) {
  //      compactor.compact<MyMapType>(key, unalignedMem);
  //    } else if (key.startsWith("my_other_type_")) {
  //      compactor.compact<MyOtherType>(key, unalignedMem);
  //    }
  //  };
  //  struct UserCompactionSyncObj : public CompactionSyncObj {
  //    UserCompactionSyncObj(UserLock& lock) : l{lock} {}
  //    std::lock_guard<UserLock> l;
  //  };
  //  auto compactionSync = [] (folly::StringPiece key) {
  //    return std::unique_ptr<CompactionSyncObj>(
  //        new UserCompactionSyncObj{getLock(key)});
  //  };
  //  config.enableCompaction(ccb, compactionSync);
  using CompactionCallback = typename ObjectCache::CompactionCallback;
  using CompactionSync = typename ObjectCache::CompactionSync;
  ObjectCacheConfig& enableCompaction(
      CompactionCallback compactionCb,
      CompactionSync compactionSync,
      std::chrono::milliseconds compactionWork = std::chrono::milliseconds{5},
      std::chrono::milliseconds compactionSleep = std::chrono::milliseconds{
          45}) {
    compactionCb_ = std::move(compactionCb);
    compactionSync_ = std::move(compactionSync);
    compactionWork_ = compactionWork;
    compactionSleep_ = compactionSleep;
    return *this;
  }
  const CompactionCallback& getCompactionCallback() const {
    return compactionCb_;
  }
  const CompactionSync& getCompactionSync() const { return compactionSync_; }
  const std::chrono::milliseconds getCompactionWorkTime() const {
    return compactionWork_;
  }
  const std::chrono::milliseconds getCompactionSleepTime() const {
    return compactionSleep_;
  }

  // TODO: add comments for persistence
  using SerializationCallback = typename ObjectCache::SerializationCallback;
  using DeserializationCallback = typename ObjectCache::DeserializationCallback;
  ObjectCacheConfig& enablePersistence(
      uint32_t persistorRestorerThreadCount,
      uint32_t restorerTimeOutDurationInSec,
      std::string persistFullPathFile,
      SerializationCallback scb,
      DeserializationCallback dcb,
      uint32_t persistorQueueBatchSize = 1000) {
    serializationCallback_ = std::move(scb);
    deserializationCallback_ = std::move(dcb);
    persistorRestorerThreadCount_ = persistorRestorerThreadCount;
    restorerTimeOutDurationInSec_ = restorerTimeOutDurationInSec;
    persistFullPathFile_ = std::move(persistFullPathFile);
    persistorQueueBatchSize_ = persistorQueueBatchSize;
    return *this;
  }

  const SerializationCallback& getSerializationCallback() const {
    return serializationCallback_;
  }
  const DeserializationCallback& getDeserializationCallback() const {
    return deserializationCallback_;
  }
  [[nodiscard]] uint32_t getPersistorRestorerThreadCount() const {
    return persistorRestorerThreadCount_;
  }
  [[nodiscard]] uint32_t getRestorerTimeOut() const {
    return restorerTimeOutDurationInSec_;
  }
  [[nodiscard]] const std::string& getPersistFullPathFile() const {
    return persistFullPathFile_;
  }
  [[nodiscard]] uint32_t getPersistorQueueBatchSize() const {
    return persistorQueueBatchSize_;
  }

 private:
  CacheAllocatorConfig cacheAllocatorConfig_;

  DestructorCallback destructorCb_;

  CompactionCallback compactionCb_;
  CompactionSync compactionSync_;
  std::chrono::milliseconds compactionWork_;
  std::chrono::milliseconds compactionSleep_;

  SerializationCallback serializationCallback_;
  DeserializationCallback deserializationCallback_;
  uint32_t persistorRestorerThreadCount_;
  uint32_t restorerTimeOutDurationInSec_;
  std::string persistFullPathFile_;
  uint32_t persistorQueueBatchSize_;
};

struct ObjectCacheStats {
  struct StatsAggregate {
    uint64_t compactions;
  };

  StatsAggregate getAggregate() {
    StatsAggregate agg;
    agg.compactions = compactions.get();
    return agg;
  }

  AtomicCounter compactions;
};

// TODO: Allow user to specify any compatible allocator resource
//       Today we force a user to specifiy which allocator resource to
//       use at compile-time. We don't actually need to impose this
//       restriction since our Allocator is designed to allow different
//       AllocatorResource to be interchanged as long as they're compatible
//       with one another. So this restriction should be loosened up.
/*
  An example of using ObjectCache is as following:

  using LruObjectCache =
    ObjectCache<CacheDescriptor<LruAllocator>,
                MonotonicBufferResource<CacheDescriptor<LruAllocator>>>;

  auto cacheAllocatorPtr = myMethodToCreateCacheAllocator(...);
  LruObjectCache objcache{cacheAllocatorPtr, ownsCache: true | false};

  using Vector = std::vector<int, LruObjectCache::Alloc<int>>;

  // Create a new vector in cache
  auto vec = objcache.create<Vector>(poolId, "my obj");

  // Insert it
  objcache.insertOrReplace(vec);

  // Find it
  auto vec2 = objcache.find<Vector>("my obj");

  // Access it
  for (int i = 0; i < 10; i++) {
    vec2->push_back(i);
  }
*/
template <typename CacheDescriptor, typename AllocatorRes>
class ObjectCache {
 public:
  using AllocatorResource = AllocatorRes;
  using Config = ObjectCacheConfig<ObjectCache>;
  using CacheAlloc = typename CacheDescriptor::Cache;
  using Item = typename CacheDescriptor::Item;
  using ItemHandle = typename CacheDescriptor::ItemHandle;

  // CompactionSyncObj is an object that holds exclusive access to an item.
  // Any class that implements CompactionSyncObj can hold a "lock" or any other
  // synchronization primitive over an item.
  using CompactionSyncObj = typename CacheAlloc::SyncObj;
  // CompactionSync should return a synchronization object given a key. On
  // failure, return an valid sync obj that returns `isValid() == false`.
  using CompactionSync =
      std::function<std::unique_ptr<CompactionSyncObj>(folly::StringPiece)>;

  // Destroy an object when it is evicted/removed from cache. This is an
  // object's destructor. Similarly, this cannot throw and cachelib will
  // crash as a result of any exception in this callback.
  using DestructorCallback =
      std::function<void(folly::StringPiece key,
                         void* unalignedMem,
                         const typename CacheAlloc::RemoveCbData& data)>;

  // Compactor is a helper class that exposes only the logic necessary to
  // compact objects.
  using Compactor = ObjectCacheCompactor<ObjectCache>;
  // CompactionCallback is a user-supplied callback that compacts object,
  // refer to Config::enableCompaction() for how to use this. This function
  // should not throw. Failure of compaction must not produce any side-effect
  // to the object we're trying to compact.
  using CompactionCallback = std::function<void(
      folly::StringPiece key, void* unalignedMem, const Compactor& cache)>;

  // SerializationCallback takes an object and serialize it to an iobuf. On
  // success, return a valid iobuf. Otherwise, return nullptr.
  using SerializationCallback = std::function<std::unique_ptr<folly::IOBuf>(
      folly::StringPiece key, void* unalignedMem)>;
  // DeserializationCallback takes an iobuf and deserialize it back into an
  // object. On success, return a valid item handle. Otherwise, return an empty
  // one. User can only create an object via "cache". Other operations are NOT
  // allowed.
  //
  // TODO: replace "cache" with a "CacheCreator" helper, and update comments
  using DeserializationCallback =
      std::function<typename ObjectCache::ItemHandle(PoolId poolId,
                                                     folly::StringPiece key,
                                                     folly::StringPiece payload,
                                                     uint32_t creationTime,
                                                     uint32_t expiryTime,
                                                     ObjectCache& cache)>;

  template <typename T>
  using ObjectHandle = CacheObjectHandle<T, CacheDescriptor, AllocatorResource>;

  template <typename T>
  using Alloc = std::scoped_allocator_adaptor<Allocator<T, AllocatorResource>>;

  // Convert an item handle to an object handle
  template <typename T>
  static ObjectHandle<T> toObjectHandle(ItemHandle handle) {
    return ObjectHandle<T>{std::move(handle)};
  }

  // Convert an item handle to a shared_ptr handle. Note that this incurs
  // a heap allocation to create the control block for shared_ptr.
  template <typename T>
  static std::shared_ptr<T> toSharedPtr(ItemHandle handle) {
    return ObjectHandle<T>{std::move(handle)}.toSharedPtr();
  }

  // TODO: add shared-mem constructors
  // Creates an object cache that owns the underlying cache
  // @param config    config to create cache allocator managed by ObjectCache
  explicit ObjectCache(Config config) : ObjectCache(createCache(config), true) {
    serializationCallback_ = config.getSerializationCallback();
    deserializationCallback_ = config.getDeserializationCallback();
    persistorRestorerThreadCount_ = config.getPersistorRestorerThreadCount();
    restorerTimeOutDurationInSec_ = config.getRestorerTimeOut();
    persistFullPathFile_ = config.getPersistFullPathFile();
    persistorQueueBatchSize_ = config.getPersistorQueueBatchSize();

    if (config.getCompactionCallback()) {
      compactionWorker_ =
          std::make_unique<CompactionWorker>(*this,
                                             config.getCompactionCallback(),
                                             config.getCompactionSync(),
                                             config.getCompactionWorkTime(),
                                             config.getCompactionSleepTime());
      compactionWorker_->start(std::chrono::seconds{1});
    }
  }

  // TODO: pass in the configs for compaction, destructor, and persistence
  //
  // Creates an object cache. This should be used when user has an existing
  // CacheAllocator instance and is the process of migrating to ObjectCache.
  // @param cache       pointer to the cache allocator
  // @param ownsCache   whether or not this class should own the underlying
  //                    cache allocator
  ObjectCache(CacheAlloc* cache, bool ownsCache)
      : cache_{cache}, ownsCache_{ownsCache} {}

  ~ObjectCache() {
    if (compactionWorker_) {
      compactionWorker_->stop();
    }
    if (ownsCache_) {
      delete cache_;
    }
  }

  // Get the underlying CacheAllocator
  CacheAlloc& getCacheAlloc() { return *cache_; }

  void persist() {
    XDCHECK(serializationCallback_);
    ObjectCachePersistor<ObjectCache> persistor(
        persistorRestorerThreadCount_, serializationCallback_, *this,
        persistFullPathFile_, persistorQueueBatchSize_);
    persistor.run();
  }

  void recover() {
    XDCHECK(deserializationCallback_);
    ObjectCacheRestorer<ObjectCache> restorer(
        persistorRestorerThreadCount_, deserializationCallback_, *this,
        persistFullPathFile_, restorerTimeOutDurationInSec_);
    restorer.run();
  }

  // Create a new object backed by cachelib-memory. This behaves similar to
  // std::make_unique. User can pass in any arguments that T's constructor
  // can take. Note this API only creates a new object but does not insert it
  // into cache. User must call insertOrReplace to make this object visible.
  //
  // @param poolId    Cache pool this object will be allocated from.
  // @param key       Key associated with the object
  // @param args...   Arguments for T's constructor
  // @return  a handle to an object
  // @throw   ObjectCacheAllocationError on allocation error
  //          Any exceptions from within T's constructor
  template <typename T, typename... Args>
  ObjectHandle<T> create(PoolId poolId,
                         folly::StringPiece key,
                         Args&&... args) {
    return createWithTtl<T>(poolId, key, 0 /* ttlSecs*/, 0 /* creationTime */,
                            std::forward<Args>(args)...);
  }

  // Same as above where you can pass ttl and creation time of the object.
  // @param poolId    Cache pool this object will be allocated from.
  // @param key       Key associated with the object
  // @param ttlSecs   Time To Live(second) for the item,
  // @param args...   Arguments for T's constructor
  // @return  a handle to an object
  // @throw   ObjectCacheAllocationError on allocation error
  //          Any exceptions from within T's constructor
  template <typename T, typename... Args>
  ObjectHandle<T> createWithTtl(PoolId poolId,
                                folly::StringPiece key,
                                uint32_t ttlSecs,
                                uint32_t creationTime,
                                Args&&... args) {
    // TODO: Allow user to specify any compatible allocator resource
    auto [handle, mbr] = createMonotonicBufferResource<AllocatorResource>(
        *cache_, poolId, key,
        getTypeSize<T>() /* reserve minimum space for this object */,
        0 /* additional bytes for storage */, std::alignment_of<T>(), ttlSecs,
        creationTime);
    new (getType<T, AllocatorResource>(handle->getMemory()))
        T(std::forward<Args>(args)..., Alloc<char>{mbr});

    // We explicitly unmark this handle as nascent so we will trigger the
    // destructor associated with this item properly in the remove callback
    detail::objcacheUnmarkNascent(handle);
    return ObjectHandle<T>{std::move(handle)};
  }

  // Compact an object by copying its content onto a single item. This is
  // only valid if the oldItem is already managed by cachelib.
  // TODO: is there a better way to design this API? This easily crashes
  //       if user passes in a non-cachelib managed object.
  // TODO: document params
  template <typename T>
  ObjectHandle<T> createCompact(PoolId poolId,
                                folly::StringPiece key,
                                const T& oldObject,
                                uint32_t ttlSecs = 0,
                                uint32_t creationTime = 0) {
    // TODO: handle when this is bigger than 4MB
    const uint32_t usedBytes = oldObject.get_allocator()
                                   .getAllocatorResource()
                                   .viewMetadata()
                                   ->usedBytes;

    // TODO: Allow user to specify any compatible allocator resource
    auto [handle, mbr] = createMonotonicBufferResource<AllocatorResource>(
        *cache_, poolId, key,
        getTypeSize<T>() /* reserve minimum space for this object */,
        usedBytes /* additional bytes for storage */, std::alignment_of<T>(),
        ttlSecs, creationTime);
    new (getType<T, AllocatorResource>(handle->getMemory()))
        T(oldObject, Alloc<char>{mbr});

    // We explicitly unmark this handle as nascent so we will trigger the
    // destructor associated with this item properly in the remove callback
    detail::objcacheUnmarkNascent(handle);
    return ObjectHandle<T>{std::move(handle)};
  }

  // Look up an object in cache
  // @param key   Key associated with the object
  // @param mode  Access mode: kRead or kWrite
  // @return  a handle to the object; nullptr if not found.
  template <typename T>
  ObjectHandle<T> find(folly::StringPiece key,
                       AccessMode mode = AccessMode::kRead) {
    auto handle = cache_->findImpl(key, mode);
    return toObjectHandle<T>(std::move(handle));
  }

  // Insert an object into cache
  // @param handle    Handle to the object
  // @return  a handle to an existing object that we replaced if present;
  //          nullptr otherwise.
  template <typename T>
  ObjectHandle<T> insertOrReplace(const ObjectHandle<T>& handle) {
    auto oldHandle = cache_->insertOrReplace(handle.viewItemHandle());
    return toObjectHandle<T>(std::move(oldHandle));
  }

  // Remove an object from cache. Note that the object may NOT be destroyed
  // immediately. The destructor will only be called when the last holder
  // of a handle to the object drops the handle.
  // @param key   Key associated with the object
  void remove(folly::StringPiece key) { cache_->remove(key); }

  // Wake up the compaction thread and trigger a compactin. This is only used
  // for testing.
  void triggerCompactionForTesting() {
    XDCHECK(compactionWorker_);
    compactionWorker_->wakeUp();
  }

  ObjectCacheStats::StatsAggregate getStats() const {
    return stats_->getAggregate();
  }

 private:
  class CompactionWorker : public PeriodicWorker {
   public:
    explicit CompactionWorker(
        ObjectCache& objcache,
        typename ObjectCache::CompactionCallback compactionCb,
        typename ObjectCache::CompactionSync compactionSync,
        std::chrono::milliseconds compactionWork,
        std::chrono::milliseconds compactionSleep)
        : objcache_{objcache},
          compactionCb_{std::move(compactionCb)},
          compactionSync_{std::move(compactionSync)},
          compactionWork_{compactionWork},
          compactionSleep_{compactionSleep} {}

    void work() override {
      util::Throttler::Config config;
      config.sleepMs = compactionSleep_.count();
      config.workMs = compactionWork_.count();
      for (auto it = objcache_.getCacheAlloc().begin(config);
           it != objcache_.getCacheAlloc().end();
           ++it) {
        if (shouldStopWork()) {
          return;
        }

        // Grab compaction sync for exclusive access to the object
        // while we perform compaction. This is to prevent any user
        // threads from mutating the object during compaction.
        std::unique_ptr<CompactionSyncObj> syncObj;
        if (compactionSync_) {
          syncObj = compactionSync_(it->getKey());
          if (!syncObj->isValid()) {
            XLOGF(ERR,
                  "Unable to grab compaction sync. Skipping key: {}",
                  it->getKey());
            continue;
          }
        }
        compactionCb_(it->getKey(),
                      it->getMemory(),
                      ObjectCacheCompactor{objcache_, std::move(syncObj)});
        objcache_.stats_->compactions.inc();
      }
    }

   private:
    ObjectCache& objcache_;
    typename ObjectCache::CompactionCallback compactionCb_;
    typename ObjectCache::CompactionSync compactionSync_;
    std::chrono::milliseconds compactionWork_;
    std::chrono::milliseconds compactionSleep_;
  };

  static CacheAlloc* createCache(Config& config) {
    // TODO: we should allow user to specify their own remove callback. We can
    //       just wrap it within object cache's own remove callback
    if (config.getCacheAllocatorConfig().removeCb) {
      throw std::invalid_argument("No remove callback allowed");
    }

    // TODO: Move callback should trigger an object level compaction
    if (config.getCacheAllocatorConfig().moveCb) {
      throw std::invalid_argument("No remove callback allowed");
    }

    if (config.getDestructorCallback()) {
      config.getCacheAllocatorConfig().setRemoveCallback(
          [dcb = config.getDestructorCallback()](
              const typename CacheAlloc::RemoveCbData& data) {
            auto key = data.item.getKey();
            dcb(key, data.item.getMemory(), data);
          });
    }

    return new CacheAlloc(config.getCacheAllocatorConfig());
  }

  mutable std::unique_ptr<ObjectCacheStats> stats_{
      std::make_unique<ObjectCacheStats>()};

  CacheAlloc* cache_{};
  bool ownsCache_{false};
  std::unique_ptr<CompactionWorker> compactionWorker_;
  SerializationCallback serializationCallback_;
  DeserializationCallback deserializationCallback_;
  uint32_t persistorRestorerThreadCount_;
  uint32_t restorerTimeOutDurationInSec_;
  std::string persistFullPathFile_;
  uint32_t persistorQueueBatchSize_;
};
} // namespace objcache
} // namespace cachelib
} // namespace facebook
