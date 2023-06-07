---
id: Object_Cache_User_Guide
title: CacheLib Object Cache User Guide
---

Object-Cache enables users to cache C++ objects natively in CacheLib.

Not sure whether you should use object-cache? Check the [object-cache decision guide](Object_Cache_Decision_Guide).

## Set up object-cache

### Create a simple object-cache

The simplest object-cache is limited by the **number of objects**, i.e. an eviction will be triggered when the total object number reaches certain limit; the limit needs to be configured by the user as `l1EntriesLimit`.

If your system is able to track the number of objects and provide that limit, you are good to use this option. If not, you may need to [create a "size-aware" object-cache](#create-a-size-aware-object-cache).

#### Configuration

You can set up a simple object-cache by configuring the following settings in `ObjectCacheConfig`:

- (**Required**) `l1EntriesLimit`: The object number limit for object-cache to hold. Above this many entries, object-cache will start evicting.
- (**Required**) `cacheName`: The name of the cache.
- (**Required**) `itemDestructor`: The callback that will be triggered when the object leaves the cache. Users must set this to explicitly delete the objects; otherwise, there will be memory leak.

```cpp
// store a single type of objects
config.setItemDestructor(
      [&](ObjectCacheDestructorData data) {
        data.deleteObject<Foo>();
});
```

```cpp
// store multiple types of objects
// One way is to encode the type in the key.
enum class user_defined_ObjectType { Foo1, Foo2, Foo3 };

config.setItemDestructor([&](ObjectCacheDestructorData data) {
     switch (user_defined_getType(data.key)) {
       case user_defined_ObjectType::Foo1:
         data.deleteObject<Foo1>();
         break;
       case user_defined_ObjectType::Foo2:
         data.deleteObject<Foo2>();
         break;
       case user_defined_ObjectType::Foo3:
         data.deleteObject<Foo3>();
         break;
       ...
     }
 });
```

- (**Suggested**) `maxKeySizeBytes`: The maximum size of the key to be inserted. It cannot exceed 255 bytes. Default to `255`. Since we also use this size to decide the size of object-cache, we suggest you set a reasonble value to avoid wasting space.
- `l1HashTablePower`: This controls how many buckets are present in object-cache's hashtable. Default to `10`. Check out [hashtable bucket configuration](../../Cache_Library_User_Guides/Configure_HashTable) to select a good value.
- `l1LockPower`: This controls how many locks are present in object-cache's hashtable. Default to `10`.
- `l1NumShards`: Number of shards to improve insert/remove concurrency. Default to `1`.
- `l1ShardName`: Name of the shards. If not set, we will use the default name `pool`.

```cpp
#include "cachelib/experimental/objcache2/ObjectCache.h"

using ObjectCache = cachelib::objcache2::ObjectCache<cachelib::LruAllocator>;
std::unique_ptr<ObjectCache> objCache;

struct Foo {
 ...
};

void init() {
    ObjectCache::Config config;
    config.setCacheName("SimpleObjectCache")
        .setCacheCapacity(10'000 /* l1EntriesLimit */)
        .setItemDestructor(
            [&](cachelib::objcache2::ObjectCacheDestructorData data) {
              data.deleteObject<Foo>();
            })
        .setMaxKeySizeBytes(8)
        .setAccessConfig(15 /* l1hashTablePower */, 10 /* l1locksPower */)
        .setNumShards(2) /* optional */
        .setShardName("my_shard") /* optional */;

    objCache = ObjectCache::create(std::move(config));
}

```

### Create a "size-aware" object-cache

If your system needs to cap the cache size by bytes where the simple version mentioned above is not good enough, you can enable the "size-awareness" feature.

A "size-aware" object-cache tracks the object size internally and is limited by the **total size of objects**, i.e. an eviction will be triggered when the total size of objects reaches certain limit; the limit needs to be configured by the user as `cacheSizeLimit`.

:exclamation: **IMPORTANT:** A few notes before you try to create a "size-aware" object-cache:

- This feature currently only works for **immutable** access.
  - Immutable access means no "in-place modification" of the object once inserted to the cache.
  - In the future, we might be able to support **mutable** access.
- Objects number is still bounded by `l1EntriesLimit`.
  - Above this many entries, object-cache will start evicting even if `cacheSizeLimit` has not been reached.
  - Make sure you set a reasonably large `l1EntriesLimit` to avoid objects early eviction when it's far from reaching `cacheSizeLimit`.
- Users are responsible to calculate the size of each object and pass the value to object-cache:
  - We provide a util class to help calculate the object size. Check out ["how to calculate object size"](#how-to-calculate-object-size).
  - Object-cache maintains the total object size internally based on the object size provided by users. See more in ["how is object size tracked"](#how-is-object-size-tracked).

#### Configuration

To set up a **size-aware** object-cache, besides the [settings](#configuration) mentioned above, also configure the following settings:

- (**Required**) `sizeControllerIntervalMs`: Set a non-zero period (in milliseconds) to enable the ["size-controller"](#what-is-size-controller). `0` means "size-controller" is disabled.
- (**Required**) `cacheSizeLimit`: The limit of cache size in bytes. If total object size is above this limit, object-cache will start evicting.

```cpp
#include "cachelib/experimental/objcache2/ObjectCache.h"

using ObjectCache = cachelib::objcache2::ObjectCache<cachelib::LruAllocator>;
std::unique_ptr<ObjectCache> objCacheSizeAware;

struct Foo {
 ...
};

void init() {
    ObjectCache::Config config;
    config.setCacheName("SizeAwareObjectCache")
          .setCacheCapacity(10'000 /* l1EntriesLimit*/,
                            30 * 1024 * 1024 * 1024 /* 30GB, cacheSizeLimit */,
                            100 /* sizeControllerIntervalMs */)
          .setItemDestructor(
            [&](cachelib::objcache2::ObjectCacheDestructorData data) {
              data.deleteObject<Foo>();
            })
          .setMaxKeySizeBytes(8)
          .setAccessConfig(15 /* l1hashTablePower */, 10 /* l1locksPower */)
          .setNumShards(2) /* optional */
          .setShardName("my_shard") /* optional */;

    objCacheSizeAware = ObjectCache::create(std::move(config));
}

```

#### How to calculate object size

It is users' responsibility to calculate each object size (i.e. how many bytes are occupied by the object). We provide a util class `ThreadMemoryTracker` that users can leverage to do the calculation.

The basic idea is:

1. Use Jemalloc util function (`thread.allocated` and `thread.deallocated`) to calculate allocated memory and deallocated memory in the current thread:

```
  memory usage = allocate memory - deallocated memory
```

2. Get the currently used memory before and after the object construction, the difference is the object memory:

```
   get before memory usage
   ...construct object
   get after memory usage
   object size = after memory usage - before memory usage
```

Example:

```cpp
#include "cachelib/experimental/objcache2/util/ThreadMemoryTracker.h"

// initialize memory tracker only at the beginning
cachelib::objcache2::ThreadMemoryTracker tMemTracker;
...

auto beforeMemUsage = tMemTracker.getMemUsageBytes();
...construct the object
auto afterMemUsage = tMemTracker.getMemUsageBytes();
// afterMemUsage < beforeMemUsage occurs very rarely when the current thread
// spawns children threads and the main thread deallocate memory allocated by
// the children thread.
auto objectSize = LIKELY(afterMemUsage > beforeMemUsage)
                        ? (afterMemUsage - beforeMemUsage)
                        : 0;
```

#### How is object size tracked

When an object is inserted to the cache via `insertOrReplace` / `insert` API, users must pass "object size" to the API.

After that, object-cache knows the size for each cached object and maintains the total object size internally.

#### What is size controller

Size-controller is the key component to achieve a "size-aware" object-cache. It is a periodic background worker that dynamically adjusts the **entries limit** by monitoring the current **total object size** and **total object number**:

```
averageObjectSize = totalObjectSize / totalObjectNum

newEntriesLimit = config.cacheSizeLimit / averageObjectSize
```

In this case, we can guarantee cache size does not exceed `cacheSizeLimit` from long-term perspective. However, as it is not a precise control, we cannot prevent a sudden increase in object sizes.

### Add monitoring

After the initialization, you should also add [cacheAdmin](../Cache_Monitoring/Cache_Admin_Overview) to enable [monitoring](../Cache_Monitoring/monitoring) for object-cache.

```cpp
#include "cachelib/facebook/admin/CacheAdmin.h"

std::unique_ptr<cachelib::CacheAdmin> cacheAdmin;

void init() {
  ... setting up the object-cache here

  CacheAdmin::Config adminConfig; // default config should work just fine
  adminConfig.oncall = "my_team_oncall_shortname";
  cacheAdmin = std::make_unique<CacheAdmin>(*objCache, adminConfig);
}
```

## Use object-cache

### Add objects

To add objects to object-cache, call `insertOrReplace` or `insert` API:

```cpp
template <typename T>
std::tuple<bool, std::shared_ptr<T>, std::shared_ptr<T>> insertOrReplace(
    folly::StringPiece key,
    std::unique_ptr<T> object,
    size_t objectSize = 0,
    uint32_t ttlSecs = 0);

template <typename T>
std::pair<AllocStatus, std::shared_ptr<T>> insert(folly::StringPiece key,
                                                  std::unique_ptr<T> object,
                                                  size_t objectSize = 0,
                                                  uint32_t ttlSecs = 0);
```

- `insertOrReplace`:
  - Insert an object into the cache with a given key.
  - If the key exists in the cache, it will be replaced with new object.
  - Return a tuple of allocation status (`kSuccess` or `kAllocError`) , `shared_ptr` of newly inserted object (even if the object is not successfully inserted, it will still be converted to a `shared_ptr` and returned), and `shared_ptr` of the old object that has been replaced (if no replacement happened, `nullptr` will be returned).
- `insert`:
  - Unique insert an object into the cache with a given key.
  - If the key exists in the cache, the new object will NOT be inserted.
  - Return a pair of allocation status (`kSuccess`, `kKeyAlreadyExists` or `kAllocError`) and `shared_ptr` of newly inserted object. Note that even if the object is not successfully inserted, it will still be converted to a `shared_ptr` and returned.

Parameters:

- (**required**) `key`: object key
- (**required**) `object`: `unique_ptr` of the object to be inserted
- `objectSize`: size of the object to be inserted
  - default to `0`
  - for non-size-aware ones, always leave the value as `0`
  - for size-aware ones, **MUST provide a non-zero value** (check out ["how to calculate object size"](#how-to-calculate-object-size))
- `ttlSecs`: Time To Live(seconds) for the object
  - default to `0` means object has no expiring time.

Example(non-size-aware):

```cpp
...
auto [allocStatus, ptr, oldPtr] =
    objcache->insertOrReplace(key,
                              std::move(foo),
                              0 /*objectSize tracking is not enabled*/,
                              ttlSecs /*optional*/);
if (allocStatus == ObjectCache::AllocStatus::kSuccess) {
  ...
  return ptr;
} else { // ObjectCache::AllocStatus::kAllocError
  ...
}
```

```cpp
...
auto [allocStatus, ptr] =
    objcache->insert(key,
                     std::move(foo),
                     0 /*objectSize tracking is not enabled*/,
                     ttlSecs /*optional*/);
if (allocStatus == ObjectCache::AllocStatus::kSuccess) {
  ...
  return ptr;
} else if (allocStatus == ObjectCache::AllocStatus::kKeyAlreadyExists) {
  ...
} else { // ObjectCache::AllocStatus::kAllocError
  ...
}
```

Example(size-aware):

```cpp
...
auto [allocStatus, ptr, oldPtr] =
    objcacheSizeAware->insertOrReplace(key,
                                       std::move(foo),
                                       objectSize /* must be non-zero */,
                                       ttlSecs /*optional*/);
if (allocStatus == ObjectCache::AllocStatus::kSuccess) {
  ...
  return ptr;
} else { // ObjectCache::AllocStatus::kAllocError
  ...
}
...
```

```cpp
...
auto [allocStatus, ptr] =
    objcacheSizeAware->insert(key,
                              std::move(foo),
                              objectSize /* must be non-zero*/,
                              ttlSecs /*optional*/);
if (allocStatus == ObjectCache::AllocStatus::kSuccess) {
  ...
  return ptr;
} else if (allocStatus == ObjectCache::AllocStatus::kKeyAlreadyExists) {
  ...
} else { // ObjectCache::AllocStatus::kAllocError
  ...
}
...
```

### Get objects

To get objects from object-cache, call `find` or `findToWrite` API:

```cpp
template <typename T>
std::shared_ptr<const T> find(folly::StringPiece key);

template <typename T>
std::shared_ptr<T> findToWrite(folly::StringPiece key);
```

- `find`:
  - Look up an object in **read-only** access.
  - Return a `shared_ptr` to a const version of the object if found; `nullptr` if not found.
- `findToWrite`:
  - Look up an object in **mutable** access.
  - Return a `shared_ptr` to a mutable version of the object if found; `nullptr` if not found.

:exclamation: **IMPORTANT:**

Separating write and read traffic is quite important here. A misuse of these two APIs can lead to unreasonable eviction result because we only promotes read traffic by default. For more details, check out ["Eviction policy"](../../Cache_Library_User_Guides/eviction_policy.md#configuration)

The guidance here is:

- Always consider `find` API first;
- Choose `findToWrite` API only when an in-place modification needs to happen.

Example:

```cpp
std::shared_ptr<const Foo> foo = objcache->find<Foo>("foo");
if (foo !== nullptr) {
    ... some read operation
}
```

```cpp
std::shared_ptr<Foo> mutableFoo = objcache->findToWrite<Foo>("foo");
if (mutableFoo !== nullptr) {
    ... some write operation
}

```

### Remove objects

To remove objects from object-cache, call `remove` API:

```cpp
bool remove(folly::StringPiece key);
```

- `remove`:
  - Remove an object from cache by its key. No-op if the key not found.
  - Return `false` if the key is not found in object-cache.

Example:

```cpp
// objcache is empty
objcache->remove<Foo>("foo"); // no-op, return `false`

objcache->insertOrReplace<Foo>("foo", std::move(foo));
...

objcache->remove<Foo>("foo"); // foo will be removed, return `true`
```

## TTL (Time To Live)

Object-Cache provides the same TTL support as in regular cacheLib.

- TTL is at the granularity of seconds
- Object will not be accessible if it is beyond the TTL (i.e. `find` API will return `nullptr`)
- Object could still exist in the cache if it is beyond the TTL. We use `Reaper` (cachelib's TTL worker) to periodically remove expired objects from the cache. By default, the Reaper runs every 5 seconds. Users can also set a different interval via `ObjectCacheConfig`:

```cpp
ObjectCache::Config config;
...
config.setItemReaperInterval(std::chrono::milliseconds{10000}) // reaper will run every 10 seconds;
```

### Set TTL

As what mentioned in ["Add objects"](#add-objects) section, set the ttl for an object upon insertion:

```cpp
objcache->insertOrReplace(key,
                          std::move(obj),
                          objectSize,
                          10 /* ttl is 10 seconds */);

objcache->insert(key,
                 std::move(obj),
                 objectSize,
                 10 /* ttl is 10 seconds */);
```

### Get TTL

To get a cached object's ttl, we provide `getExpiryTimeSec` and `getConfiguredTtl` APIs:

```cpp
template <typename T>
uint32_t getExpiryTimeSec(const std::shared_ptr<T>& object) const;

template <typename T>
std::chrono::seconds getConfiguredTtl(const std::shared_ptr<T>& object) const;

```

- `getExpiryTimeSec`:
  - Return the expiry timestamp of the passed object (in seconds)
- `getConfiguredTtl`:
  - Return the configured TTL of the passed object (in seconds)

:exclamation: **IMPORTANT:**

- The passed object shared pointer must be fetched from object-cache APIs (e.g. find, insert APIs).

Usage 1:

```cpp
auto obj = objcache->find<T>("key");
auto expiryTimeSec = objcache->getExpiryTimeSec(obj);
if (expiryTime != 0) { // ttl is set
    if (expiryTimeSec < util::getCurrentTimeSec()) { // not expired
        ...
    } else {
        ...
    }
}
```

Usage 2:

```cpp
auto obj = objcache->find<T>("key");
auto ttlSecs = objcache->getConfiguredTtl(obj).count();
if (ttlSecs != 0) { // ttl is set
    ...
}
```

Object's `expiryTime` is also accessible via `ObjectCacheDestructorData`. One usage could be checking whether the object is expired when it's leaving the cache:

```cpp
ObjectCache::Config config;
...
config.setItemDestructor(
       [&](cachelib::objcache2::ObjectCacheDestructorData data) {
  ...
  if (data.expiryTime >= util::getCurrentTimeSec()) { // expired
     ...
  }
  ...
  data.deleteObject<T>();
}
```

### Update TTL

To update a cached object's ttl, we provide `updateExpiryTimeSec` and `extendTtl` APIs:

```cpp
template <typename T>
bool updateExpiryTimeSec(std::shared_ptr<T>& object,
                         uint32_t newExpiryTimeSecs);

template <typename T>
bool extendTtl(std::shared_ptr<T>& object, std::chrono::seconds ttl);
```

- `updateExpiryTimeSec`:
  - Update the expiry timestamp to `newExpiryTimeSecs`
  - Return `true` if the expiry time was successfully updated
- `extendTtl`:
  - Extend the expiry timestamp to `now + ttl` (in seconds)
  - Return `true` if the expiry time was successfully extended

:exclamation: **IMPORTANT:**

- The passed object shared pointer must be fetched from object-cache APIs (e.g. find, insert APIs).

Usage 1:

```cpp
auto obj = objcache->findToWrite<T>("key"); // calling find() API is also fine
objcache->updateExpiryTimeSec(obj, util::getCurrentTimeSec() + 300 /* 5mins */); // expiryTime becomes now + 5mins
...
```

Usage 2:

```cpp
auto obj = objcache->findToWrite<T>("key"); // calling find() API is also fine
objcache->extendTtl(obj, std::chrono::seconds(300) /* 5 mins*/); // expiryTime becomes now + 5mins
...
```

## Cache Persistence

Cache persistence is an opt-in feature in object-cache to persist objects across process restarts. It is useful when you want to restart your binary without losing previously cached objects. Currently we support cache persistence in a multi-thread mode where user can configure the parallelism degree to adjust the persistence/recovery speed.

Before enabling cache persistence, please be aware of the following limitations:

- Only works when you restart the process in the same machine. Across machines persistence is not supported.
- Only Thrift objects can be persisted.

To enable cache persistence, you need to configure the following parameters:

- `threadCount`: number of threads to work on persistence/recovery concurrently
- `persistBasefilePath`: file path to save the persistent data
  - cache metadata will be saved in "persistBasefilePath";
  - objects will be saved in "persistBasefilePath_i", i in [0, threadCount)
- `serializeCallback`: callback to serialize an object, used for object persisting
  - it takes `ObjectCache::Serializer` which has a `serialize<T>()` API that serializes the object of type `T` and returns a `folly::IOBuf`.
- `deserializeCallback`: callback to deserialize an object, used for object recovery
  - it takes `ObjectCache::Deserializer` which has a `deserialize<T>()` API that deserializes the object of type `T` and inserts it to the cache; returns `true` when the insertion is successful.

### Configure cache persistence

Example (single-type):

```cpp
ObjectCache::Config config;
...
config.enablePersistence(threadCount,
                         persistBaseFilePath,
                           [&](ObjectCache::Serializer serializer) {
                             return serializer.serialize<ThriftType>();
                           },
                          [&](ObjectCache::Deserializer deserializer) {
                             return deserializer.deserialize<ThriftType>();
                           });


```

Example (multi-type):

```cpp
ObjectCache::Config config;
...
config.enablePersistence(threadCount,
                         persistBaseFilePath,
                           [&](ObjectCache::Serializer serializer) {
                   switch (user_defined_getType(serializer.key)) {
                         case user_defined_Type::ThriftType1:
                              return serializer.serialize<ThriftType1>();
                         case user_defined_Type::ThriftType2:
                              return serializer.serialize<ThriftType2>();
                         case user_defined_Type::ThriftType3:
                              return serializer.serialize<ThriftType3>();
                         default:
                              …
                           },
                          [&](ObjectCache::Deserializer deserializer) {
                     switch (user_defined_getType(serializer.key)) {
                         case user_defined_Type::ThriftType1:
                              return deserializer.deserialize<ThriftType1>();
                         case user_defined_Type::ThriftType2:
                              return deserializer.deserialize<ThriftType2>();
                         case user_defined_Type::ThriftType3:
                              return deserializer.deserialize<ThriftType3>();
                         default:
                              …
                           });

```

### Use cache persistence

Once cache persistence is enabled, to persist or recover objects, it is as simple as an API call.

To persist, user should call `persist()` API upon cache shutdown:

```cpp
objCache->persist(); // all non-expired objects will be saved to files
```

To recover, user should call `recover()` API upon cache restart:

```cpp
objCache->recover(); // all saved non-expired objects will be recovered
```

Notes:

- Expired objects won't be persisted or recovered.
- To correctly recover objects, user must put the same `persistBaseFilePath` as the previous persistent cache instance.
- `threadCount` is for persisting parallelism of the current cache instance. Recovery will always use the same `threadCount` as the previous persistent cache instance. For example:

```cpp
config1.enablePersistence(5 /*threadCount*/, "baseFile_1", ..., ...);
auto objCache1 = ObjectCache::create(config1);
...
// ... shutting down cache
objCache1.persist(); // threadCount = 5

...
config2.enablePersistence(10 /*threadCount*/, "baseFile_1", ..., ...);
auto objCache2 = ObjectCache::create(config2);
//... restarting cache
objCache2.recover(); // threadCount = 5
...
//... shutting down cache
objCache2.persist(); // threadCount = 10

```
