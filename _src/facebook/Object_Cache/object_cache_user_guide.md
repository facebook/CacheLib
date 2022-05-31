---
id: Object_Cache_User_Guide
title: CacheLib Object Cache User Guide
---
Object-Cache enables users to cache C++ objects natively in CacheLib.

Not sure whether you should use object-cache? Check the [object-cache decision guide](Object_Cache_Decision_Guide).


## Set up object-cache

### Create a simple object-cache
You can set up object-cache by configuring the following settings in `ObjectCacheConfig`:
- (**Required**) `l1EntriesLimit`: Above this many entries, object-cache will start evicting. The object-cache size is also estimated based on this number.
- (**Required**) `cacheName`: The name of the cache.
- (**Required**) `maxKeySizeBytes`: The maximum sizeof key to be inserted. It cannot exceed 255 bytes.
- `l1HashTablePower`: This controls how many buckets are present in object-cache's hashtable. Default to `10`.
- `l1LockPower`: This controls how many locks are present in object-cache's hashtable. Default to `10`.
- `l1NumShards`: Number of shards to improve insert/remove concurrency. Default to `1`.

```cpp
#include "cachelib/experimental/objcache2/ObjectCache.h"

using ObjectCache = cachelib::objcache2::ObjectCache<cachelib::LruAllocator>;
std::unique_ptr<ObjectCache> objCache;

struct Foo {
 ...
};

void init() {
    cachelib::objcache2::ObjectCacheConfig config;
    config.l1EntriesLimit = 10'000;
    config.maxKeySizeBytes = 8;
    config.cacheName = "MyObjectCache";

    objCache = ObjectCache::create<Foo>(std::move(config));
}

```

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
// Insert the object into the cache with given key. If the key exists in the
// cache, it will be replaced with new obejct.
//
// Returns a pair of allocation status and shared_ptr of newly inserted object.
template <typename T>
std::pair<bool, std::shared_ptr<T>> insertOrReplace(
    folly::StringPiece key,
    std::unique_ptr<T> object, /* object to be inserted */
    uint32_t ttlSecs = 0, /* object expiring seconds */
    std::shared_ptr<T>* replacedPtr = nullptr /* object to be replaced if not nullptr */ );

// Insert the object into the cache with given key. If the key exists in the
// cache, the new object won't be inserted.
//
// Returns a pair of allocation status and shared_ptr of newly inserted object.
// Even if object is not inserted, it will still be converted to a shared_ptr and returned.
template <typename T>
std::pair<AllocStatus, std::shared_ptr<T>> insert(folly::StringPiece key,
                                                  std::unique_ptr<T> object,
                                                  uint32_t ttlSecs = 0);

```
Example:
```cpp
std::shared_ptr<Foo> cacheFoo(folly::StringPiece key, std::unique_ptr<Foo> foo) {
    ...
    auto [allocStatus, ptr] = objcache->insertOrReplace(key, std::move(foo));
    if (allocStatus == ObjectCache::AllocStatus::kSuccess) {
         ...
         return ptr;
    } else { // ObjectCache::AllocStatus::kAllocError
        ...
    }
    ...
}

std::shared_ptr<Foo> cacheFooUnique(folly::StringPiece key, std::unique_ptr<Foo> foo) {
    ...
    auto [allocStatus, ptr] = objcache->insert(key, std::move(foo));
    if (allocStatus == ObjectCache::AllocStatus::kSuccess) {
         ...
         return ptr;
    } else if (allocStatus == ObjectCache::AllocStatus::kKeyAlreadyExists) {
        ...
    } else { // ObjectCache::AllocStatus::kAllocError
        ...
    }
    ...
}
```

### Get objects
To get objects from object-cache, call `find` or `findToWrite` API:
```cpp
// Look up an object in read-only access.
template <typename T>
std::shared_ptr<const T> find(folly::StringPiece key);

// Look up an object in mutable access
template <typename T>
std::shared_ptr<T> findToWrite(folly::StringPiece key);
```
Example:
```cpp
std::shared_ptr<const Foo> foo = objcache->find<Foo>("foo");
if (foo !== nullptr) {
    ... some read operation
}

std::shared_ptr<Foo> mutableFoo = objcache->findToWrite<Foo>("foo");
if (mutableFoo !== nullptr) {
    ... some write operation
}

```

### Remove objects
To remove objects from object-cache, call `remove` API:
```cpp
// Remove an object from cache by its key. No-op if object doesn't exist.
void remove(folly::StringPiece key);
```
Example:
```cpp
// objcache is empty
objcache->remove<Foo>("foo"); // no-op

objcache->insertOrReplace<Foo>("foo", std::move(foo));
...

objcache->remove<Foo>("foo"); // foo will be removed
```
