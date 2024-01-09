---
id: Object_Cache_Architecture_Guide
title: Object-Cache Design
---

## Introduction

Object-Cache enables CacheLib users to cache complex C++ objects natively.

Before Object-Cache, users who need to cache complex/non-POD C++ structs are forced to choose between the following 2 ways:

- (common) serializing/deserializing objects on each write/read -> which is CPU-expensive and sometimes hard to implement
- re-implementing the C++ data structures to work with CacheLib memory allocator (e.g. [cachelib map](../../Cache_Library_User_Guides/Structured_Cache.md#map)) -> which is difficult to develop and maintain over time

With Object-Cache, users are able to cache complex C++ objects without going through either of the above two pains. Once the object has been allocated on the heap (e.g. via `std::make_unique`), Object-Cache tracks the **object pointer** instead of maintaining a full copy of the object. In this case, each Object-Cache lookup is almost as efficient as a pointer access; each Object-Cache allocation only copies the object pointer plus some metadata (note that user is responsible for the heap allocation of the object).

This approach also implies that Object-Cache achieves CPU efficiency and implementation simplification by sacrificing memory efficiency: an unserialized object can be much larger than its serialized version. Therefore, when considering if Object-Cache v.s. the regular CacheLib is a better fit, you should always evaluate your system's constraints (CPU, memory, etc.) and the amplification rate (i.e. unserialized object size : serialized object size). A general rule is only considering Object-Cache when you have to cache non-POD C++ objects and your system has enough memory headroom to store objects in an unserialized form.

## Design Details

Object-Cache is built on top of the regular CacheLib. We can view its memory composition as two parts: CacheLib allocated memory (RAM-only) and heap memory.

|  | Allocator | Data | Data type |
| --- | --- | --- | --- |
| CacheLib memory | CacheLib MemoryAllocator | object pointer, key, metadata | only allow POD |
| heap memory | C++ heap memory allocator, e.g. jemalloc | actual object | any type of C++ structs |

### CacheLib allocated memory

CacheLib allocated memory contains `ObjectCacheItem`, `CacheItem` and the cache key.

- `ObjectCacheItem`: object pointer and the amount of heap memory required by the object

```cpp
struct ObjectCacheItem {
  uintptr_t objectPtr; // raw pointer of the object
  size_t objectSize; // used for size-awareness which will be explained later
};
```

- `CacheItem`: regular CacheLib Item, storing regular CacheLib required metadata
- cache key: key to look up the object

#### allocation size

`ObjectCacheItem` is essentially the "value" stored in the CacheLib cache. For each CacheLib allocation, the required size can be calculated using the following formula:

```cpp
required size of a CacheLib allocation
= value.size() + key.size() + sizeof(CacheItem)
= sizeof(ObjectCacheItem) + key.size() + sizeof(CacheItem) +
= 16 bytes + key.size() + 32 bytes
= 48 bytes + key.size()
```

Therefore, a single allocation class can be used for all CacheLib allocations.

```cpp
defaultAllocSize = (48 bytes + maxKeySize) align to 8 bytes
```

_The maximum key size here is configurable (default to be the maximum value 255-byte)._

#### cache size

CacheLib allocated memory is pre-allocated memory. To estimate the total size of this part of memory, we ask the user to specify `entriesLimit`: the maximum number of objects stored in Object-Cache. Then the CacheLib cache size is approximate to `entriesLimit * defaultAllocSize`.

```cpp
CacheLib cache size ~= entriesLimit * defaultAllocSize
```

#### sharding

CacheLib allocated memory can still be split into multiple pools. While here the "pool" functions more like a "shard": it is no longer to partition different workloads but to improve insertion concurrency. All pools have the same pool size and use the same allocation size (i.e. `defaultAllocSize`). To determine the pool id of an incoming object, we use a hash-based sharding mechanism.

```cpp
auto hash = cachelib::MurmurHash2{}(key.data(), key.size());
poolId = static_cast<PoolId>(hash % numShards);
```

### Heap memory

The actual objects are allocated on the heap. If jemalloc is the default allocator, it will be allocated via jemalloc. A normal way to do such allocation is calling `std::make_unique<ObjectType>(...)`.

#### size-awareness: tracking and controlling heap memory usage

Obviously, this part of memory is dynamic allocated and not managed by CacheLib. Tracking it won't be as precise and straightforward as tracking memory allocated by CacheLib. Considering there is already a limit for #objects, user may not have to track the memory footprint here. However, if the object sizes can vary within a wide range, only tracking #objects is not enough. In this case, we introduced **size-awareness** feature to track and control the heap memory usage within certain limit. Note that this feature assumes **jemalloc** as the allocator.

1. calculate the amount of net allocated heap memory when constructing the object
   - this can be done by [jemalloc util functions](../Object_Cache/object_cache_user_guide.md#how-to-calculate-object-size)
2. pass the size information to Object-Cache so that it can track each object's size and the total size
   - insertion: set `ObjectCacheItem::objectSize` and increase `totalObjectSize` by `ObjectCacheItem::objectSize`
   - mutation: update `ObjectCacheItem::objectSize` by delta and update `totalObjectSize` by the same delta
   - remove and eviction: decrease `totalObjectSize` by `ObjectCacheItem::objectSize`
3. run a background thread to peridocially adjust the current `entriesLimit` based on the current `totalObjectSize`, and eviction will be triggered when the limit is reached
   - **size-controller** is the background thread for step 3. It starts when Object-Cache is created. Since then it will periodically check `totalObjectSize` and adjust the `currEntriesLimit` accordingly:
     - initial value of `currEntriesLimit` is `config.entriesLimit`
     - allocating placeholders to decrease `currEntriesLimit` and trigger eviction
     - deallocating placeholders to increase `currEntriesLimit`

```cpp
// get new entriesLimit
averageObjSize = current totalObjSize / current #objects;
newEntriesLimit = min(configured entriesLimit, configured heapSizeLimit / averageObjSize)

// adjust placeholders
if (newEntriesLimit < currEntriesLimit) {
   if (currentEntriesNum reaches newEntriesLimit) {
     // allocate more placeholders to shrink the cache
     allocate (currEntriesLimit - newEntriesLimit) more placeholders
   } else {
     do nothing
   }
} else if (newEntriesLimit > currEntriesLimit) {
   if (currentEntriesNum reaches currEntriesLimit) {
     // deallocate placeholders to expand the cache
     deallocate (newEntriesLimit - currEntriesLimit) more placeholders
   } else {
     do nothing
   }
}

currEntriesLimit = newEntriesLimit

```

### Object lifetime

You might ask since Object-Cache only stores the pointers, how to ensure the object can stay on the heap until Object-Cache no longer need to access the object? Actually, the control of the object's lifetime will be transferred to Object-Cache once it is inserted into Object-Cache.

1. `std::make_unique` to allocate the object on the heap
2. request an Object-Cache insertion
3. request a CacheLib allocation -> returns a CacheLib Item's [`Handle`](../../Cache_Library_User_Guides/Item_and_Handle.md)
4. create an `ObjectCacheItem` by copying the object pointer (`object.get()`)
5. copy `ObjectCacheItem` to the `Handle`
6. insert the `Handle` to CacheLib
7. after a successful insertion, the ownership of the object will be transferred to Object-Cache; since then, Object-Cache manages the reference counts of the object internally and is responsible for destroying the object when it's evicted or removed

Object-Cache lookup returns a shared pointer using a custom deleter. This custom deleter holds a `Handle` generated via CacheLib find API and will only release this `Handle` when the shared pointer reaches 0 reference count. This aligns with the existing CacheLib design that a `Handle` is similar to a `std::shared_ptr`. Only when all handles are released which will trigger the item destructor, we should destroy the object memory.
