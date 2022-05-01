---
id: Item_and_Handle
title: Item and Handle
---

An *item* is the fundamental memory allocation backing an object in cache. Throughout this guide, we sometimes use item and allocation interchangeably. We use *allocation* when we discuss memory allocation or footprint. And we use *item* when we want to emphasize cached objects. An item is associated with a key and a byte array allocated by the `allocate()` method. We use the key to look up the item.

There are 2 types of *Handle*: `ReadHandle` and `WriteHandle`.

A `ReadHandle` is similar to a `std::shared_ptr<const Item>`. Cachelib APIs like `find()` returns a `ReadHandle`.

A `WriteHandle` is similar to a `std::shared_ptr<Item>`. Cachelib APIs like `allocate()`, `findToWrite()`and `insertOrReplace()` return a `WriteHandle`.

`ItemHandle` is the old name of a `WriteHandle`, which will be deprecated in the future.

Because an item may be accessed concurrently, to ensure that the underlying memory backing the item is valid, use its `ReadHandle` to access it for read-only purpose and use `WriteHandle` to access it for read & write purpose. This guarantees that during the lifetime of the `ReadHandle` / `WriteHandle`, its item will never be evicted or reclaimed by any other thread.

However, `ReadHandle` / `WriteHandle` does NOT synchronize between read and write access to the item's memory. They're only used to indicate to CacheLib if an access is intended to be read-only or read/write. To properly synchronize between concurrent reads and writes to `Item::getMemory()`, user must implement their own synchronization on top. (e.g. use a SharedMutex to synchronize reads and writes).

To ensure consistency of data across HybridCache (Ram & NVM), we need to know whether user is performing a read or a write. For example, requesting a `WriteHandle` via `findToWrite()` API is more expensive than requesting a `ReadHandle` via `find()` API in the context of NvmCache as it incurs an invalidation call to this item's copy in NvmCache.

An item's handle also provides future semantics offered in HybridCache. Calling `toSemiFuture()` via a `ReadHandle` or a `WriteHandle` will return `folly::SemiFuture<ReadHandleImpl>`. For more information, see [Hybrid Cache](HybridCache).

For more details about `ReadHandle` and `WriteHandle`, see allocator/Handle.h.

## Item memory overhead

When you call the `allocate()` method to allocate memory from cache for an item, cachelib allocates extra 32 bytes (overhead) for the item's metadata, which is used to manage the item's lifetime and other aspects. For example, cachelib stores a refcount, pointer hooks to the intrusive data structures for cache like hash table, LRU, creation time, and expiration time. Some of these are for internal book keeping; and others are accessible through the item's public API. For details, see allocator/CacheItem.h.

## Handle lifetime

Like a `std::shared_ptr`, a "handle" (i.e. `ReadHandle` / `WriteHandle`)'s lifetime is independent from the other instances of "handle" that points to the same item. Holding a "handle" guarantees that the item it points to is alive at least as long as this "handle" instance is alive. The next section describes what *at least* means. A "handle" is only *movable*.

## Item lifetime

Items in cache can be evicted to make space for other new items. For any item, having its outstanding "handle" prevents us from evicting the item or release its memory due to slab release.

An item `x` without outstanding handles is destroyed immediately when you explicitly call `remove()` to remove it or call `insertOrReplace()` to replace it with another item having the same key as `x`'s. With outstanding handles, the item's memory is guaranteed to not be reclaimed until the last outstanding handle is dropped. It is similar to use a `shared_ptr` to ensure that the underlying object is not destroyed until the last reference goes out of scope.

See the following state diagram for the state of a cachelib item when we're using the HybridCache setup (ram + flash).
![](item_state.png)
