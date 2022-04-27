---
id: Item_Destructor
title: Item Destructor
---
## Introduction

*Item Destructor* provides destructor semantics for an item in the cache. This
is useful when you want to execute some logic on removal of an item from the
cache. When you use cachelib APIs to concurrently allocate memory from the
cache for an item, insert an item into the cache, or remove an item from the
cache, the item's lifetime ends when the item is evicted or removed from the
cache and the last handle held by all sources drops. *Item Destructor* provides
you an ability to capture this and take some appropriate action if needed.



*Item Destructor* is an extension to
[Remove callback](Remove_callback), it guarantees
that the destructor is executed **once and only once** when the item is dropped
from the cache, includes **both DRAM and NVM**. If Nvm Cache is not enabled,
*Item Destructor* does the exact same behavior as *Remove Callback*. These two
features can't be used at the same time.

## Example

For example, suppose you want to maintain a counter for the total number of
items in your cache and increment the counter when you call the
`insertOrReplace()` method. The item you inserted could be evicted or removed
from the cache when you again call `insertOrReplace()` or `allocate()`. To
decrement the counter when the item you inserted is evicted or removed by
another thread, you can have your logic encapsulated as i*tem destructor* .

*Item Destructor* takes the following signature and can be provided in the
config for initializing the cache:

```cpp
auto itemDestructor = [&](const Allocator::DestructorData& data) { --totalItems; };
config.setItemDestructor(itemDestructor);
// Adds an item to cache and increment the counter.
void addToCache(std::string key, size_t val) {
  auto handle = cache.allocate(keys[i], 100); // allocate an item
  cache.insertOrReplace(handle); // insert into cache.
  ++totalItems;
}
// Suppose your cache can contain at most 5 items and
// it evicts beyond that.
for (int i = 0; i < 1000; i++) {
  addToCache(std::to_string(i), 100);
}
std::cout << totalItems << std::endl; // this will print 5.
```

## DestructorData

*Item Destructor* gets called with the following pieces of information:

```cpp
// used by ItemDestructor, indicating how the item is destructed
enum class DestructorContext {
  // item was in dram and evicted from dram. it could have
  // been present in nvm as well.
  kEvictedFromRAM,

  // item was only in nvm and evicted from nvm
  kEvictedFromNVM,

  // item was present in dram and removed by user calling
  // remove()/insertOrReplace(), or removed due to expired.
  // it could have been present in nvm as well.
  kRemovedFromRAM,

  // item was present only in nvm and removed by user calling
  // remove()/insertOrReplace().
  kRemovedFromNVM
};

struct DestructorData {
  // remove or eviction
  DestructorContext context;

  // item about to be freed back to allocator
  // when the item is evicted/removed from NVM, the item is created on the
  // heap, functions (e.g. CacheAllocator::getAllocInfo) that assumes item is
  // located in cache slab doesn't work in such case.
  // chained items must be iterated though @chainedAllocs
  // internal APIs getNext and getParentItem are broken for
  // items destructed from NVM.
  Item& item;

  // Iterator range pointing to chained allocs associated with @item
  // when chained items are evicted/removed from NVM, items are created on the
  // heap, functions (e.g. CacheAllocator::getAllocInfo) that assumes items
  // are located in cache slab doesn't work in such case.
  folly::Range<ChainedItemIter> chainedAllocs;
};
```

* `context`
This refers to the context of removal. `ItemDestructor` can be called [invoked]
on an item when it is explicitly removed by the user through the `remove()` API
or when it is replacing an old item through the `insertOrReplace()` API, or
when it being evicted to make room for a new item. For the first two calls on
`ItemDestructor`, the context is `kRemovedFromRAM`or `kRemovedFromNVM` depends
on the last location of the item; and for eviction, the context is
`kEvictedFromRAM` or `kEvictedFromNVM`.

* `item` Reference to the item that is being
destroyed. Modifying the item at this point is pointless because this is the
last handle to the item and the memory will be recycled after the call to the
destructor.When the item is evicted/removed from NVM, the item is created on
the heap, functions (e.g. CacheAllocator::getAllocInfo) that assumes item is
located in cache slab doesn't work in such case.

* `chainedAllocs` This provides a reference to
the list of chained items associated with the given item if they exist. For
details on what chained allocations are, see
[[Cache_Library_User_Guides/Visit_data_in_cache/ | visit data in
cache]].Chained items must be iterated though `chainedAllocs`, internal APIs
`getNext` and `getParentItem` are broken for items destructed from NVM.

## Guarantees

Cachelib guarantees the following for *Item Destructor* executions:

* The destructor will be executed *exactly once* when the last handle for the
item goes out of scope and the item is no longer accessible through the cache
upon calling `remove()` or `insertOrReplace()` causing a replacement.
* The destructor will be executed for any item that is evicted from cache.
* When the destructor is executed, there can be no other future or concurrent accessors to the item.
* The destructor will *not* be executed when item is in-place mutated,
including add/pop chained items, and value mutation via `getMemory()`.
* The destructor will *not* be executed if you allocate memory for an item and
don't insert the item into the cache.
* The destructor will *not* be executed when items are moved internally.

Note that *Item Destructor* is executed per item, not per key. For example, if
you already have an item in cache and call `insertOrReplace()` to replace it
with another item with same key (even same value), cachelib will execute the
destructor for the replaced item.

## Performance Impact

1. One lock is added to protect against race conditions for concurrent
operations, but the lock is sharded and performance impact is minimal.
2. Additional flash read is done in BlockCache in order to execute the
destructor, it will be retried if the region is being reclaimed, and fail to
read (e.g. io error) will disable Nvm Cache.

## Migrate from RemoveCallback

The migration is very simple:

1. `config.setRemoveCallback` -> `config.setItemDestructor`
2. Update your callback parameter `RemoveCbData` -> `DestructorData`, and the context.
3. Make sure chained items are iterated though `chainedAllocs`.

The impact to you:

* If you only use DRAM cache, no impact, both functionality and performance are same.
* If you're using hybrid cache, the callback will only be triggered when
item kicked off from the whole cache, and there is some performance impact but
it should be minimal (see 'Performance Impact' section for
details).

## **Known Issue**

We try our best to guarantee that the destructor is triggered once and only
once for each item, but there is one scenario which the destructor would not be
executed: if we get a reference overflow exception when an item is evicted or
removed from NvmCache. This scenario should happen very rare,
`nvm.destructors.refcount_overflow` stat can be used to track if this has ever
happened.
