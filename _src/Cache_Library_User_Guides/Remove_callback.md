---
id: Remove_callback
title: Remove callback
---

Use [Item Destructor](Item_Destructor) whenever is possible, contact us if it
doesn't satisfy your requirement and RemoveCallback has to be used.


*Remove callback* provides destructor semantics for an item in the cache. This is useful when you want to execute some logic on removal of an item from the cache. When you use cachelib APIs to concurrently allocate memory from the cache for an item, insert an item into the cache, or remove an item from the cache, the item's lifetime ends when the item is evicted or removed from the cache and the last handle held by all sources drops. *Remove callback* provides you an ability to capture this and take some appropriate action if needed.

For example, suppose you want to maintain a counter for the total number of items in your cache and increment the counter when you call the `insertOrReplace()` method. The item you inserted could be evicted or removed from the cache when you again call `insertOrReplace()` or `allocate()`. To decrement the counter when the item you inserted is evicted or removed by another thread, you can have your logic encapsulated as *remove callback*.

*Remove callback* takes the following signature and can be provided in the config for initializing the cache:


```cpp
auto removeCB = [&](const Allocator::RemoveCbData& data) { --totalItems; };
config.setRemoveCallback(removeCB);

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


## RemoveCBData

*Remove callback* gets called with the following pieces of information:


```cpp
// Holds information about removal, used in RemoveCb
struct RemoveCbData {
  // Remove or eviction
  RemoveContext context;

  // Item about to be freed back to allocator
  Item& item;

  // Iterator range pointing to chained allocs associated with items
  folly::Range<ChainedItemIter> chainedAllocs;
};
```


* `context` This refers to the context of removal. `RemoveCB` can be called [invoked] on an item when it is explicitly removed by the user through the `remove()` API or when it is replacing an old item through the `insertOrReplace()` API, or when it being evicted to make room for a new item. For the first two calls on `RemoveCB`, the context is `kRemoval`; and for eviction, the context is `kEviction`.
* `item` Reference to the item that is being destroyed. Modifying the item at this point is pointless because this is the last handle to the item and the memory will be recycled after the call to the remove callback.
* `chainedAllocs` This provides a reference to the list of chained items associated with the given item if they exist. For details on what chained allocations are, see [visit data in cache](Visit_data_in_cache).

## Guarantees

Cachelib guarantees the following for remove callback executions:

* The callback will be executed *exactly once* when the last handle for the item goes out of scope and the item is no longer accessible through the cache upon calling `remove()` or `insertOrReplace()` causing a replace.
* The callback will be executed for any item that is evicted from cache.
* When the callback is executed, there can be no other future or concurrent accessors to the item.
* The callback will *not* be executed if you allocate memory for an item and don't insert the item into the cache.
* The callback will *not* be executed when items are moved internally.

Note that remove callback is executed per item, not per key. For example, if you already have an item in cache and call `insertOrReplace()` to replace it with another item with same key, cachelib will execute remove callback for the replaced item.

## NvmCache

Currently remove callback is not supported seamlessly when NvmCache is enabled. This will be addressed and available in the near future.
