---
id: HybridCache
title: HybridCache
---

HybridCache feature enables `CacheAllocator` to extend the DRAM cache to NVM. With HybridCache, cachelib can seamlessly move Items stored in cache across DRAM and NVM as they are accessed. Using HybridCache, you can shrink your DRAM footprint of the cache and replace it with NVM like Flash. This can also enable you to achieve large cache capacities for the same or relatively lower power and dollar cost.

## Design

When you use HybridCache, items allocated in the cache can live on NVM or DRAM based on how they are accessed. Irrespective of where they are, **when you access them, you always get them to be in DRAM**.

Items start their lifetime on DRAM when you `allocate()`. As an item becomes cold it gets evicted from DRAM when the cache is full. `CacheAllocator` spills it to a cache on the NVM device. Upon subsequent access through `find()`, if the item is not in DRAM, cachelib looks it up in the HybridCache and if found, moves it to DRAM. When the HybridCache gets filled up, subsequent insertions into the HybridCache from DRAM  will throw away colder items from HybridCache. `CacheAllocator` manages the synchronization of the lifetime of an item across RAM and NVM for all cachelib APIs like`insertOrReplace()`, `remove()`, `allocate()`, and `find()`. Conceptually you can imagine your cache to span across DRAM and NVM as one big cache similar to how an OS would use virtual memory to extend your main memory using additional SWAP space.

## Using HybridCache

The following sections describe how to configure HybridCache, how to allocate memory from the cache to store items, and how to access the items on the cache.

### Configuration

To use HybridCache, you need to set the appropriate options in the cache's config under the `nvmConfig` section. Refer to [Configure HybridCache](Configure_HybridCache) for a list of options to configure for the HybridCache's backing engine.

For example:


```cpp
nvmConfig.navyConfig.set<SETTING NAME> = <YOUR SETTING VALUE>;

// set the nvmConfig in the main cache config.
lruAllocatorConfig.enableNvmCache(nvmConfig);
```


After seting up a HybridCache, you can use the cache almost the same way as a pure DRAM based cache.

### Allocating items

Use the `allocate()` API to allocate memory for an item and get a `WriteHandle`:


```cpp
// Allocate memory for the item.
auto handle = cache.allocate(pool_id, "foobar", my_data.size());

// Initialize the item
std::memcpy(handle->getMemory(), my_data, my_data.size());

// Make the item accessible by inserting it into the cache.
cache.insertOrReplace(handle);
```


### Accessing items on cache

When you call the `find()` API to look up an item by it key, cachelib returns a handle as before. However, the handle might not be immediately *ready* to dereference and access the item's memory.  CacheLib will promote the item from NVM to DRAM and notify the handle to be ready. Note that `Handle` will always eventually become ready. Cachelib provides the following `Handle`  states and corresponding APIs to distinguish the semantics

1. **Ready**
This indicates that the item is in DRAM and accessing the Item's memory through the `Handle` will not block.
2. **Not Ready**
This indicates that the handle is still waiting for the item to be pulled into DRAM. Accessing the item through `Handle` will block.

To check whether a Handle is ready, call the `isReady()` method via the Handle.

If the application can tolerate the latency of accessing NVM for some of your accesses from cache, there is not a lot of change that is needed on how you use cachelib today.

However, if you are impacted by latency, you can do the following:
1. Get a `folly::SemiFuture` on the handle by calling the `toSemiFuture()` method.
2. You may attach any additional logic to be executed on the item via SemiFuture's `defer()` callback.
3. Pass the SemiFuture to an execution context (e.g. folly's EventBase) that can then execute your code when the handle becomes ready.

The following code snippet highlights the various techniques.


```cpp
/* Accessing item on a fiber or in blocking way with NvmCache */
auto handle = cache.find("foobar");

/* This will block or switch your fiber if item is in NVM, until it is ready. */
if (handle) {
  std::cout << handle->getMemoryAs<const char*>(), handle->getSize();
}

/* Accessing item and getting a future */
auto handle = cache.find("foobar");
auto semiFuture = handle.toSemiFuture();

/* Attach (optional) callback that is invoked when semifuture is executed */
auto sf = std::move(semiFuture).deferValue([] (const auto itemHandle) {
  /* Do something with the item */
});

/* Schedule semi future to be executed async, when the item is ready */
std::move(semiFuture).via(
  folly::Executor::getKeepAliveToken(
    folly::EventBaseManager::get()->getExistingEventBase()));
```


## Semantics difference from pure DRAM cache

`HybridCache` is in active development. Not all DRAM cache features are correspondingly available in `HybridCache` setup. Some notable ones to consider :

- Pools only partition within DRAM, but not on HybridCache.  Items are sticky to their pools when they are moved to/from NVM.
- The `insert()` API cannot be used with HybridCache.
- Iteration of the cache only iterates over the DRAM part.
- When looking at the stats, some stats are broken down by DRAM and NVM. The semantics of getting a uniform stats across both mediums is currently not supported across all stats.
