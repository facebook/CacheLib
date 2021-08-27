---
id: terms
title: terms
---

**Item**


An item is an object stored in cache. In addition to storing the payload (the
actual data), it also stores metadata: intrusive hooks, reference count, flags,
creation time, and expiration time. An item is assigned a `key` that
cachelib API uses to find the item. For more information, see
[Items and ItemHandle](Item_and_ItemHandle/).


**ItemHandle**


An ItemHandle is similar to a `std::shared_ptr<Item>`. It is used to
access an item. For more information, see [Item and
ItemHandle](Item_and_ItemHandle/ ).


**Chained item**


An item has fixed memory size in cache. To grow the item, add chained items to it. A chained item doesn't have a key; thus you must use its parent item to access it. For more information, see [Chained Items](chained_items/ ).



**Key**



A key is a byte-array identifier assigned to a cached item. Cachelib uses it to find the item. A key can be a string or a POD (memcpy-safe).


**Allocation size**


This is the size (in number of bytes) of all the content associated with an item: header, key, and value.


**Allocation class**


Each allocation class represents a specific allocation size. Items, within the same memory pool, of the same allocation size come from the same allocation class. Each allocation class can take a number of slabs and give out allocations from the slabs (like a classic slab allocator).


**Slab**


A slab is a logical unit of a fixed chunk of memory; it is hardcoded to 4 MB currently. Cachelib separates the whole cacheable memory into a number of slabs. Memory allocations for each item are carved out of slabs.


**Compact cache**


A compact cache is used to store small key-value data, usually less than tens of bytes per entry. The size of the key must be fixed at compile time, whereas the size of the value can be fixed, variable, or empty. For more information, see [Compact Cache](compact_cache/).




**Pool**


A pool is a block of memory of a specific size in a cache. Pools enable you to separate different objects in the cache; that is, objects in one pool are isolated from objects in the other. In addition, they improve hit ratio. For more information, see [Partitioning cache into pools](Partition_cache_into_pools/ ).


**Pool rebalance**


When you cache objects of variable sizes, over time, the cache memory is fragmented. To fix this, enable pool reblancing. For more information, see [Pool Rebalance Strategy](pool_rebalance_strategy/ ).


**Eviction policy**

* **LRU**
The least recently used (LRU) eviction policy ensures that the least recently used item is evicted first. For more information, see [Eviction Policy](eviction_policy/ ).
* **LRU 2Q**
LRU 2Q uses three queues to manage hot, warm, and cold items. If an item in the cold queue is accessed, it gets promoted to the warm queue. An item evicted from the cold queue gets evicted from the cache. For more information, see [Eviction Policy](eviction_policy/ ).
* **TinyLFU**
TinyLFU consists of a frequency estimator (FE) and LRU. FE is an approximate data structure that computes an item's access frequency before it is inserted to LRU. Only items that pass the frequency threshold get accepted to LRU and evicted. For more information, see [Eviction Policy](eviction_policy/ ).

**Compact cache**


A compact cache is used to store small key-value data, usually less than tens of bytes per entry. The size of the key must be fixed at compile time, whereas the size of the value can be fixed, variable, or empty. For more information, see [Compact Cache](compact_cache/ ).


**HybridCache**


HybridCache is non-volatile memory (NVM) cache (flash). When you use HybridCache, items allocated in cache can live on NVM or DRAM. For more information, see [HybridCache](HybridCache/ ).



**Item lifetime**

Time in cache since the creation of the *Item* up until eviction of the *Item* from the cache.


**Eviction Age**

Time in cache for an Item  from it's last access time up to its eviction.
