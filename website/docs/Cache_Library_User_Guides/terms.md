---
id: terms
title: Terms
---

**Item**
An item is an object stored in cache. In addition to storing the payload (the
actual data), it also stores metadata: intrusive hooks, reference count, flags,
creation time, and expiration time. An item is assigned a `key` that
cachelib API uses to find the item. For more information, see
[Items and Handle](Item_and_Handle).


**WriteHandle and ReadHandle**
A WriteHandle(fka ItemHandle) is similar to a `std::shared_ptr<Item>`. It is used to
access a mutable item.
A ReadHandle is similar to a `std::shared_ptr<const Item>`. It is used to access a read-only item.
For more information, see [Item and Handle](Item_and_Handle).


**Chained item**
An item has fixed memory size in cache. To grow the item, add chained items to it. A chained item doesn't have a key; thus you must use its parent item to access it. For more information, see [Chained Items](chained_items).



**Key**
A key is a byte-array identifier assigned to a cached item. Cachelib uses it to find the item. A key can be a string or a POD (memcpy-safe).


**Allocation size**
This is the size (in number of bytes) of all the content associated with an item: header, key, and value.


**Allocation class**
Each allocation class represents a specific allocation size. Items, within the same memory pool, of the same allocation size come from the same allocation class. Each allocation class can take a number of slabs and give out allocations from the slabs (like a classic slab allocator).


**Slab**
A slab is a logical unit of a fixed chunk of DRAM memory; it is hardcoded to 4 MB currently. Cachelib separates the whole cacheable memory into a number of slabs. Memory allocations for `Item` is carved out of slabs.


**Pool**
A pool is a block of memory of a specific size in a cache. Pools enable you to separate different objects in the cache; that is, objects in one pool are isolated from objects in the other. In addition, they improve hit ratio. For more information, see [Partitioning cache into pools](Partition_cache_into_pools).


**Pool rebalance**
When you cache objects of variable sizes, over time, the cache memory is fragmented. To fix this, enable pool reblancing. For more information, see [Pool Rebalance Strategy](pool_rebalance_strategy).

**Eviction**
Eviction refers an item exiting from cache because the cache storage is full
or because the item is no longer relevant to be cached. Evictions are
triggerred internally by CacheLib and are controlled by either the `Eviction
Policy` (see [Eviction Policy](eviction_policy)) or the `Admission Policy`  of
the cache depending on the setup. An `Item` can also be evicted from
the cache in response to certain internal events like Pool rebalancing.

**Removal**
Removal refers to an item exiting the cache when the `remove` api is invoked.

**Expiry**
Expiry refers to an item exiting the cache when it's time to live (TTL) is
exhausted. By default, item's don't expire unless a TTL is explicitly set.


**Item lifetime**
Time in cache since the creation of the `Item` up until eviction of the `Item` from the cache.


**Eviction Age**
Time in cache for an Item  from it's last access time up to its eviction.

**HybridCache**
HybridCache is non-volatile memory (NVM) cache (flash). When you use HybridCache, items allocated in cache can live on NVM or DRAM. For more information, see [HybridCache](HybridCache).

**Compact cache**
A compact cache is used to store small key-value data, usually less than tens of bytes per entry. The size of the key must be fixed at compile time, whereas the size of the value can be fixed, variable, or empty. For more information, see [Compact Cache](compact_cache).
