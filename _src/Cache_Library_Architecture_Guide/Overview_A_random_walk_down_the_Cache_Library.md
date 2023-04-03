---
id: overview_a_random_walk
title: "Overview: A random walk down the Cache Library"
---

This guide is intended for people without background knowledge in CacheLib who
are interested in how CacheLib works. It contains links to code and other guides
of CacheLib but it should be selfcontained without referring to any of those.

## So what is CacheLib?

CacheLib is a library that is used for caching locally.

- It's for caching. It means (a lot of things but people tend to forget) that data put into a cache may not be present when accessed and it is **not** persistent (not a storage).
- It is a local library. It means CacheLib does not deal with many classical challenges in caching that involves multiple hosts like consistency, sharding, etc.

Let's put this into more context. Imagine you are an engineer who is looking to switch from a multi-gigabyte evictable hash map to CacheLib. You can quickly set it up and use it similar to using a map. Follow this guide and set up your own cache. ([Setup a simple cache](/docs/Cache_Library_User_Guides/Set_up_a_simple_cache ))



The way to interact with CacheLib's "hashmap" is via Item and Item's handle, which are basically the raw memory of the content you cached. Logically, an Item is an entry in the map. The "handle" of the item is a wrapper for lifetime management. ([Item and Handle](/docs/Cache_Library_User_Guides/Item_and_Handle/ ))


Just like all the caches, there are many knobs you can tune. They are all on the left side panel of the [User Guide page](/docs/Cache_Library_User_Guides/About_CacheLib). It's normal that you don't understand all of them right now, but you will at the end of this guide. Below are some common examples of the knobs.

* Eviction policy in memory: We support three flavors of evictions in DRAM: LRU (with custom insert point) , a variation of 2Q (hot, warm, cold queues), LFU (LRU + frequency estimator). They are all LRUs with variations, trying to achieve better hit rate with different attempts. ([DRAM eviction policy](/docs/Cache_Library_User_Guides/eviction_policy/ ))
* Remove callback/Item desctructor: A callback that's called when the item is removed from RAM (remove callback) or from the cache (item destructor). Typical intended use case is to do book-keeping of items when they are destroyed (for example, counters). ([Remove callback](/docs/Cache_Library_User_Guides/Remove_callback/), [Item Destructor](/docs/Cache_Library_User_Guides/Item_Destructor/))
* Persistence between restarts: ([Cache Persistence](/docs/Cache_Library_User_Guides/Cache_persistence/))
   * You can avoid dropping the cache between restarts by this feature, however
   * this "persistence" only works if the process shuts down normally. Cachelib is not a persistent storage engine and the cache content will not survive power failure.
* Pools: You may understand one "pool" as a different cache: it can have different eviction policy and allocation size but the pools can rebalance their resource. ([Cache Pools](/docs/Cache_Library_User_Guides/Partition_cache_into_pools/))

Now you become more interested and are curious about how these are implemented.

## Overview

CacheLib has two levels of caches: DRAM and flash. Logically, each of them is further divided into two types of caches for items smaller or larger than a certain threshold.

There will be a section discussing each of the bullets below.

* DRAM refers to **d**ynamic-**r**andom **a**ccess **m**emory. There is also SRAM (static). Typically DRAM is the main memory (the RAM you think of when the word RAM comes to your mind) and SRAM is used in processor cache. CacheLib works on DRAM, not SRAM.
* The official name for "flash" here is NVM ([**n**on-**v**olatile **m**emory](https://en.wikipedia.org/wiki/Non-volatile_memory)). Since we have another library component called NvmCache, we would use the word "flash" to refer to the library we build on top of NVM device as flash is a common example.
* The cache for small items in DRAM is CompactCache.
  * This component is getting deprecated soon.
* The cache for large/regular items in DRAM - it doesn't have a name, but it is the main subject that many client facing knobs are talking about (eviction policy, remove callback only refer to this part of the cache).
* The cache for small items in flash is called BigHash.
* The cache for large items in flash is called BlockCache.
* There combination of BigHash and BlockCache (and some other components to stitch them together) is called Navy (**n**on-**v**olatile memory cache).
* The coordination layer between DRAM cache and Navy is NvmCache.




## DRAM regular cache

* All the memory (including that assigned to small item cache, details later) is first divided into pools. Each pool has a number of allocation classes (logically).
* Pool is defined by user, usually based on the purpose. For example in a cache use case for a social graph, the cache can be divided into two pools: one for the edges and one for the nodes.
* An allocation class is saying that all the items in the class have the same size allocated. This reduces fragmentation and helps with pointer compression with an index offset in the slab.
* A slab is the physical continuous unit of memory. Each slab is assigned to one allocation class in one particular pool, or unassigned. Each allocation class have a number of slabs. The slabs belonging to the same allocation class do not have to be next to each other physically.
* Indexing: When the client wants to access an item, a hash table (`allocator/ChainedHashTable.h?lines=26`) is looked up. This hash table is from the cache's key to the cache item's compressed pointer. The compressed pointer is the slab index plus the item index within that slab. ([Indexing](/docs/Cache_Library_Architecture_Guide/RAM_cache_indexing_and_eviction/#accesscontainer))
   * This component is called AccessContainer. ChainedHashTable is one type of such AccessContainer, called an AccessType.
   * From the slab index, you can retrieve the slab. In the slab header, it contains what allocation class it is assigned to. And from there you can calculate the exact address from the item index within the slab.
   * We have one single AccessContainer for the entire DRAM cache. So when quering, you don't need to know the pool, size (allocation class), the cache will return all that for you if it is a hit.
* Eviction: The number of LRUs mentioned in the user guide are implemented. ([Eviction](/docs/Cache_Library_Architecture_Guide/RAM_cache_indexing_and_eviction/#mmcontainer))
   * LRU: `allocator/MMLru.h`; LRU2Q: `allocator/MM2Q.h`; TinyLFU: `allocator/MMTinyLFU.h`
   * The eviction queues are implemented via a doubly linked list.
   * This component is formally known as MMContainer. Each of these LRUs is one MMType.
   * Eviction happens on cache item level. Each allocation class has its own eviction queue.
* You can mix and match different AccessType and MMType to form a cache that you want, since they are independent. A combination of AccessType and MMType is a CacheTrait.
   * Today CacheLib officially supports only ChainedHashTable as AccessType. But you can prototype and create your own.
   * These are all the CacheTraits that we support: `allocator/CacheTraits.h`
* Refcount: When client lookup the cache, a pointer is given back. So we keep a reference count on the item.
   * Refcount contains 16 bits and it has some reserved bits for special signals. So CompactCache (more details later, but is being deprecated) is trying to make a tradeoff by dropping the refcount for less space overhead and using memcpy.
* Allocator: A cache allocator is in charge of allocating the memory for the item. Since CacheLib already have the memory from the OS, the word allocate here means identifying a piece of memory on an appropriate slab, so that it can be safely returned to the client. The allocator tries to find a free piece of memory by the below steps:
   * The allocator keeps a queue of free allocations. These allocations are items that are freed (refcount=0, and some other conditions). (`allocator/memory/AllocationClass.cpp?lines=157`)
   * In case that the cache pool/allocation class is not full yet, the allocator may have a slab that it's currently holding and carving into allocation. It uses this slab if it's available. (`allocator/memory/AllocationClass.cpp?lines=166`)
   * In case that the cache pool/allocation class is not full, the allocator may have a list of empty slabs that it can use. If there is no current slab that it is working on from the previous case, it tries to get a slab from the free slabs. (`allocator/memory/AllocationClass.cpp?lines=172`) If there is no free slab, the allocation class turns to the memory pool for a slab.
   * The memory pool keeps a list of free slabs. If the list isn't empty, the memory pool assigns a slab to the allocation class. The allocation class will use this slab for the new allocation. If there is no free slab on the memory pool, it turns to the slab allocator for a slab.
   * The slab allocator is responsible of carving the large memory space managed by CacheLib into slabs. It serves all the memory pools. Similarly, it keeps a list of available slabs that are not assigned to any memory pools yet and assigns from this list first. If there is no free slab, it tries to create a new slab, if not all the memory has been utilized. If there is not more free memory, the allocation fails. The allocation class returns a failure.
   * Otherwise, the cache allocator tries to force an eviction, in hope of freeing an allocation like in the first case.  (`allocator/CacheAllocator-inl.h?lines=370`). The eviction starts from the end of the eviction queue and tries to identify an item that is eligible for eviction (refcount==0 and some other conditions) and it will continue a finite number of iterations.
   * If all these fails, the allocation fails and returns to the client.
* Intrusive containers: MMContainer and AccessContainer are intrusive containers with minimum memory overhead. Please skip the following bullet points if you know what intrusive containers are.
   * Intrusive containers refer to containers that store pointers instead copies of its elements. These containers interact with their elements via intrusive hooks. The hook is typically a function (powered by a class member) of the element type.
   * Linked list is an example of intrusive container. The "hook" in this case is the `item* next()` function. To support this, elements of linked list need to have a class member `item* next`. As a result, the container itself has very minimum overhead: it only holds a head pointer.
   * To put this into example: Let's say you


## DRAM CompactCache

[Compact cache guide](/docs/Cache_Library_User_Guides/compact_cache/)
* This component is being deprecated.
* CompactCache is designed to have little overhead per item. Each of the slab managed by CompactCache is further divided into buckets. Each bucket is designed to store 8 items.
* Indexing: Given a key, hash it and modular to find the slab, modular again to find the bucket. Then iterate through the possibly 8 items to compare.
* Eviction: When an insert happens on a full bucket, LRU happens on that bucket by memcpy and memmove with a constant overhead.
* Memory saving: No refcount; No hooks for containers.
* Disadvantages: LRU happens within the bucket so hotspot may occur; No refcount, so lookup is a memcpy, which makes the cost grows with the item size; DRAM only, not backed by flash;
* Interaction with CompactCache is done through a separate API. The client application would need to keep an extra reference other than the CacheAllocator interface and use that directly. A CompactCache is a separate pool in the CacheLib cache.
* We decided to deprecated CompactCache because the benifit of saving overhead by a separate design does not exceed the complexity and the efficiency burden brought to the system. We will focus on reducing item level overhead as an optimization for smaller item instead of having a separate cache for them.

## DRAM quick recap

* Insert:
   * For CompactCache: Hash and modular the key to identify a slab. Insert to the head of LRU.
   * For regular cache: The insert request comes with a pool and allocation size. Find the allocation class and try to allocate memory with the following attempts: free allocations --> free slabs (allocation class -> memory pool -> slab allocator) --> eviction. Then the client copies the content into the allocated address. The cache allocator then makes the item available in the access container and insert it to MM container.
* find:
   * For CompactCache: Similar to insert, hash and modular the key to identify a slab and promote the item.
   * For regular cache: Find the item in the chained hash map. From the item, get the slab it lives on and form the slab, identify the allocation class and promote the item on that particular LRU queue. Increment the refcount and return the item handle.
## Flash overview

Flash is organized in a similar way: there is a cache for smaller items (BigHash) and for larger item (Block Cache). Unlike DRAM, the client does not get to choose where the item goes. It's done automatically thresholding the size. Together, this constitutes [Navy](/docs/Cache_Library_Architecture_Guide/Navy_Overview ) -- the flash cache engine of CacheLib.

* "block device" refers to devices that's read/write happen with a fixed size block (if it helps, substitute the word "page" here). It means you can't write with precision of bytes but have to incur overhead if you don't write an entire block.

In the later sections, we will go over the design of Navy in the reverse order: starting from how small and large items are cached and back to how an item goes from DRAM into Navy.



## BigHash: the small item engine
[BigHash Page](/docs/Cache_Library_Architecture_Guide/Small_Object_Cache )

The space managed by BigHash is divided into buckets.

* Given a key, hash it and modular to find the bucket for this key. Then go iterate in the items in the bucket to perform read/write. You have the option to use a bloom filter in memory for negative cache. This design is similar to CompactCache.
* Eviction: Each bucket has a FIFO queue for the items in it.

## BlockCache: the large item engine
[BlockCache Page](/docs/Cache_Library_Architecture_Guide/Large_Object_Cache )

The space managed by BlockCache is divided into regions of the same size (default 16MB, max 256MB).

* Writes: There is an in memory buffer. Once the buffer is full, it gets flushed into a region on the flash storage. If it helps you understand, this is like the LSM tree in storage engine, except there won't be "merging" later.
* Reads: Each item is stored in a B tree index in memory. When a read is performed, the region that item is in is promoted in the FIFO queue in memory. Unlike DRAM where each allocation class has its own queue, we have one single queue for all the regions across all size classes.
* Evictions/Removal: Evictions happens for the entire region. There are three eviction policies: LRU, FIFO and segmented FIFO. In case of LRU, promotion happens at the region level as well, which means if an item is accessed, the entire region is promoted. Item removal issued from the client would result in the item key being removed from the index tree and the reclamation of the space happens with the region eviction.
* There are some similarities between BlockCache and the regular DRAM cache:
   * A region is similar to a slab in DRAM. It is a fixed size piece of space/memory where all items are of the same size. The difference is that for DRAM, item is the unit of eviction but for flash it's the entire region.
   * Size classes are similar to allocation classes.

## Driver

The component that decides which cache engine to go, and part of ordering.

* Driver interacts with the upstream component - NvmCache, which we would talk about later.
* Admission policy: There is a boolean function that controls whether Navy accepts a write. The reason for rejecting is for device burn/ iops/ hit rate.
* Job scheduler: A request gets enqueued to one of IO thread. Navy does not concern ordering. At any particular time there can be only one job per item in the queuing system.

## NvmCache: The interface between DRAM and Navy

* We needed this interface between RAM and flash, so that we can switch the flash engine from (our clients') old implementation to Navy.
* Functionally, it coordinates between anything that happens between DRAM and flash. Examples:
   * Cancels requests if concurrent write comes in. This together with the job scheduler guarantees we don't write stale data into Navy.
   * Reinserts items fetched from flash into RAM, and return the item handle to client.
      * When an item is reinserted to RAM, the copy in flash is not evicted directly. The item in RAM would be marked as NVM_CLEAN, meaning that this item has a copy in Nvm (flash). So when the item in RAM is evicted again, it will not be considered for insertion into NVM since the NVM copy is still thre. When the item in flash is getting evicted with a region, there is a callback that sets the RAM item as NVM_DIRTY, if it exists.
* When an item is evicted from DRAM, an optional NvmAdmissionPolicy is called before inserting into NvmCache. This admission policy, separated from the Driver's admission policy, can be set by the client with application level logic to reject an admission for endurance and performance reason.



## Flash recap: Insert (evicting from DRAM into flash)

* When an Item is evicted from DRAM, item is tested by NvmAdmissionPolicy, then Drive's admission policy. If passed,
* NvmCache schedules the insertion with Navy driver.
* The insertion gets picked up from the job queue.
* The item is inserted into BigHash if it is small:
   * Compute hash. Find the bucket. Insert. Evict the last item in the bucket if necessary. Update the bloom filter.
* The item is inserted into BlockCache if it is large:
   * Find its allocation size's active region. Write to the region and promote the region. Update the in-memory index map. Evict a region if there's no more region available.

## Flash recap: Find (looking up from flash and reinserting to DRAM)

* The client asks for a key and it gets a miss in DRAM and now is going to NVM.
* NvmCache schedules the read and cancels all existing writes from Navy driver.
* The read gets picked up from the job queue.
* Look at BlockCache's in memory index:
   * We look at BlockCache first because it is indexed better.
   * If it's a hit, read the item into RAM and reinsert it to DRAM cache. Mark the NVM_CLEAN flag and return the in memory ItemHandle.
* If the previous step missed, look at BigHash:
   * Hash the key and find the bucket. If the bloom filter indicates a hit, iterate through the bucket and find the item.
   * Similar process follows if it's a hit.
* If an item is found, insert the item into DRAM and return the item in DRAM to client.

## Recap: what are those knobs again?

That's most of the skeleton of CacheLib RAM / Flash stack. Let's go back to the user guide and see what exactly each knob is about.

* EvictionPolicy: This is in regard of the DRAM cache only. You can't change the eviction policy for CompactCache and BigHash. BlockCache's eviction policy is configured via another way. LRU, LFU, 2Q translates to the three MMType in `allocator/CacheTraits.h`. ([Eviction Policy](/docs/Cache_Library_User_Guides/eviction_policy))
* Remove callback: This is called when an item leaves the DRAM cache. It does not indicate whether the item is actually leaving the cache since it might get admitted into flash. And the callback for removing from the entire cache is Item Destructor. ([Remove callback](/docs/Cache_Library_User_Guides/Remove_callback/), [Item Destructor](/docs/Cache_Library_User_Guides/Item_Destructor/))
* Persistence between restarts: This is self-explanatory. Please remember it requires a proper shutdown to be called.
* Pools: This refers to the DRAM cache only. ([Cache Pool](/docs/Cache_Library_User_Guides/Partition_cache_into_pools/))
* TTL: This applies across DRAM and flash. If an item in flash passes TTL, it won't get reinserted to RAM. But it also won't get evicted directly. Eviction happens with the region. ([Time-to-live](/docs/Cache_Library_User_Guides/ttl_reaper/))
* Configurable hash table: This is for DRAM only as parameters of ChainedHashTable. ([Configure Hashtable](/docs/Cache_Library_User_Guides/Configure_HashTable/))

Hopefully now you have a high level picture of the CacheLib cache!





## Things we skipped (most of them only has client guide but not a deep dive):

* Chained items: What if you have item that can grow its size? [Chained Items](/docs/Cache_Library_User_Guides/chained_items/ )
* Memory advising: How the memory management interacts with the kernel. [OOM Protection](/docs/Cache_Library_User_Guides/oom_protection/ )
