---
id: ram_cache_design
title: "RAM Cache Design"
---

This is a high level document that explains the design of different components within RAM cache and how they fit together and work. This is a good place to learn about the language for terms in the code as well.

## Requirements for a cache

Some of the critical goals for a cache are the following and most of the design is centered around this.

- Must be restorable when we restart or deploy new binaries
- Support high rate of evictions in the order of hundred thousand per second and be able to linearly scale
- Very fast and efficient access based on a binary key
- Support for isolating workloads with different cache patterns
- Ability to detect change in workload and adapt, providing a good hit ratio over time.

## Non-Goals

- Native `std::` container or data type support. We expect clients of cache library to have a malloc like interface that gives back a piece of memory and do what they need with it. The users are free to implement complex data structures on top of this and cache library will provide necessary support as we see fit.
- Portability across wide variety of hardware architectures. Some of the decisions around alignment are specific to the processors that we run on in production. We assume the penalty for misaligned reads are non-existent in the hardware we use. There is a small penalty for unaligned atomics, which we are ready to sacrifice for better memory utilization.
- Optimizing for RSS. Typically memory allocators try to avoid defragmentation and keeping the working set size of the pages minimal. However, a memory allocator for a cache is designed to be such that there is not a lot of free memory from the MemoryAllocator perspective. Therefore, it is not effective to return or advise-away free pages for the optimization. Exception is when there are high memory pressure in the system in which case we are trying to release pages to prevent the OOM.

## Major Components

The components under cachelib(RAM part) can be broadly divided into these buckets.

1. **Shared Memory Management**: These provide ability to allocate memory in large chunks and be able to detach and re-attach to them across processes. We use these for most of our cache and storing metadata information as well. The code for this is under `cachelib/shm` directory and deals with different abstractions and implementations for shared memory.
2. **MemoryAllocator**: This component takes in a large chunk of memory and implements a malloc like interface on top of that. Unlike traditional memory allocators, this one is optimized for the goals listed above for building a multi-threaded cache. The memory allocator supports pools, custom allocation sizes to help with defragmentation, and does not care much about alignment for allocations. The memory allocator also supports persisting the state across binary restarts, by integrating with the shared memory components. The last and important feature in the memory allocator is support for dynamically resizing the distribution of memory across pools and across different allocation sizes within a pool. Such re-distributions are performed by the mechanism called [slab release](#slab-release).
3. **CacheAllocator**: This is the component that provides the final API for the cache. The CacheAllocator encapsulates the memory allocator and provides apis to allocate some memory with a binary key, fetch the memory corresponding to a key. It also supports pools, resizing and rebalancing the pools. The cache is thread safe for its apis, but the user has to explicitly manage the synchronization for any mutations to the memory allocated through the cache if required.

**GENERAL CACHE LINGO**
- Warm Roll: Restarting the process without losing the cache.
- Cold Roll: Dropping the cache on restart of the process.
- Arenas: Logical separation of the cache. This maps to a pool inside the cache.
- Item: refers to an allocation in the cache.

## Shared Memory Management

The following are the main players in this area, bottom up:

- **PosixShmSegment/SysVShmSegment**: Implementations for allocating shared memory through the POSIX api and old SYS-V api. They provide apis to create segments based on a key and be able to attach them to the process address space across binaries.
- **ShmSegment**: Provides a common interface across the two shm segment type (POSIX/SYS-V) that can be used by other components above the stack. `usePosix` parameter in the constructor determines whether POSIX or SYS-V will be used. The code can be found in `cachelib/shm/Shm.h`.
- **ShmManager**: Provides api for managing a bunch of segments. The api supports attaching to them across restart using the same keys, managing the lifetime of the segments. The important aspect here is that segments are only persisted for warm roll when they are properly shutdown. The `ShmManager` has no limit on the number of segments it can manage. You can remove some segments and not attach to them on a warm roll. The `ShmManager` identifies the segments based on a name for the segment and the cache directory. The cache directory needs to be the same for `ShmManager` to be able to re-attach to the segments with the same name on a warm roll.

## Memory Allocator

As outlined above, the goal of the memory allocator is to provide support for a malloc like interface with pools and resizing. To achieve this, we allocate a chunk of memory initially and divide it into Slabs similar to a typical Slab based allocator. The memory allocator has a fixed size and if you want to grow the amount of memory, you need to instantiate a new one. However, with an existing memory allocator, we can resize the pools by growing a pool and shrinking others. In the future, when we move completely off of SYS-V apis, we will be able to grow or shrink the overall size of the memory allocator as well.

The memory allocator is optimized for operation under full capacity. Once the cache grows to its full size, we evict existing allocations and repurpose them to make room for new ones without going to the memory allocator. Hence, the memory allocator is designed to operate under the assumption that once the cache grows, the allocator will mostly be full and out of memory. The memory allocator is primarily involved in moving slabs of memory across its pools and within each pool across different allocation sizes once the cache is full.

Other than these, the `MemoryAllocator` also has support to compress the pointers to allocations. The current algorithm for pointer compression is optimized for unCompressing since we typically will be uncompressing pointers(accessing the item) much more than compressing them(when we allocate new items).

- **Slab**: Fixed sized chunks typically in the range of 4MB. The size of slab is defined at compile time and can not be changed without dropping the cache. Each slab at any point of time can be actively used or un-allocated or free. When it is actively used, the slab belongs to a particular pool and a particular allocation size within the pool. This is identified in the header for the slab and is synchronized by the lock in the `AllocationClass` and in `MemoryPool` when they acquire a slab. When a slab is un-allocated, it belongs to no pool or allocation class. When a slab is free, it can potentially belong to a pool or not. But does not belong to any allocation class. The Slab header also contains information about the allocation size for the slab and whether the slab is currently in the process of release.
- **SlabAllocator**: `SlabAllocator` manages the large chunk of memory and carves it into Slabs. The main api is to allocate and free slabs. It also provides support for pointer compression and maintains the apis for accessing the slab for a given memory location, its slab header etc. So getting the pool and allocation class information from any random memory location happens through the `SlabAllocator`.
- **AllocationClass**:  An `AllocationClass` belongs to a Pool and corresponds to a particular allocation size within the pool. Each allocation class has a unique `ClassId` within the pool it belongs to. It can allocate allocations of its configured chunk size until it runs out of slabs. When it runs out of slabs, the allocations fail until more slabs are added into it.
- **MemoryPool**: Consists of a set of `AllocationClass` instances, one per configured allocation size for the pool. Each pool is identified by a name and has a unique PoolId.  When a given `AllocationClass` runs out of slabs, the `MemoryPool` can fetch more slabs from the `SlabAllocator` and add it to the `AllocationClass`. The pool can allocate slabs from the `SlabAllocator` until it reaches the memory limit.
- **MemoryPoolManager**: Provides a simple interface to resolve a string name to `PoolId` and fetching the `MemoryPool` corresponding to the name or `PoolId`. The `MemoryPoolManager` also handles creating or removing pools and keeping tabs on the over-all memory limit across the different pools. The sum of memory limits for all the pools should be strictly less than the `MemoryAllocator`'s total size.
- **MemoryAllocator**: Puts together all the above and provides the following APIs:
    1. allocate memory from a pool
    2. release an allocated memory
    3. adding or removing pools
    4. slab release

The main reason for doing Slab based memory allocation is to avoid fragmentation and enable us to rebalance or resize the cache in chunks of Slabs. By ensuring that a Slab can only contain allocations of one size, we can support a real simple implementation of compressing those pointers by just indexing the slab and the offset of allocation within the slab.

## CacheAllocator

`CacheAllocator` builds a cache using the `MemoryAllocator`.  It is a template class on `MMType` and `AccessType` and provides a malloc like API for allocating memory, that can be accessed through a key. The `MMType` and `AccessType` are template arguments to let `CacheAllocator` mix and match different implementations  of these based on application requirements. For instance, replace LRU with TimerWheels or replace ChainedHashTable with SomeFancyHashTable without having to re-implement the rest of the cache logic.

The allocations done through `CacheAllocator` are called `Item` and they are refcounted. The users have a handle for the item when accessing it through any of the APIs and the `Handle` takes care of maintaining the refcount when it goes out of scope. Items are allocated out of an existing pool that must be created through the same `CacheAllocator` instance. `CacheAllocator` supports saving and restoring the cache through shared memory.

The main APIs for CacheAllocator are:
```cpp
WriteHandle allocate(PoolId pid, Key key, uint32_t size);
ReadHandle find(Key key);
```

The `WriteHandle` / `ReadHandle` is a valid handle to the `Item` and the caller holds a reference to the memory through the `WriteHandle` / `ReadHandle`. Its main interface is `getMemory()` which returns a `void*` for `WriteHandle` and `const void*` for `ReadHandle` that the user can use as a pointer to memory of requested size. It also supports API to access the key for the allocation.

Some of the key abstractions in this are :
- **MMTYpe/MMContainer**: The `MMType` is an implementation that helps with memory management of the Items. Items are initially allocated from the `MemoryAllocator` and then added to a `MMContainer`. When the allocator no longer can make allocations, we recycle an existing Item through the `MMContainer`. In that aspect, the `MMContainer` basically figures out which Items are important and which ones can be evicted. `MMContainer` provides an eviction iterator that `CacheAllocator` uses to walk and find a suitable candidate for eviction. An example `MMType` could be an LRU or FIFO or BucketBasedExpiration. Every `MMType` implementation has an intrusive hook that is part of `Item` and acts as an intrusive container. The goal is to have all MMTypes have a standard interface that `CacheAllocator` can work with. There is one `MMContainer` per allocation size in every Pool. Hence when we want to allocate an Item of size X and are out of memory in the `MemoryAllocator`, we recycle an existing Item from the corresponding MMContainer for the size X in the requested pool. The MMContainer's API is thread safe and protected by its own synchronization method(locks).
- **AccessType/AccessContainer**: The `AccessType` controls how we access Items in the cache. So this is typically some implementation of HashTable. The AccessContainer is an intrusive hook based container and supports a standard interface that works with `CacheAllocator`. We currently have an implementation of `ChainedHashTable` that does chaining to resolve collisions.  Similar to the `MMContainer`, the `AccessContainer`'s API is also thread safe and protected by internal locks.
- **Refcount**: Item's life cycle is managed through refcounts. We currently use 16 bits for refcount. Some of these bits are reserved for special purposes and the remaining is used for keeping track of the number of current `Handles` that are handed out of the cache. Typically, when the refcount drops to 0 during any time, we consider the Item to be free and release the memory for the Item back to the MemoryAllocator. The only exception to this is during eviction when we recycle the memory and make a new Item out of existing one without going through the free → alloc cycle. All refcounts operations are atomic and don't need any special synchronization by themselves. But interpreting the refcount would need appropriate synchronization in some cases. Some of the special bits in the 16 bit refcount are
    - kLinked : This bit indicates that the `Item` is present in the `MMContainer` and is active allocation. This bit is used by the `MMContainer`.
    - kAccessible : This bit indicates that the `Item` is present in the `AccessContainer` and is accessible from the find method.  This bit is used by the `AccessContainer`.
    - kMoving : This bit indicates that the `Item` belongs to a `Slab` that is currently being released. Only the slab rebalancing thread can set or unset this bit. Typically, this bit is set conditionally when one of the other bits are set to deal with races during slab rebalancing. The purpose of the bit is to have the slab rebalancing thread take ownership of freeing the `Item` eventually without having to grab the locks for `MMContainer` or `AccessContainer` causing contention.
- **KAllocation**: This is a simple wrapper that encapsulates a block of memory that holds the `Key`, size of the `Key` and the available memory in an `Item`.

### Slab Release
`CacheAllocator` releases slabs for either resizing a `Pool` or rebalancing different `MMContainers` within a `Pool` to improve application specific metric(hit ratio, cpu etc). The latter is more relevant to a cache since the workloads change quite often when you have a cache server running for a long period of time. Depending on the application using Cachelib, we might want to react in a different way on how we want to do the rebalancing or resizing. So `CacheAllocator` supports providing a user defined implementation of `RebalanceStrategy` and `ResizeStrategy`. The goal of these are to help CacheAllocator pick a slab reassigning to a different allocation size that can help some application specific metric.

To kick off a slab release, `CacheAllocator` talks to `MemoryAllocator` and starts a slab release through `startSlabRelease` call. This gives back a context that contains the slab we are targeting to release and the current active allocations in that slab at the point when the context was created. For the active allocations, `CacheAllocator` needs to ensure they are all freed back to the allocator before calling `completeSlabRelease` which finishes the slab release.  One of the challenges here is to ensure that we safely free the active allocations without any expensive synchronization. Since `CacheAllocator` by itself does not have any synchronization, this is a little hard. The active allocation can be in any of the possible states :
1. Active allocation that is present in the `MMContainer` or `AccessContainer`.
2. Active allocation that is not present in either but has active references held by user
3. Active allocation that is present in one of these containers and in the process of being recycled.
4. Active allocation that is in the process of being freed to the allocator.

Typically, when `CacheAllocator` is dealing with an `Item`, it synchronizes access to the `Item` through the `MMContainer` or `AccessContainer`. We grab references to the Item upon these synchronization. During slab release, one possible option is to wait until all allocations get freed through some other regular cache process. But this could take a while. So the slab rebalancer tries to evict these active Items and free them manually. But, we need to ensure that we don't race with any other threads and double free any active allocations. To help with this across several states that the `Item` can be in, the rebalancing thread uses a special bit in the refcount to indicate that the `Item` is being moved. We set this bit conditionally when one of the other bits is set. This ensures that when the `Item` is in any valid state and we set this bit, it can not be freed by any other process. We also ensure that during the eviction, we don't touch Items that are in moving state. For these Items which have the moving bit set, the slab rebalancing thread then tries to remove them from the cache and wait for the refcounts to drop before calling free on them.

### Pool rebalancer

The `PoolRebalancer` is an asynchronous thread that rebalances the allocation of slabs to different allocation sizes within a pool. It uses a user provided RebalanceMetric to determine which allocation sizes need more memory and which ones don't and executes a slab release periodically when it makes sense to rebalance. Pools are rebalanced only after they have grown to their full sizes.

### Pool resizer

The `PoolResizer` ensures that the pools get sized down when they are over their limit. This typically happens when we size down a pool and the current size of the pool is more than its limit. The resizer watches for these pools and triggers a slab release from the pools that releases the slab back to the MemoryAllocator for use in other pools. This is asynchronous and pools slowly grow in size and shrink as the resizer works through the slabs.
