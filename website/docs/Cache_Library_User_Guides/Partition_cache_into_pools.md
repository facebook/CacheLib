---
id: Partition_cache_into_pools
title: Partition cache into pools
---

One easy way to manage your cache's memory is to partition it into pools. This is useful when your application has knowledge of different objects and you want to cache them into different pools or you want to set cache memory limits based on application specific context. For example, if you are caching photos on a 20 GB cache, you can separate celebrity photos from cat photos by storing them in two different pools and restrict the size of each pool.

So you don't need to create separate cache instance; you can create a single cache and use cachelib to partition it into two or more pools.

By partition your cache into pools, you can achieve the following:

- **Restricting memory footprint** Each pool's workload is isolated from others' when it comes to eviction and memory footprint. If you configure a pool to take 10 GB out of a 30 GB cache, cachelib ensures that the pool's footprint across all objects can not grow beyond that.

- **Improving hit ratio** Each pool has its own eviction domain. Once a pool is full, cachelib only evicts data in the same pool and does not pull in memory not belonging to the pool. This is useful because, often times, the cache hit ratio curve (hits vs cache size) varies between different workloads. Using pools to isolate workloads ensures that one workload does not impact the other. For example, if you want to guarantee different hit ratios for different objects you cache, partition the cache into different pools, one for each object type.

## Creating pools

You can create and resize pools at runtime. Cachelib supports up to 64 pools. `CacheAllocator.h` defines the following methods to manage pools:


```cpp
// Create a pool with the specified name and size.
public PoolId addPool(
  folly::StringPiece name,
  size_t size,
  const std::set<uint32_t>& allocSizes = {},
  MMConfig config = {},
  std::shared_ptr<RebalanceStrategy> rebalanceStrategy = nullptr,
  std::shared_ptr<RebalanceStrategy> resizeStrategy = nullptr,
  bool ensureProvisionable = false,
);

// Shrink the existing pool by the specified number of bytes.
public bool shrinkPool(PoolId pid, size_t bytes)

// Grow the existing pool by the specified number of bytes.
public bool growPool(PoolId pid, size_t bytes);

// Return the ID for the specified pool.
PoolId getPoolId(folly::StringPiece pool_name) const noexcept;
```


For example, the following code creates two pools from a 30 GB cache:


```cpp
// Create two pools.
auto photoPoolId = cache.addPool("photos", 10 * 1024 * 1024 * 1024);    // 10 GB pool
auto regularPoolId = cache.addPool("regular", 20 * 1024 * 1024 * 1024); // 20 GB pool
```


If you have the name of an existing pool, call the `getPoolId()` method to get its ID:


```cpp
auto poolId = cache.getPoolId("photos");  // "photos" is the pool name
```


To allocate memory from a specific pool for an item, call the `allocate` with the pool's ID:


```cpp
// Allocate memory for the "my-cat" item from the pool with ID photoPoolId.
auto photoHandle = cache.allocate(photoPoolId, "my-cat", 1024);

// Allocate memory for the "my blob" item from the pool with ID regularPoolId.
auto regularHandle = cache.allocate(regularPoolId, "my blob", 400);
```


Each item is associated with a unique key. Use the key to find the item (you don't need the pool ID to look up the item):


```cpp
auto handle = cache.find("my-cat");       // find item in photo pool
auto otherHandle = cache.find("my blob"); // find item in regular pool
```


## Customizing pools

The `addPool()` method provides default parameters:


```cpp
public PoolId addPool(
  folly::StringPiece name,
  size_t size,
  const std::set<uint32_t>& allocSizes = {},
  MMConfig config = {},
  std::shared_ptr<RebalanceStrategy> rebalanceStrategy = nullptr,
  std::shared_ptr<RebalanceStrategy> resizeStrategy = nullptr,
  bool ensureProvisionable = false,
);
```


When calling `addPool()` to create a pool, override some of the default parameters to customize your pool:

1. **Allocation sizes** Allocation sizes can be customized on a per pool basis. Cachelib uses a slab allocator which can take custom allocation sizes per pool. Tuning the allocation sizes can help you reduce overall memory fragmentation and improving cache hit ratio.

2. **Eviction parameters** Your `MMConfig` parameter can be customized on a per pool basis when you create the pool. For more information, see [Eviction parameters configuration](#Eviction_parameters_configuration ).

3. **Rebalance strategy** This lets you override the default strategy that is used for rebalancing the memory in the pool across its various allocation sizes if you choose to. For example, you can set the overall cache to use `HitsPerSlab` strategy and set `LruTailAge` strategy for some specific ones where you want to ensure uniformity in eviction ages across cache objects of different sizes.

### Eviction parameters configuration

For `MMLru`, look at the config structure in `cachelib/allocator/MMLru.h`.

For `MM2Q`, look at the config structure in `cachelib/allocator/MM2Q.h`.

When configuring your eviction policy, look out for these:

* `defaultLruRefreshTime` By default, this controls how many seconds since the last access; we will promote this item to the head of LRU. Setting this to a longer time (for example, 300 seconds) may help reduce contention in the LRU. However, setting this too high (i.e., close to or higher than the eviction age) will make you evict items sooner because before you get to promote it, it will be evicted.

* `lruRefreshRatio` This is useful if you want a dynamically configured LRU refresh time. For example, you don't know if you should use 60 seconds or 300 seconds. Then you can simply set this to be a ratio of the eviction age, for example, 0.1 which is 10% of the eviction age. In that case, if your eviction age is 1,000 seconds, we will promote an item if it has been 100 seconds since we last accessed it. Or for an eviction age of 3,000 seconds, we will only promote if it has been 300 seconds since its last access.

* `mmReconfigureIntervalSecs` This controls how often we adjust the LRU refresh time per the ratio explained above.

* `updateOnWrite` Should we promote an item to be the head when you access it for mutation?

* `updateOnRead` Should we promote an item to be the head when you access it for pure reads?

## Resizing pools

The following describes two ways to resize a pool at runtime.

### 1. Manual pool resizing

An application can use cachelib APIs to resize pools asynchronously. When changing the pools' sizes, cachelib will shrink or grow the pools asynchronously based on the `PoolResizer` configuration. Depending on the `PoolResizer` configuration and the amount of memory to be resized, cachelib might take a few minutes to hours to finish resizing the pool.

For example:


```cpp
// Move 1 GB from photos pool to regular pool.
auto res = cache.resizePools(photosPoolId, regularPoolId, 1024 * 1024 * 1024);
```


When the cache has free memory, call the `growPool()` method to increase the pool's size:


```cpp
/**
 * Assume the cache has 100 GB memory, 20 GB is added to the
 * photos pool, and 50 GB is added to the regular pool.
 * The remaining 30 GB is unclaimed.
 */
size_t extra_bytes = 1024 * 1024 * 1024; // 1 GB
auto res = cache.growPool(photosPoolId, extra_bytes);
```


To free up some of a pool's memory for another new pool, call the `shrinkPool()` method:


```cpp
size_t memory_size = 1024 * 1024 * 1024; // 1 GB
// Shrink an existing pool.
auto res = cache.shrinkPool(regularPoolId, memory_size);

// Create a new pool.
auto newPoolId = cache.addPool("new-pool", memory_size);
```


### 2. Automated pool resizing

Cachelib provides `PoolOptimizer` to track the hits from each pool and adjust the sizes automatically after the initial setup. We're actively polishing `PoolOptimizer` for wider use. Reach out to us in the Cache Library Users group if you are interested in using it.

## Pool specific stats

### Stats API

You can track the breakdown of `find()`, `allocate()`, hits, number of items in the cache, etc. by pool. For example:


```cpp
auto poolStats = cache.getPoolStats(photosPoolId);
```


`PoolStats` exposes the following counters and information:


```cpp
struct PoolStats {
  // Pool name given by users of this pool.
  std::string poolName;

  // Total pool size assigned by users when adding pool.
  uint64_t poolSize;

  // Container stats that provide evictions etc.
  std::unordered_map<ClassId, CacheStat> cacheStats;

  // Stats from the memory allocator perspective. This is a map
  // of MPStat for each allocation class that this pool has.
  MPStats mpStats;

  // Number of get hits for this pool.
  uint64_t numPoolGetHits;

  const std::set<ClassId>& getClassIds() const noexcept {
    return mpStats.classIds;
  }

  // Number of attempts to allocate.
  uint64_t numAllocAttempts() const;

  // Number of attempts that failed.
  uint64_t numAllocFailures() const;

  // Number of attempts that were invalid.
  uint64_t numInvalidAllocs() const;

  // Toal memory fragmentation size of this pool.
  uint64_t totalFragmentation() const;

  // Total number of free allocs for this pool.
  uint64_t numFreeAllocs() const noexcept;

  // Amount of cache memory that is not allocated.
  size_t freeMemoryBytes() const noexcept;

  // Number of evictions for this pool.
  uint64_t numEvictions() const noexcept;

  // Number of items in this pool.
  uint64_t numItems() const noexcept;

  // Total number of allocations currently in this pool.
  uint64_t numActiveAllocs() const noexcept;

  // Number of hits for an alloc class in this pool.
  uint64_t numHitsForClass(ClassId cid) const {
    return cacheStats.at(cid).numHits;
  }

  uint64_t numSlabsForClass(ClassId cid) const {
    return mpStats.acStats.at(cid).totalSlabs();
  }

  // This is the real eviction age of this pool as this number
  // guarantees the time any item inserted into this pool will live.
  uint64_t minEvictionAge() const;

  // Computes the maximum eviction age across all class Ids.
  uint64_t maxEvictionAge() const;
}
```
