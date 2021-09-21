---
id: Configure_HashTable
title: Configure lookup performance
---

When you use a cache instance, it comes with a hash table that the `find()` method uses to look up an item in cache by its key. The current implementation uses a chained, open addressable hash table for keeping the lookup performance as small as possible. There are a few knobs to tune that will impact the lookup and insert cost of the cache. For this reason, the default parameters for this are left as the most-unoptimized state. To tune the `HashTable`, you need configure two important parameters in the `AccessConfig` type in your cache.

## Hashtable bucket configuration

`ChainedHashTable::Config::bucketsPower`, an exponent to base 2, is used to configure the number of buckets in the hash table. It is a good idea to set this to twice the number of elements you roughly expect to store in cache. For example, setting it to 21 creates a hash table of 2 million buckets and size 8 MB. For production use cases dealing with millions of items, set this to values around 26-28. If you are dealing with billions of items in cache, set this to 30+. Note for tests, you might want to lower this value so that the memory foot print of tests running concurrently is not an issue.

For example:


```cpp
// Set cache to have 15 million items using 32 million buckets.
Cache::Config cfg;
cfg.accessConfig.bucketsPower = 25;
```


### Choosing a good value

For a fast lookup/insert performance in a chained hash table, keep the chaining at minimum. A good rule of thumb for such a hash table is to ensure there are no more than 50% of buckets occupied with any elements. However, that also means that you will need a bigger hash table and waste memory from unused buckets. You can use the following table to estimate a good approximate bucket power to start with. If you can tolerate the lookup performance and memory usage is important, you can go down by one factor in your bucket power.


|Max number of items in millions |Bucket power |Memory overhead    |
|--------------------------------|-------------|-------------------|
|tests                           | 20          | 4 MB              |
|<32                             | 26          | 256 MB            |
|32 - 64                         | 27          | 512 MB            |
|64 - 128                        | 28          | 1 GB              |
|128 - 256                       | 29          | 2 GB              |
|256 - 512                       | 30          | 4 GB              |
|512 - 1024                      | 31          | 8 GB              |
|1024+                           | 32          | 16 GB             |


### Detecting a bad value

The following helps you find out whether your hash table size is misconfigured:

1. Note that the latency for `find()` or `insertOrReplace()` or `allocate()` is too much.
2. Running strobelight or perf shows that most of the cycles are spent on a function in `ChainedHashtable`.
3. Look at Stats from a production instance. To figure out in production how bad your hash table chaining is, use the `getAccessContainerDistributionStats()` API to see the distribution of buckets by their occupancy.

The `AccessDistribution` stats contains the following structure:


```cpp
// Stats describing the distribution of items (keys) in the hash table.
struct DistributionStats {
  uint64_t numKeys{0};
  uint64_t numBuckets{0};
  std::map<unsigned int, uint64_t> itemDistribution{};
};
```


The `itemDistribution` map contains the distribution of buckets by their occupancy of items; i.e., `itemDistribution[0]` contains the number of buckets with 0 elements, `itemDistribution[1]` contains the buckets with one element, etc. A lower number of buckets with 0 will indicate that your cache will suffer from a bad bucket power.

## Hashtable concurrency performance

`ChainedHashTable::Config::locksPower` parameter controls the concurrency of accessing the hash table from multiple threads. To optimize for concurrent lookups, cachelib shard's the hash table by locks across the keys. Most likely you do not have to configure this parameter from its default value of 10 unless you notice `perf` samples showing SharedMutex stack in cachelib code path.


```cpp
// Set cache to be sharded by 1 K locks for the hash table.
Cache::Config cfg;
cfg.accessConfig.locksPower = 10;
```

## Auto-tune Hashtable
If you know the estimated number of items to cache, instead of figuring out the two parameters yourself, you can pass #items in `setAccessConfig` API to automatically tune the hash table with reasonable values of these two parameters:
```cpp
Cache::Config cfg;
cfg.setAccessConfig(20000000 /* items number */);
```
which is equivalent to:
```cpp
Cache::Config cfg;
cfg.setAccessConfig({25 /* bucketsPower */, 15 /* locksPower */});
```

If you are using chained items, you should also configure `chainedItemAccessConfig` and `chainedItemLocksPower`.

Please note, `chainedItemLocksPower` is different from `ChainedHashTable::Config::locksPower`. It controls the number of locks to synchronize chained item's operations(add, pop, move, transfer, etc.). By default, it is set to be 10.

Similarly, you can passing the number of estimated chained items in `configureChainedItems` API to auto-tune the hash table:
```cpp
cfg.configureChainedItems(20000000 /* items number */, chainedItemLocksPower);
```
which is equivalent to:
```cpp
cfg.configureChainedItems({25 /* bucketsPower */, 15 /* locksPower */}, chainedItemLocksPower);
```
