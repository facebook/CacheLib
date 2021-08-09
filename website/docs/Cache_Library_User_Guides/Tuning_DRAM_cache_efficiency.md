---
id: Tuning_DRAM_cache_efficiency
title: Tuning DRAM cache efficiency
---

## Reduce fragmentation

When you use cachelib to allocate memory from cache, you get a piece of memory rounded up to the closest allocation size in the cache. This can lead to wastage of memory (e.g., if you allocate 60 bytes, you get  memory from the 80-byte allocation class; 20 bytes is wasted). Once this grows beyond 5%, there is an opportunity to reduce the fragmentation and increase the usable cache size by tuning the internal allocation sizes.

To estimate the current fragmentation size, use this ODS counter to export the current fragmentation bytes per pool and see if the overall volume is more than 5% of the `mem.cache_size`. See this sample [ODS graphs](https://fburl.com/ods/6b7x7h2c) from twfeed.

**Remediation**: To tune the allocation sizes, you must first identify the allocation sizes contributing to the fragmentation by using this [Scuba query](https://fburl.com/scuba/cachelib_admin_ac_stats/y0eeyagc) (modify the cache name accordingly). Once you identify the size ranges, we can configure the allocation sizes to be more granular and override the default allocation sizes. For example D20638993 resulted in reducing the fragmentation from 12% to 3.8% and improved the hit ratio. *Changing allocation sizes would need the cache to be dropped if you have cache persistence enabled.*

## Configure TTL reaper

If you have enabled TTL for your objects, reaping them as soon as they expire would free up the space for other objects that can benefit from more cache space. If you don’t pass a TTL to `allocate()` and you don’t think you have a notion of TTL, you can ignore this.

To estimate if you have objects significantly outliving their TTL, use this [Scuba query](https://fburl.com/scuba/cachelib_admin_items_stats/pn83btur) to see if a significant portion of the expired objects live in the cache.

**Remediation**: Please follow the instructions on [time to live on item](ttl_reaper/ ) to turn on the reaper. If you have a reaper already and don’t have the `slabWalk` mode enabled, enabling it would speed up the reaping of the expired items.

## Enable pool rebalancing

If your objects are of different sizes, their relative memory footprint in the cache might be suboptimal. For example, you might have too many of the large objects hogging space or certain sized objects getting evicted faster than the other. These can lead to suboptimal hit ratios. Turning on pool rebalancing can help in this.

There isn’t an easy way to tell if this is causing regressions; the easiest way is to experiment on a small canary with a few [options](pool_rebalance_strategy/#picking-a-strategy ) offered by cachelib. Tuning this often results in improvements to hit ratio. For example, Barkeep saved 300M misses/sec to memcache by enabling pool rebalancing on their local cache.

## Avoid copying from cache

If your mode of operation to access the cache is to copy from cache to deserialize or send out of the network, you can usually avoid the copy by passing the `ItemHandle` to the appropriate places. For example, if you read from the cache and copy into Thrift reply, you can avoid the copy by converting the `ItemHandle` into an `IOBuf`. Similarly, if you copy to deserialize, you can avoid this by deserializing out of the `IOBuf` from `ItemHandle`.

To estimate if this is an issue, collect a strobelight profile to see if you spend more than a few % cycles on memcpy variants. If you do, follow some of the examples in [`convertToIoBuf`](https://fburl.com/codesearch/2rmxq5e4) to try to accomplish similar things.

For more information on the `convertToIOBuf()` method, see the [comments](https://fburl.com/diffusion/9u7549k4).

## Tune eviction policy

Both `LruAllocator` and `Lru2QAllocator` provide knobs to tune them that are mostly turned off. Trying these out could improve hit ratio. For example, if you are using LRU, trying to turn on mid-point insertion could help get objects that are accessed just once out of the cache faster. Similarly, trying the 2Q allocator captures more access patterns that LRU doesn’t (e.g., 2Q can effectively evict items that have spiky accesses after the spike had ceased).

For more details on the available knobs, see [eviction policy](eviction_policy/ ).

## Reduce lock contention

Cachelib has locks at several granularity to protect the internal data structures. If there is a contention, there are few options to reduce the contention depending on the type.

To estimate, collect a strobelight profile. If the strobelight profile indicates more than 4% spent on `allocate()` or `insertOrReplace()` or `find()`, there is potential to look into this further.

- Contention in `insertOrReplace`
If you see contention in `insertOrReplace` resulting in `SharedMutex` showing up beneath it, usually this is a result of misconfigured Hashtable. This can be tuned by adjusting the [lockPower](Configure_HashTable/#lockspower ).

- Contention in `addChainedItem` (or related functions)
Similar to above, we have a separate hash table to keep track of chained items. If you see contention here, adjust lockPower the same way as above.

- Contention in LRU
If you notice contention under MMLru or MM2Q, it indicates you have quite a lot of activity (400k/sec+) concentrated around objects of a particular size. To remediate this, we have a few options. If the number of `allocate()` calls per alloc size is too high, sharding the `allocate()` calls by creating additional pools would help. If the contention is coming from `find()`, adjusting the `lruRefreshThreshold` or turning on `dynamicLruRefreshThreshold` could help as documented in [eviction policy](eviction_policy/ ).

## Analyze cache friendliness of your workload

If you suspect the workload is not uniformly cacheable, enabling Working Set Analysis(WSA) can help us identify potential hit ratio improvements.  Follow the instructions in [working set analysis](Working_set_analysis ) to enable WSA. After enabling WSA, reach out to the Cachelib team to run the analysis pipelines that can help you dig into the workload.
