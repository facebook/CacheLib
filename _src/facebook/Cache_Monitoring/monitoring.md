---
id: monitoring
title: Monitoring cache health
---

Cachelib offers both ODS and Scuba monitoring for your cache health. To enable monitoring, see [Add monitoring for cache](Add_monitoring_for_cache). In Scuba and ODS, you can find such information as hit rate, rate of allocations, eviction age, fragmentation, etc. This comprehensive set of metrics helps you to know how well your cache is performing or to debug issues related to your cachelib usage.

## CacheLib Monitoring Dashboards

In addition to building your own dashboard (and adding alarms), you can also use Cachelib's dashboards for high level metrics on how your cache is doing. We offer two dashboards:
1. [ODS](https://fburl.com/unidash/ehpb743v) - High level cachelib metrics on ODS
2. [Scuba](https://fburl.com/unidash/5l3bbo4u) - Deeper dive into cachelib's ram cache behavior

Please contact our oncall if you have a feature request or any ideas to improve these dashboards further.

## ODS

Cachelib publishes ODS stats via *service data* (which exports to the fb303 port). If you already use Thrift, you should already get the fb303 port free. Otherwise, you need to open a fb303 port in your service. For cachelib stats to show up on ODS, you must set up a monitoring config that registers cachelib’s stats. For example, [see Feed Leaf's fb303 monitoring](https://fburl.com/diffusion/tk55n07s). Essentially you just need to add the following to your configerator config:

```cpp
import cache.cachelib.cachelib.mon.cinc as cachelib
cachelib.add_cachelib_collector(cfg, "cachelib_fb303", <OPTIONAL: YOUR_FB303_PORT>)
```

Cachelib ODS stats start with the prefix `cachelib.<your_cache_name>`. Many cachelib stats comes in the accumulative form. For example, `cachelib.<your_cache_name>.items.total` is the accumulative number of items in cache since your service has started. Other stats are offered in rate variants (for example, `cachelib.<your_cache_name>.cache.alloc_attempts.60`). If you think certain stats should be exported but are missing, reach out cachelib oncall for assistance.

The status is also broken down by pools. For example on DRAM cache size:

```cpp
cachelib.<cache-name>.mem.size
cachelib.<cache-name>.mem.usable_size
cachelib.<cache-name>.mem.advised_size
```

To obtain break-down of advised memory per pool:

```cpp
cachelib.<cache-name>.<pool-name>.size
cachelib.<cache-name>.<pool-name>.usable_size
cachelib.<cache-name>.<pool-name>.advised_size
```

Use our [ODS dashboard](https://fburl.com/unidash/7uqai4oc) to check key stats (you need to select the cacheName and enter the entity). For a complete list of cachelib ODS stats, see [this sheet](https://docs.google.com/spreadsheets/d/1udXP8uavr3wFjOhRoU9Eb_FI5IS8FDrPkz_O9LwNz0k/edit?usp=sharing) and the [code pointer](https://fburl.com/code/84oidxl8) that generate all cachelib ODS stats.

## Scuba

We have several Scuba tables, each servering a different purpose. They help you to triage different type of issues. We'll describe the three most relevant tables first, covering other tables in [Misc_Scuba_tables_for_debugging](https://www.internalfb.com/intern/wiki/Cache_Library_User_Guides/monitoring/#Misc_Scuba_tables_for_debugging). If you find any stat missing in this list of tables, contact us and we'll add them here. Note that all our counters, Scuba or ODS, are accumulative counters. They are not rate of change counters.


### Cachelib Admin pool stats

This dataset gives you high level information on how well your cache is doing. On a per-pool basis, you can find the following information: How many allocs are happening? How many items are there in my cache? How much fragmentation am I seeing? What's the rate of eviction? See the sample [Cachelib Admin Pool Stats](https://fburl.com/scuba/nwhc6gb8).

Note all stats applies to an individual memory pool. If you only have one pool, then this is basically the global cache stats. Otherwise, you need to sum the stats for each pool to get true global cache stats.

1. **Active Allocs**
Number of outstanding allocations (items).
2. **Alloc Attempts**
How many times you have called the `allocate()` function for this pool.
3. **Alloc Failures** How many times of the above resulted in a failure. The cause is usually no free memory. A high rate of this indicates the rate of allocation is too much and evictions cannot keep up.
4. **Evictable Items** Number of evictable allocations (items). Please recall cachelib also lets you allocate non-evictable items. This number should be always be less than or equal to Active Allocs.
5. **Unevictable Items** We let you allocate permanent items (items that will never be evicted). This is the number of those. Beware that if this number is high (i.e., more than a few percent of the unevictable items), you may see performance impacts (suboptimal hit-rate and longer amount of time to rebalance your cache).
6. **Evictions** Number of evictions since cache started.
7. **Fragmentation Size** How much fragmentation there is. If this figure is more than 10% of your total pool size, it means you should consider customizing your allocation sizes in this pool to fit your allocations better. We have had use cases where we cut their fragmentation down from 15% to 5%, effectively giving them 10% more memory for caching.
8. **Free Allocs** Number of allocations that are freed. Usually if you have not configured your cache with TTL or have logic that explicitly invalidates cached items, you should not see a high number for this stat.
9. **Free Memory** This is the total memory footprint from Free Allocs and Free Slabs.
10. **Free Slabs** This indicates how many slabs in your pool that are not being used. You're expected to see this number when your cache is still warming up. However, in steady state, you should expect 0 or (very close to 0) across your tier.
11. **Invalid Allocs** How many allocations were rejected because you requested for an invalid size; that is, the size requested is too big. We have a hard ceiling of 4 MB for the entire item, which means (4 MB - 31 bytes) for your key and value. Also you can lower this ceiling yourself (but not increase it).
12. **Items** This is the same as Active Allocs. It's redundant.
13. **Pool Get Hits** How many hits there have been in this pool since the cache started. Unfortunately we're not able to give you a hit-rate because we do not know which pool the misses went. If you only have one pool, then divide this number by the total requests to cache to get the equivalent to hit rate.
14. **Pool Size** Configured size of this pool. This does not indicate how big this pool really is at this moment. It means how big this pool can grow to be. In general, in steady state, you should expect the total slabs stat to match this.
15. **Total Slabs** The number of slabs in this pool. Used and unused. This stat multiplied by 4MB should be equal to *Pool Size* in steady state.
16. **Unallocated Slabs** Number of slabs not yet allocated from the slab allocator. You're expected to see this number when your cache is still warming up. However, in steady state, you should expect 0 or (very close to 0) across your tier.
17. **Min/Max Eviction Age** This is the minimum and maximum eviction age for the allocation classes in this pool. Cachelib uses a slab allocator so different alloc sizes may have different eviction age. In general, this tells you that if you insert an item into this pool and never read it, how long you can expect this item to stay cached. It is a good approximate for retention time.

### Cachelib Admin Ac Stats

This is similar to *cachelib admin pool stats* but a more in-depth view at each allocation class. Cachelib uses a slab allocator underneath, so we divide memory inside each pool into a set of allocation sizes. Each allocation size is an allocation class. This table gives you the following information: How many items are in this allocation size? How much fragmentation is there? Rate of eviction? How many free allocations are available? What is the eviction age? See the sample [Cachelib Admin Ac Stats](https://fburl.com/scuba/1yhoxsuu).

All the stats here applies to an individual allocation class. For example, your cache may be configured to have only two allocation sizes: 80 bytes and 1 MB. This means all your allocations will either be allocated from 80 bytes (if what you request via `CacheAllocator::allocate()` is less than or equal to 80 bytes) or 1 MB. Anything above 1 MB will result in an exception (reflected in the number of Invalid Allocs in Pool Stats). You will see two sets of AC stats for your pool. One refers to the 80-byte class and the other refers to the 1 MB class.

1. **Active Allocs** Same as Pool Stats but only for this allocation class.
2. **Alloc Attempts** Same as above.
3. **Alloc Failures** Same as above.
4. **Alloc Size** The allocation size for this class. This is the real allocation you get when you request for a size that is less than or equal to this allocation class's size. For example, if you have two AC's: 80 bytes and 100 bytes. And you request for 92 bytes. You will get an allocation that is actually 100 bytes and thus you have just incurred a fragmentation of 8 bytes.
5. **Evictable Items** Same as Pool Stats but only for this allocation class.
6. **Eviction Age** This is how long your item for this allocation class will be cached if you never access it again after insertion. Use this to get a sense of how long the cache retention time is for all of your objects that are of this allocation size.
7. **Evictions** Number of evictions in this allocation class since cache started up.
8. **Fragmentation Size** Total amount of fragmentation incurred in this allocation class.
9. **Free Allocs** Same as Pool stats but only apply for this allocation class.
10. **Free Slabs** Same as above.
11. **Full** This allocation class is full, which means it has zero free slab. Eviction is the only way to obtain new allocations.
12. **Lru Refresh Time Evictable** How often do we promote an item on read or write? This applies only to evictable items.
13. **Lru Refresh Time Unevictable** Same as above. Only applies to unevictable items.
14. **Num Hits** Number of cache hits that are from items in this allocation class.
15. **Total Free Memory**: The total available memory for allocations.
16. **Total Used Bytes** How many bytes are used for allocations (this includes all the fragmentation incurred by allocations).
17. **Unevictable Items** Same as Pool Stats but only for this allocation class.
18. **Used Slabs** How many slabs are used currently by this allocation class.

### Cachelib Admin Items stats

This table  gives you information on individual items that can help you reason about the composition of the cache. The table is populated by picking a random item weighted by its size and uploading the  metadata for  this item. Overtime (several days to a few weeks), you can get a very good idea on what sort of items live in your cache. For example, if you're running a service like Memcache, you may be interested in finding out what are the top key families that consume most of your cache memory (items that start with the same key prefix is one key family). Some of the other stats we offer is how long this item has been around in cache and how much fragmentation was introduced by allocating this item. See the sample [Cachelib Admin Items Stats](https://fburl.com/scuba/pnxdevhe).

The following columns are available for sampled items.
1. **Item Age**   - time since the creation of the item.
2. **Key Size** - the size of the key for the item.
3. **Value Size** - the allocation size requested for the item in bytes.
3. **Total Item Size** - the size requested for key and the value and item metadata.
2. **Alloc Size** - the size class the item was allocated to.
3. **Fragmenation Size** - unused fragmentation size for the item.
3. **Class Id** - class id corresponding to the allocation.
4. **Pool Id** - pool id corresponding to the allocation.
5. **Creation Time** - time when the item was created.
6. **Est Lifetime** - the ttl which was used to allocate the item.
10. **Ttl** - time remaining from the sampling point to actual expiry of the item.
7. **Expiry Time** - time when the item
8. **Is Chained Item** - if the item is a  chained allocation that belongs to a chain of allocations.
9. **Has Chained Item** - if the item is the parent item and has chained allocations.
10. **Key** - the key for the item in binary format
11. **Hex Key** - the key represented in hexadecimal format.

> :exclamation: IMPORTANT:
>
> The cachelib_admin_item_stats table contains senstive cache information in raw form (key/hex-key). Due to this, the access to the table is restricted to cachelib team. If this data would be useful to debugging for your team in a particular issue, you can request temporary access through the scuba workflow.

## Misc Scuba Tables for Debugging

### Cachelib Logs

This table has all the logs cachelib internally writes (at a sampled rate). This can be useful for debugging. For example, suppose you notice one day that cachelib's slab rebalancing is stuck on a few machines; you then filter for these hosts in this dataset, and you may find we're logging some items are stuck in moving, which could then indicate a deadlock in the `movingSync` you supplied to us or point to an internal cachelib bug. You look at this table only when things are not working as expected.

Add the following to enable cachelib logging:

```cpp
// In your program's main.cpp:
namespace folly {
const char* getBaseLoggingConfig() {
  facebook::cachelib::registerCachelibScubaLogger();
  return CACHELIB_FOLLY_LOGGING_CONFIG;
  // Alternatively, you can configure your own log levels.
  //  return "cachelib=<LOG_LEVEL>"
  //                 ":<LOG_HANDLER>"
  //                 ";<LOG_HANDLER>=<NAME_OF_LOG_HANDLER>";
  //  e.g., "cachelib=ERR:cachelib_scuba;cachelib_scuba=cachelib_scuba_logger"
}
} // namespace folly
```

To override the default folly logging level, pass in a command line option when starting your binary or set the option via Thrift fb303. Refer to the folly logging documentation for more information on log levels and categories.

```none
# Change log level on startup.
# Changes the default logging level to "WARN". By default cachelib logs everything at INFO.
<your_binary> --logging "WARN"

# Change log level during runtime.
# Changes cachelib logging level to "DBG". By default cachelib logs everything at INFO.
buck build common/fb303/scripts:fb303_logging_config
buck-out/gen/common/fb303/scripts/fb303_logging_config.par \
  --host <YOUR_HOST>:<YOUR_FB303_PORT> \
  --update 'cachelib:=DBG'
```

### Cachelib Admin Metadata

(**Deprecated**) This table has some basic information such as cache version, cache size, and cache name. When a service running cachelib first starts, it sends a sample to this table. In general, this is a good way to visualize what version every cachelib user is running on. It is **deprecated** in favor of *Cachelib Admin Allocator Config*. See the sample [Cachelib Admin Metadata](https://fburl.com/scuba/0daj3xn9).

### Cachelib Admin allocator config

This is intended to replace *Cachelib Admin Metadata*. It uploads everything the metadata table does and also uploads all the cache configurations. This will be very useful to check what config your service is running (when you're rolling out a new change for example or debugging an on-going issue). For example, you can find out which host has slab rebalancing enabled or how big the hash table is configured for a particular tier.
