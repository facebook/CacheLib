---
id: ttl_reaper
title: TTL Reaper
---

Cachelib allocators support time to live (TTL) on an item natively at the granularity of seconds. When you set a TTL on an item, the item is automatically reaped if it is still present in the cache after its expiry.  The `find()` method returns an empty handle (`nullptr`) for an item that has expired.

For example:


```cpp
uint32_t ttlSecs = 600;   // expires in 10 mins
auto item =  cache.allocate(poolId, "hello-world", size, ttlSecs);
cache.insertOrReplace(item);

// Try to find the item after 10 mins.
auto item = cache.find("hello-world");  // returns nullptr
```


Note that setting a TTL on an item *does not guarantee that the item lives for the TTL*, but rather makes the item stale and not accessible if it is still present in the cache beyond the TTL.


## Configure reaper

The reaper is responsible for reclaiming the memory from expired items. Reaper is enabled by default. The speed at which reaper runs can be controlled in the following way.

* `reaperIntervalSecs`  Configures the interval at which reaper scans the cache to reclaim the memory for expired items. Recommend setting this to `10 seconds`.
* `reaperConfig` This provides a more fine grained throttling on the reaper if you want to control the CPU variance of the reaper.


```cpp
config.enableItemReaperInBackground(
  std::chrono::milliseconds interval,
  util::Throttler::Config reaperConfig = {}
);
```


Call the `getReaperStats()` method to access the reaper statistics, which provides a a breakdown of the number of items visited against the reaped count.
