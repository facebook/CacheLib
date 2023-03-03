---
id: Add_monitoring_for_cache
title: Add monitoring for cache
---

Now that we have a working cache. We need to add monitoring to it. Without ODS and Scuba monitoring, you would be flying blind regarding the performance and health of your cache. CacheLib offers comprehensive monitoring support, and we'll walk you through how to set it up below.

## Background

Recall when you instantiated the cache, you have the following header and code:

```cpp
#include "cachelib/allocator/CacheAllocator.h"

using Cache = facebook::cachelib::LruAllocator;
std::unique_ptr<Cache> cache;
facebook::cachelib::PoolId default_pool;

void initializeCache() {
  Cache::Config config;
  config
      .setCacheSize(1 * 1024 * 1024 * 1024) // 1 GB
      .setCacheName("My cache")
      .setAccessConfig({25, 10})
      .validate();
  cache = std::make_unique<Cache>(config);
  default_pool =
      cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);
}
```

## Create CacheAdmin
To add `CacheAdmin`, which is the component that will export CacheLib stats to ODS and Scuba, you need to (1) add `//cachelib/facebook/admin:admin` to your `TARGETS` file and (2) add the following code:

```cpp
#include "cachelib/facebook/admin/CacheAdmin.h"

std::unique_ptr<cachelib::CacheAdmin> admin;

void initializeCache() {
  ... setting up the cache here

  CacheAdmin::Config adminConfig; // default config should work just fine
  adminConfig.oncall = "my_team_oncall_shortname"; // Please do not forget to add your team's oncall shortname!
  admin = std::make_unique<CacheAdmin>(*cache, adminConfig);
}
```

## Export Stats to ODS
Cachelib publishes ODS stats via service data (which exports to the fb303 port). If the service uses Thrift, it should already get the fb303 port as a part of it. Otherwise, you need to open a fb303 port in your service. Once fb303 is setup,  the  monitoring config must be updated to  registers cachelib’s stats. For example, [see Feed Leaf's fb303 monitoring](https://fburl.com/diffusion/tk55n07s).  Add the following to your monitoring config:


```cpp
import cache.cachelib.cachelib.mon.cinc as cachelib
cachelib.add_cachelib_collector(cfg, "cachelib_fb303", <OPTIONAL: YOUR_FB303_PORT>)
```


## Check Your Stats

After adding the above to your code and configerator monitor config. You want to canary it on the host you intend to run cachelib. And then, you can start up a cache and have it running for about 10 minutes. Then go on ODS and look up the following query:

```none
cachelib.<YOUR CACHE NAME>.ram.uptime
```

You should see the stats shows up. Make sure to check a few more stats to verify they're all being exported. If you see anything missing, reach out cachelib oncall for assistance.

Please refer to [Monitoring cache health](monitoring) for a more in-depth look at the stats we offer on ODS and Scuba.

## Monitor Your Cache
In addition to building your own dashboard (and adding alarms), you can also use Cachelib's dashboards for high level metrics on how your cache is doing. We offer two dashboards:
1. [ODS](https://fburl.com/unidash/ehpb743v) - High level cachelib metrics on ODS
2. [Scuba](https://fburl.com/unidash/5l3bbo4u) - Deeper dive into cachelib's ram cache behavior

Please contact our oncall if you have a feature request or any ideas to improve these dashboards further.
