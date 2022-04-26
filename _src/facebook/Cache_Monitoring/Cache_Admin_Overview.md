---
id: Cache_Admin_Overview
title: "Cache Admin Overview"
---
CacheAdmin is a component for monitoring the cache health for CacheLib cache. Its purpose is to fetch cache stats from Cache, and then upload them to ODS or Scuba as appropriate. The code is located in fbcode/cachelib/facebook/admin/. CacheAdmin contains the following components. As you can see, we separate them into roughly three groups per their update interval.

![](cache_admin_overview.png)

## Exporter & How Does It Work

Each exporter inherits from DataExporter class. The DataExporter base class takes in a folly::FunctionScheduler, and is expected to schedule the implementation of its “exportData" function on the scheduler. The way we use the exporters in CacheAdmin is to share the same scheduler with all of them. This way, we use only a single thread (one thread per FunctionScheduler) for all of our exporters. Now let’s go over the exporters one by one.

## Allocator Config Exporter

Each cache instance (i.e. CacheAllocator) is created with a CacheAllocatorConfig config object. This exporter simply serializes the config object and upload it to scuba. The scuba table name is: cachelib_admin_allocator_config.

This table allows us to examine how cache is configured for a particular host or a service. It’s useful if we need to confirm which hosts/service is using which feature. When we add a new config argument, we must also add it to the serialization method `CacheAllocatorConfig::serialize()`.

## Pool Stats Exporter & AC Stats Exporter

Cache can be divided into multiple cache pools for the purpose of resource isolation. Each cache pool has its own set of stats (allocations, evictions, how many bytes are used). This exporter is responsible for aggregating each cache pool’s stats and upload it to scuba. The scuba table name is: cachelib_admin_pool_stats. Each time we add a new stat to a cache pool, we need to make sure to export this in the pool stats exporter.

AC Stats Exporter is similar to Pool Stats Exporter but is responsible for aggregating stats across allocation classes within a single cache pool. This is one of the most useful stats exporter as the stats here can reveal a lot when debugging issues. The scuba table name is: cachelib_admin_ac_stats. Each time we add a new stat to an allocation class, we need to make sure to export this in the ac stats exporter.

For a detailed break down on what stats we export. Refer to "Monitoring cache health":

* [Pool stats](monitoring/#cachelib-admin-pool-stat)
* [AC stats](monitoring/#cachelib-admin-ac-stats)

## Item Stats Exporter

We offer two variants here. One is always initialized: “default item stats exporter”. This will upload basic item stats (cache key, size, is it a chained item, etc.) to a cachelib-owned scuba table: cachelib_admin_items_stats. In addition, we allow the user to create their custom callback to parse a cachelib item, and also to specify their own scuba table. We do not use logger but instead directly write to scuba as it’s simpler system for us to work with.

Enable custom item stats
```cpp
enableItemStatsSampling(
    const AllocatorT& allocator /* unused */,
    const std::string& scubaTable,
    const std::function<rfe::ScubaData::Sample(typename AllocatorT::Item&)>& cb);
```

## ODS Stats Exporter

This exporter exports only a few stats. They’re stats that describe all the machines running cachelib. We have version stats that describe the allocator version, and the ram and nvm format versions. Allocator version is useful when we add a new feature and would like to know how many hosts has this new feature been rolled out (we bump it on adding such a new feature). Format version is when we make a format change that requires a cold roll.

We also have stats that describe the number of servers running cachelib, how much memory and nvm are managed by cachelib across facebook.

We usually don’t need to change this much. Only change it if we want a new stat that captures something across all the hosts running cachelib.

## Service Data Exporter

This exporter also uploads to ODS. The difference from the “ODS stats exporter” is that this exporter is responsible for all stats for a particular “cache name”. Typically each cache use case has its own unique cache name. We upload stats that describe the entire cache (hit rate, mem size, number of items, number of lookups into flash, etc.). Add any stats here if you want to see it reflected in real time on ODS.
