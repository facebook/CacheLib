---
id: Enabling_WSA
title: Enabling WSA
---

### Enabling Working Set Analysis logging in Cache Library

> :exclamation:**Note: only use cachelib's built-in WSA if your use-case uses cachelib as a typical lookaside KV cache. If you have complex business logic (e.g. reading part of objects, mutating parts of objects, double read on misses, read cache before write, etc.), you should consider building your own WSA logger and log directly from within your application code**

To enable CacheLib's built-in Working Set Analysis, add the appropriate options to the config before instantiating the cache:

```cpp
// Use cacheConfig to instantiate the cache.
facebook::cachelib::CacheAdmin::enableWorkingSetAnalsysis(cacheConfig);
// ...
// Construct the cache.
 facebook::cachelib::LruAllocator cache{cacheConfig};
```

#### Advanced configuration

Working Set Analysis logging component has a number of advanced configuration options that you can control by setting some parameters while you initialize the `CacheAdmin::Config::WSAConfig` object. The following options are available:

```cpp
// Path to the sampling config for Working Set Analsysis in configerator.
folly::StringPiece samplingConfig;

// Number of shards for Working Set Analysis logging to use.
// Increasing shards reduces lock contention during logging at the expense
// of increased duplication (identical requests on different shards are not
// merged).
unsigned int nShards;

// Collection interval for Working Set Analysis logging library.
// Under normal circumstances the library will persist data once per
// interval. Multiple identical requests that happen during this interval
// are collapsed.
std::chrono::seconds collectionInterval;

// Maximum backlog (in records) for the Working Set Analysis library.
unsigned int maxBacklog;

// Maximum buffer size for copying keys in Working Set Analysis logging.
size_t maxBufferSize;
```

To configure the advanced configuration options for Working Set Analysis, call the `enableWorkingSetAnalysis()` method.

> :exclamation:**Note: you must configure this in the config object BEFORE you start CacheLib!**

```cpp
facebook::cachelib::CacheAdmin::WSAConfig wsaConfig;
wsaConfig.nShards = 12;
wsaConfig.collectionInterval = std::chrono::seconds(120);
wsaConfig.maxBacklog = 1024;

// Optional, define this if you want to group "unrelated" keys together
// For example, for each request if you need to look up two separate keys in cache
//  foo-1 and foo-2
// In this case, you can define a transformer that chops off suffixes such as "-1" and "-2"
// from the key. It allows the logger to always log keys that have prefix foo. It's useful
// if the behavior of all foo-* are the same.
wsaConfig.keyTransformer = /* user defined transformer */;

// Optional, define this if you want to surface some relationship among your keys.
// For example, if you run a multi-tenant service, some of your keys belong to
// the same use case, you can define this function that returns the use case
// name for a key. This will allow us to examine your cache's workload use-case by use-case.
wsaConfig.keyGroupingFn = /* user defined grouping function */;

// cacheConfig is the config object for the cache.
// This line MUST be before creating CacheLib instance.
facebook::cachelib::CacheAdmin::enableWorkingSetAnalysis(cacheConfig, wsaConfig);
// ...
// Construct the cache
facebook::cachelib::LruAllocator cache{cacheConfig};
```

### Sampling configuration

Working Set Analysis logging library has a deterministic sampling component that determines operations on what keys get recorded. It is controlled through configerator.

The default config file for Cache Library is specified by the `kWsaSamplingConfig` constant in `CacheAdmin.h` (by default this is [source/graphene/working_set/cachelib_working_set_analysis.cinc](https://fburl.com/diffusion/ald97i9p). Currently we support customizing sampling rates on a per region and per-use case (cache name) basis. Setting sampling rate to 0 turns off logging for the use case. In general , if an operation on a key is in the Working Set Analysis dataset, every other operation on that key is also logged.

To enable sampling, you need to add a selector matching your tier to the config file as follows (see example [diff](https://www.internalfb.com/intern/diff/D17211216/)):

```cpp
// ...
CachelibSamplingConfig(
    selector=GeographySelector(type=GeographySelectorType.GLOBAL),
    samplingRate=5000,
    cacheNameFilter=CachelibCacheNameFilter(
        cacheNames={"&lt;your cache name&gt;"}
    ),
),
// ...
```

The sampling rate you decide will impact the footprint in Hive and potentially the CPU overhead for logging. We recommend a default value of 5,000 (e.g., 1 in 5,000 keys will be logged deterministically). Increase it if you suspect there’s too much noise in the data.

> :exclamation:**NOTE**: This config is re-loaded at runtime when it gets updated. So it will take effect immediately when it lands. Please canary this change on your tier carefully.

For more information on sampling configuration, see [Working Set Sampler Configuration](https://www.internalfb.com/intern/wiki/Core_Data/Graphene/Working_Set_Analysis/Sampler_Configuraiton/).

### What do we log?

For each cachelib operation, we log the following:

### CacheLib Operation Results
Refer to `AllocatorApiResult` in `cachelib/common/EventInterface.h`.
```cpp
// Enum to describe possible outcomes of Allocator API calls.
enum class AllocatorApiResult : uint8_t {
  FAILED = 0,              // Hard failure.
  FOUND = 1,               // Found an item in a 'find' call.
  NOT_FOUND = 2,           // Item was not found in a 'find' call.
  NOT_FOUND_IN_MEMORY = 3, // Item was not found in memory with NVM enabled.
  ALLOCATED = 4,           // Successfully allocated a new item.
  INSERTED = 5,            // Inserted a new item in the map.
  REPLACED = 6,            // Replaced an item in a map.
  REMOVED = 7,             // Removed an item.
  EVICTED = 8,             // Evicted an item.
};
```

* The key (e.g., `cdn:v/t31.0-8/fr/cp0/e15/...`)
* The operation (e.g., `FIND`, `REMOVE`, `INSERT_FROM_NVM`, etc.)
* The operation result (`NOT_FOUND`, `FOUND`, `REMOVED`, etc.)
* The operation time (to determine the gap between `GET`'s and `SET`'s for a given key)
* The total size in bytes (to simulate theoretical size given different retention)
* The sampling rate (to extrapolate total footprint based on size)
* The Cachelib use case (e.g., `big_cache`)
* The host name and region of the box logging
