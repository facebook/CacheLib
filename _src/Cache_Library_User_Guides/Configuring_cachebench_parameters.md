---
id: Configuring_cachebench_parameters
title: Configuring cachebench parameters
---


## Command line parameters
Cachebench takes command line parameters to control its behavior. The following are the semantics of the command line parameters:

### JSON test configuration

`--json_test_config` is the most important command line parameter that is needed for  specifying the workoad and cache configuration for cachebench.  See the section below on JSON config for more details.

### Watching progress

While the benchmark runs, you can monitor the progress so far. The interval for progress update can be configured using the `--progress` and specifying a duration in seconds.

### Recording periodic stats

While the benchmark runs, you can have cachebench output a snapshot of its internal stats to a file periodically. To do this,   you have to pass a suitable file location to  `--progress_stats_file`. cachebench will appendd stats to this file every `--progress` interval.

### Stopping after a certain duration

If you would like cachebench to terminate after running for X hours, you can use `--timeout_seconds` to pass a suitable timeout.

### Sample JSON test config

Cachebench takes in a json config file that provides the workload and cache configuration. The following is a sample json config file:

```
{
   "cache_config" : {
     "cacheSizeMB" : 512,
     "poolRebalanceIntervalSec" : 1,
     "moveOnSlabRelease" : false,
     "numPools" : 2,
     "poolSizes" : [0.3, 0.7]
   },

   "test_config" : {
     "numOps" : 100000,
     "numThreads" : 32,
     "numKeys" : 1000000,
     "distribution" :  "range",

     "opDelayBatch" : 1,
     "opDelayNs" : 200,

     "keySizeRange" : [1, 8, 64],
     "keySizeRangeProbability" : [0.3, 0.7],

     "valSizeRange" : [1, 32, 10240, 409200],
     "valSizeRangeProbability" : [0.1, 0.2, 0.7],

     "getRatio" : 0.15,
     "setRatio" : 0.8,
     "delRatio" : 0.05,
     "keyPoolDistribution": [0.4, 0.6],
     "opPoolDistribution" : [0.5, 0.5]
   }
}

```

This config file controls the parameters for the cache and the generated synthetic workload in two separate sections.

## Tuning workload parameters

You can tune the workload parameters by modifying the `test_config` portion of the json file. The workload generator operates over a key space and their associated sizes. It generates cachebench operations to be executed for those keys.

### Duration of replay

To run cachebench operation for longer, increase the `numOps` appropriately in the config file.

### Number of benchmark threads

You can adjust `numThreads` to run the benchmark with more threads. Running with more threads should increase throughput until you run out of cpu or hit other bottlenecks from resource contention. For in-memory workloads, it is not recommended to set this beyond the  hardware concurrency supported on your machine.

### Number of keys in cache

To adjust the working set size of the cache, you can increase or decrease the `numKeys` that the workload picks from.

### Operation ratios

Cachebench picks operation types by its specified popularity ratios. The following list the supported operation types:
* `getRatio`
Generates a get request resulting in `find` API call.
* `setRatio`
Generates a set request by overriding any previous version of the key if it exists. This results in a call to the `allocate()` API, followed by a call to the `insertOrReplace()` API.
* `delRatio`
Generates a remove request to remove a key from the cache.
* `addChainedRatio`
Generates operations that allocate a chained allocation and adds it to the existing key. If the key is not present, it is created.
* `loneGetRatio`
Generates a get request for a key that is definitely not present in the cache to simulate one-hit-wonders or churn.

In conjuction with these operations, `enableLookaside` emulates a behavior where missing keys are set in the cache. When this is used, `setRatio` is usually not configured.

### Workload generator

You can configure three types of workload generators through the `generator` parameter by specifying the corresponding identifier string.

* **workload**
Generates keys and popularity ahead of time. This is the generator with the lowest run time overhead and hence is useful for measuring maximum throughput. The generator however consumes memory to keep keys and generated cache operations in memory and is not suitable when your memory footprint needs to be contained.
* **online**
Generates keys and popularity online. Has very low overhead in terms of memory, but consumes marginal CPU to generate synthetic workload.
* **replay**
Replays a trace file passed in. Tracefile should contain lines with csv separated key, size, and number of accesses.

### Popularity and Size distribution

cachebench supports generating synthetic workloads using a few techniques. The technique is configured through the *distribution* argument. Based on the selected technique there can be additional parameters that can be configured. The supported techniques are

* **default**
Generates popularity of keys through a discrete distribution specified in  *popularityBuckets* and *popularityWeights* parameter. Discrete sizes are generated through a discrete distribution specified through `valSizeRange` and `valSizeRangeProbability`. The value size configuration can be provided inline as an array or through a `valSizeDistFile` in json format.

* **normal**
Uses normal workload distribution for popularity of keys as opposed to discrete popularity buckets. For value sizes, it supports both discrete and continuous value size distribution. To use discrete value size distribution, the `valSizeRangeProbabilithy` should have same number of values as `valSizeRange` array. When `valSizeRangeProbabilithy` contains one less member than `valSizeRange`, we interpret the probability as corresponding to each interval in `valSizeRange` and use a piecewise_constant_distribution.

In all above setups, cachebench overrides the `valSizeRange` and `vaSizeRangeProbability` from inline json array if `valSizeDistFile` is present.

### Throttling the benchmark

To measure the performance of HW at a certain throughput, cachebench can be artificially throttled by   specifying a non-zero `opDelayNs`, that is applied every `opDelayBatch` worth of operations per thread. To run un-throttled, set `opDelayNs` to zero.

### Consistency checking

You can enable runtime consistency checking of the APIs through cachebench. In this mode, cachebench validates the correctness semantics of API. This is useful when you make a cache to CacheLib and want to validate any data races resulting in incorrect API semantics.


### Populating items

You can enable *populateItem* to fill cache items with random bytes. When consistency mode is enabled, we populate the item automatically with unique values for validation.

## Tuning DRAM cache parameters

The `cache_config` section specifies knobs to control how the cache is configured. The following options are available to configure the DRAM cache parameters. DRAM cache parameters come into play when using hybrid cache as well as stand-alone DRAM cache mode.

### DRAM cache  size

You can set `cacheSizeMB` to specify the size of the DRAM cache.

### Allocator type and its eviction parameters

CacheLib supports LruAllocator and Lru2QAllocator to choose from. You can specify this by setting the *allocator* to "LRU" or "LRU-2Q". Based on the type you choose you can configure the corresponding properties of DRAM eviction.

Common options for  LruAllocator and Lru2QAllocator:
* `lruRefreshSec`
Seconds since last access that initiates a bump on access.
* `lruRefreshRatio`
Lru refresh time specified as a ratio of the eviction age.
* `lruUpdateOnRead`
Controls if read accesses lead to updating LRU position.
* `lruUpdateOnWrite`
Controls if write accesss lead to updating LRU position.
* `tryLockUpdate`
Skips updating the LRU position on contention.

Options for LruAllocator:
* `lruIpSpec`
Insertion point expressed as power of two.

Options for Lru2QAllocator:
* `lru2qHotPct`
Percentage of LRU dedicated for hot items
* `lru2qColdPct`
Percentage of LRU dedicated for cold items.

For more details on the semantics of these parameters, see the documentation in [Eviction Policy guide](eviction_policy).

### Pools

The DRAM cache can be split into multiple pools. To create the pools, you need to specify `numPools` to the required number of pools and set `poolSizes` array  to represent  the relative sizes of the pools.

When using pools, you can tune the workload to generate a different workload per pool. To split the keys and operations across pools, specify the following:

* Breakdown of keys through `keyPoolDistribution` array where each value represents the relative footprint of keys from `numKeys`.
* Breakdown of operations per pool through `opPoolDistribution` where each value represents the relative footprint of `numOps` across pools.

You can specify a seperate array of workload config that describes the key, size and popularity distribution per pool through `poolDistributionConfig`. If not specified, the global configuration is applied across all the pools.

### Allocation sizes

You can specify custom allocation sizes by passing in an `allocSizes` array. If `allocSizes` is not present, we use default allocation sizes with a factor of 1.5, starting from 64 bytes to 1MB. To control allocation sizes through alloc factor, you can specify `allocFactor` as a double and set `minAllocSize` and `maxAllocSize`.

### Access config parameters

CacheLib uses a hashtable to index keys. The configuration of the hashtable can have a big impact on throughput. `htBucketPower` controls the number of hashtable buckets and `htLockPower` configures the number of locks.  Usually, these should be configured in conjunction with the observed numItems in DRAM when the cache warms up.  See


### Pool rebalancing

To enable cachelib pool rebalancing techniques, you can set `poolRebalanceIntervalSec`. The default strategy is to randomly release a slab to test for correctness. You can configure this to your preference by setting `rebalanceStrategy` as "tail-age" or "hits". You can also specify `rebalanceMinSlabs` and `rebalanceDiffRatio` to configure this further per documentation in [Pool rebalancing guide](pool_rebalance_strategy).

## Hybrid cache parameters

Hybrid cache parameters are configured under the `cache_config` section. To enable hybrid cache for cachebench, you need to specify a non-zero value to the `nvmCacheSizeMB` parameter.

### Storage file/device/directory path info

You can configure hybrid cache in multiple modes.  By default, if you set only `nvmCacheSizeMB` and nothing else, cachebench will use an in-memory file device for simplicity. This is often used to test correctness quickly. To use an actual non-volatile medium, you can configure `nvmCachePaths`, which is taken as an array of strings.

If `nvmCachePaths` is set to a single element array that is a  directory, cachebench will create a suitable file inside the path and clean it up upon exit. Instead if `nvmCachePaths` is single element array referring to a file or a raw device, cachebench will use it as is and leave it as is upon exit.  If the file specified is a regular file and is not to the specified size, CacheLib will try to fallocate to the necessary size. If more than one path is specified, CacheLib will use software RAID-0 across them and treat each file to be of `nvmCacheSizeMB`.  By default, CacheLib uses direct io.

###  Monitoring write amplification

CacheBench can monitor the write-amplification of supported underlying devices if you specify them through `writeAmpDeviceList` as an array of device paths. If the device is unsupported, an exception is logged, but the test proceeds. If this is empty, no monitoring is performed.

###  Storage engine parameters

Set the following parameters to control the performance of the hybrid cache storage engine. See [Hybrid Cache](HybridCache) for more details.

* `navyReaderThreads`  and `navyWriterThreads`
Control the reader and writer thread pools.
* `navyAdmissionWriteRateMB`
Throttle limit for logical write rate to maintain device endurance limit.
* `navyMaxConcurrentInserts`
Throttle limit for in-flight hybrid cache writes.
* `navyParcelMemoryMB`
Throttle limit for the memory footprint of in-flight writes.
* `navyDataChecksum`
Enables check-summing data in addition to the headers.
* `navyEncryption`
Enables transparent device level encryption.
* `navyReqOrderShardsPower`
Number of shards used for request ordering. The default is 21, corresponding to 2 million shards. The more shards, the less false positives and better concurrency. But this plateus beyond a certain number.
* `truncateItemToOriginalAllocSizeInNvm`
Truncates item to allocated size to optimize write performance.
* `deviceMaxWriteSize`
This controls the largest IO size we will write to the device. Any IO above this size will be split up into multiple IOs.

###  Small item engine parameters

Use the following options to tune the performance of the Small Item engine (BigHash): BigHash operates a FIFO cache on SSD and is optimized for caching small objects.

* `navySmallItemMaxSize`
Object size threshold for small item engine.
* `navyBigHashSizePct`
When non-zero enables small item engine and its relative size.
* `navyBigHashBucketSize`
Bucket size for small item engine.
* `navyBloomFilterPerBucketSize`
Size in bytes for the bloom filter per bucket.

###  Large item engine parameters

Use the following options to tune the Large Item engine (BlockCache): Block cache is designed for caching objects that are around or larger than device block size. It can support variety of eviction policies from FIFO/LRU/SegmentedFIFO and can operate with stacked mode or size classes.

* `navyBlockSize`
Underlying device block size for IO alignment.
* `navySizeClasses`
By default, cachebench uses a pre-determined array of sizes. Values are stored in slots rounded up to this size.  The default value is overridden by passing an array of sizes explicitly to control size classes. If an empty array is passed, Navy uses stacked mode.
* `navySegmentedFifoSegmentRatio`
By default Navy uses coarse grained LRU. To use FIFO, this parameter is set to an array with single value. To use segmented FIFO, this parameter is configured to control the number of segments by  specifying their ratios.
* `navyHitsReinsertionThreshold`
Control the threshold for reinserting items by their number of hits.
* `navyProbabilityReinsertionThreshold`
Control the probability based reinsertion of items.
* `navyNumInmemBuffers`
Number of memory buffers used to optimize write performance.
* `navyCleanRegions`
When un-buffered, the size of the clean regions pool.
* `navyRegionSizeMB`
This controls the region size to use for BlockCache. If not specified, 16MB will be used. See [Configure HybridCache](Configure_HybridCache) for more details.
