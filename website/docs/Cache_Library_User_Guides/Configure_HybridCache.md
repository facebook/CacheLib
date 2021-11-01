---
id: Configure_HybridCache
title: Configure HybridCache
---

## Enabling hybrid cache

You can configure the Navy engine (flash cache engine when running in the HybridCache mode) through the NvmCache::Config::navyConfig in the Cache config by using APIs provided in navy::NavyConfig, for example:



<details> <summary> Simple hybrid cache setup </summary>

```cpp
#include "cachelib/allocator/CacheAllocator.h"
LruAllocator::Config lruConfig;

LruAllocator::NvmCacheConfig nvmConfig;
nvmConfig.navyConfig.setBlockSize(4096);
nvmConfig.navyConfig.setSimpleFile(FLAGS_navy_file_name,
                                   FLAGS_dipper_device_size_mb * 1024 *1024 /*fileSize*/,
                                   false /*truncateFile*/);
nvmConfig.navyConfig.blockCache().setRegionSize(16 * 1024 * 1024);

lruConfig.enableNvmCache(nvmConfig);
```

</details>


All settings are optional, unless marked as **"Required"**. The default value is shown after the equal sign `=`.

## Basic configuration

###  Device
 * (**Required**) Set a simple file or RAID files
   * simple file:
     * (**Required**)`file name`
      File/device path with cache.
      * (**Required**) `file size`
      Size (in bytes) of the file/device with cache.
      *  `truncate file = false`
      Default is `false`. If it is `true`, do `ftruncate` on the file to the requested size.

    ```cpp
    navyConfig.setSimpleFile(fileName, fileSize, truncateFile /*optional*/);
    ```
   * RAID files:
     * (**Required**) `RAID paths`
      Multiple files/devices to be used as a single cache. Note they must be identical in size.
     * (**Required**) `file size`
      Size (in bytes) of a single file/device.
     * `truncate file = false`
      Default is `false`. If it is `true`, do `ftruncate` on the file to the requested size.

   ```cpp
   navyConfig.setRaidFiles(raidPaths, fileSize, truncateFile /*optional*/);
   ```


 * `block size = 4096`
Device block size in bytes (minimum IO granularity).
* `device metadata size = 0`
The size of the metadata partition on the Navy device.
* `device max write size = 1024*1024`
This controls what’s the biggest IO we can write to a device. After it is configured, any IO size above it will be split and issued sequentially.

 ```cpp
 navyConfig.setBlockSize(blockSize);
 navyConfig.setDeviceMetadataSize(deviceMetadataSize);
 navyConfig.setDeviceMaxWriteSize(deviceMaxWriteSize);
 ```

### Job scheduler

* (**Required**) `reader threads = 32`
Number of threads available for processing *read* requests.

* (**Required**) `writer threads = 32`
Number of threads available for processing *write* requests and navy-internal operations.


* `request ordering shards = 20`
If it is non-zero, we will enable request ordering where we put requests into 2<sup>N</sup> shards and ensure each shard executes requests in order.

```cpp
navyConfig.setReaderAndWriterThreads(readerThreads, writerThreads);
navyConfig.setNavyReqOrderingShards(navyReqOrderingShards);
```


### Other

* `max concurrent inserts = 1'000'000`
This controls how many insertions can happen in parallel. This is an effective way to avoid too many insertions backing up that drives  up the write latency (it can happen if the use case is too heavy on writes).
* `max parcel memory = 256 (MB)`
 Total memory limit for in-flight parcels. Once this is reached, requests will be rejected until the parcel memory usage gets under the limit.


```cpp
navyConfig.setMaxConcurrentInserts(maxConcurrentInserts);
navyConfig.setMaxParcelMemoryMB(maxParcelMemoryMB);
```


## Navy engine specific configuration

### Large object cache (BlockCache)
These configuration control the behavior of the storage engine that caches
large objects

#### Eviction policy (choose one of the followings):
   * LRU: default policy

   * FIFO: once enabled, LRU will be disabled.

   ```cpp
  enableFifo();
   ```


   * segmented FIFO: once enabled, LRU and FIFO will be disabled.

   ```cpp
   // sFifoSegmentRatio maps to segments in the order from least-important to most-important.
  //  e.g. {1, 1, 1} gives equal share in each of the 3 segments;
  //       {1, 2, 3} gives the 1/6th of the items in the first segment (P0 least important),
  //       2/6th of the items in the second segment(P1),
  //      and finally 3/6th of the items in the third segment (P2).
   enableSegmentedFifo(sFifoSegmentRatio);
   ```


#### Reinsertion policy (choose one of the followings):
  * hits based
If this is enabled, we will reinsert item that had been accessed more than the threshold since the last time it was written into block cache. This can better approximate a LRU than the region-based LRU. Typically users configure this with a region-granularity FIFO policy, or SFIFO policy.  It cannot be enabled when percentage based reinsertion policy has been enabled.

   ```cpp
   enableHitsBasedReinsertion(hitsThreshold);
   ```


   * percentage based
     This is used for testing where a certain fraction of evicted items(governed by the percentage) are always reinserted.The percentage value is between 0 and 100 for reinsertion. It cannot be enabled when hits based reinsertion policy has been enabled.

   ```cpp
   enablePctBasedReinsertion(pctThreshold);
   ```


#### Configuring the write mode

* `clean regions = 1`
How many regions do we reserve for future writes. Set this to be equivalent to your per-second write rate. It should ensure your writes will not have to retry to wait for a region reclamation to finish.

* `in-memory buffers = 0`
A non-zero value enables in-mem buffering for writes. All writes will first go into a region-sized buffer. Once the buffer is full, we will flush the region to the device. This allows BlockCache to internally pack items closer to each other (saves space) and also improves device read latency (regular sized write IOs means better read performance).

   ```cpp
   // Clean Regions and in-mem buffer will be set together.
   // Once in-mem buffer is enabled, it is 2 * clean regions.
   setCleanRegions(cleanRegions, true /* enable in-mem buffers*/);
   ```


* `size classes = []`
A vector of Navy BlockCache size classes (must be multiples of block size). If enabled, Navy will configure regions to allocate rounded up to these size classes and evict regions within a size classs. A given region allocates corresponding to a given size class. By default, objects will be stack allocated irrespective of their size on available regions.
* `region size = 16777216` (16 Mb)
Region size in bytes.

* `data checksum = true`
This controls whether or not BlockCache will verify the item’s value is correct (equivalent to its checksum). This should always be enabled, unless you’re doing your own checksum logic at a higher layer.

```cpp
navyConfig.blockCache()
          .enableSegmentedFifo(sFifoSegmentRatio)
          .enableHitsBasedReinsertion(hitsThreshold)
          .setCleanRegions(cleanRegions, true)
          .useSizeClasses(sizeClasses)
          .setRegionSize(regionSize)
          .setDataChecksum(false);

```

### Small object cache (BigHash)

* (**Required**) `size percentage`
Percentage of space to reserve for BigHash. Set the percentage > 0 to enable BigHash. The remaining part is for BlockCache. The value has to be in the range of [0, 100]. Default value is 0.

* (**Required**) `small item max size` (bytes)
Maximum size of a small item to be stored in BigHash. Must be less than the bucket size.

* `bucket size = 4096` (bytes)
Bucket size in bytes.

* `bucket bloom filter size = 8`
Bloom filter, bytes per bucket. Must be power of two. 0 means bloom filter will not be applied

```cpp
navyConfig.bigHash()
      .setSizePctAndMaxItemSize(bigHashSizePct, bigHashSmallItemMaxSize)
      .setBucketSize(bigHashBucketSize)
      .setBucketBfSize(bigHashBucketBfSize);
```


> `NavyConfig` provides a public function NavyConfig::serialize() so that you can call it to print out the data, e.g.
> ```cpp
> XLOG(INFO) << "Using the following navy config"
>           << folly::toPrettyJson(
>                        folly::toDynamic(navyConfig.serialize()));
> ```

## Admission policy

HybridCache can leverage an admission policy to control burn rate of the underlying nvm devices (e.g. SSD drives). Using a suitable admission policy for your workloads can often not only improve device longevitiy but also improve the cache hit rate. You can configure an admission policy like the following example:

```cpp
auto policy = std::make_shared<cachelib::RejectFirstAP<LruAllocator>>(/* ... */);

cacheConfig.setNvmCacheAdmissionPolicy(std::move(policy));
```

### Configuring probabilistic admission policy
There are 2 types general purpose admission policy: "random" and "dynamic_random" to use probabilistic admission. Users can choose one of them to enable.

#### Random policy
This policy just rejects `P%` of inserts, picking victims randomly. It lets user to reduce IOPS (and so increase flash life time).

  * (**Required**) `admission probability`
Acceptance probability. The value has to be in the range of [0, 1].

 ```cpp
 navyConfig.enableRandomAdmPolicy()
           .setAdmProbability(admissionProbability);
```

#### Dynamic Random policy
*  (**Required**) `admission write rate` (bytes/s)
Average **per day** write rate to target. Default to be 0 if not being  explicitly set, meaning no rate limiting.
* `max write rate = 0` (bytes/s)
The max write rate to device in bytes/s to stay within the device limit of saturation to avoid latency increase. This ensures writing at any given second doesn't exceed this limit despite a possibility of writing more to stay within the target rate above.
*  `admission suffix length = 0`
 Length of suffix in key to be ignored when hashing for probability.
 *  `admission base size = 0`
 Navy item base size of baseProbability calculation. Set this closer to the mean size of objects. The probability is scaled for other sizes by using this size as the pivot.

 ```cpp
 navyConfig.enableDynamicRandomAdmPolicy()
           .setAdmissionWriteRate(admissionWriteRate)
           .setMaxWriteRate(maxWriteRate)
           .setAdmissionSuffixLength(admissionSuffixLen)
           .setAdmissionProbBaseSize(admissionProbBaseSize);
 ```

### Configuring reject first policy

This policy helps if flash cache contains lots of inserted and never accessed items. It maintains a running window (sketch) of keys that were accessed. If a key is inserted for the first time, the policy rejects it. Second inserts get into the cache. A sketch consists of several splits. As times goes, old splits are discarded. With larger split, rejection gets more accurate (less false accepts).

### Dynamic reject (or rate throttle)

This is a smart random reject policy. Users specify the maximum size of data that can be written to the device per day. Policy monitors *write* traffic and as it grows beyond the target (how much can be written up to this time of the day) it starts randomly reject inserts. It prefers to reject larger items to make hit ratio better. This behavior is tunable to allow users to control flash's wearing out.

### Machine learning based admission policy

CacheLib also supports using ML based admission policy to make intelligent decision on what to admit into nvm devices. However, the use of ML policy requires careful analysis of cache workloads, and set up a training pipeline to train the model on a conintuous basis. Please try out the other admission policies first, and if you're not satisfied with them, then reach out directly to the CacheLib team to discuss using a ML-based policy.
