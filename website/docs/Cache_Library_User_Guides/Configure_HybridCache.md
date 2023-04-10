---
id: Configure_HybridCache
title: Configure HybridCache
---

## Enabling hybrid cache

You can configure the Navy engine (flash cache engine when running in the HybridCache mode) through the `NvmCache::Config::navyConfig` in the Cache config by using APIs provided in `navy::NavyConfig`, for example:



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


All settings are optional, unless marked as **"Required"**.

## How to set Navy settings
### 1. Common Settings - Device
 ```cpp
  navyConfig.setSimpleFile(fileName, fileSize, truncateFile /*optional*/);
  navyConfig.setBlockSize(blockSize);
  navyConfig.setDeviceMetadataSize(deviceMetadataSize);
  navyConfig.setDeviceMaxWriteSize(deviceMaxWriteSize);
 ```
 **OR**
  ```cpp
  navyConfig.setRaidFiles(raidPaths, fileSize, truncateFile /*optional*/);
  navyConfig.setBlockSize(blockSize);
  navyConfig.setDeviceMetadataSize(deviceMetadataSize);
  navyConfig.setDeviceMaxWriteSize(deviceMaxWriteSize);
 ```
* (**Required**) `file name`/ `RAID paths`

   * `file name` (for simple file): File/device path with cache.
   * `RAID paths` (for RAID files): Multiple files/devices to be used as a single cache. Note they must be identical in size.

* (**Required**) `file size`

    Size (in bytes) of a single file/device with cache.

*  `truncate file` = `false` (default)

    Default is `false`. If it is `true`, do `ftruncate` on the file to the requested size.

* `block size` = `4096` (default)

  Device block size in bytes (minimum IO granularity).

* `device metadata size` = `0` (default)

  The size of the metadata partition on the Navy device.

* `device max write size` = `1024 * 1024` (default)

  This controls what’s the biggest IO we can write to a device. After it is configured, any IO size above it will be split and issued sequentially.


### 2. Common Settings - Job Scheduler
```cpp
navyConfig.setReaderAndWriterThreads(readerThreads, writerThreads);
navyConfig.setNavyReqOrderingShards(navyReqOrderingShards);
```

* (**Required**) `reader threads` = `32` (default)

  Number of threads available for processing *read* requests.

* (**Required**) `writer threads` = `32` (default)

  Number of threads available for processing *write* requests and navy-internal operations.


* `request ordering shards` = `20` (default)

  If it is non-zero, we will enable request ordering where we put requests into 2<sup>N</sup> shards and ensure each shard executes requests in order.


### 3. Common Settings - Other
 ```cpp
 navyConfig.setMaxConcurrentInserts(maxConcurrentInserts);
 navyConfig.setMaxParcelMemoryMB(maxParcelMemoryMB);
 ```

* `max concurrent inserts` = `1'000'000` (default)

 This controls how many insertions can happen in parallel. This is an effective way to avoid too many insertions backing up that drives  up the write latency (it can happen if the use case is too heavy on writes).


* `max parcel memory` = `256(MB)` (default)

 Total memory limit for in-flight parcels. Once this is reached, requests will be rejected until the parcel memory usage gets under the limit.


### 4. Admission Policy Settings
There are 2 types of admission policy: **"random"** and **"dynamic_random"**. Users can choose one of them to enable.

* "random" policy
  ```cpp
 navyConfig.enableRandomAdmPolicy()
           .setAdmProbability(admissionProbability);
 ```
  * (**Required**) `admission probability`

  Acceptance probability. The value has to be in the range of [0, 1].

* "dynamic_random" policy
 ```cpp
 navyConfig.enableDynamicRandomAdmPolicy()
           .setAdmWriteRate(admissionWriteRate)
           .setMaxWriteRate(maxWriteRate)
           .setAdmSuffixLength(admissionSuffixLen)
           .setAdmProbBaseSize(admissionProbBaseSize);
 ```
  *  (**Required**) `admission write rate` (bytes/s)

  Average **per day** write rate to target. Default to be 0 if not being  explicitly set, meaning no rate limiting.
  * `max write rate`  = `0 (bytes/s)` (default)

  The max write rate to device in bytes/s to stay within the device limit of saturation to avoid latency increase. This ensures writing at any given second doesn't exceed this limit despite a possibility of writing more to stay within the target rate above.
  *  `admission suffix length` = `0` (default)

  Length of suffix in key to be ignored when hashing for probability.
  *  `admission base size` = `0` (default)

  Navy item base size of baseProbability calculation. Set this closer to the mean size of objects. The probability is scaled for other sizes by using this size as the pivot.


### 5. Engine Settings - Block Cache
```cpp
navyConfig.blockCache()
          .enableSegmentedFifo(sFifoSegmentRatio)
          .enableHitsBasedReinsertion(hitsThreshold)
          .setCleanRegions(cleanRegions)
          .setRegionSize(regionSize)
          .setDataChecksum(false);
```
* eviction policy (choose one of the followings):
   * LRU: default policy

   * FIFO: once enabled, LRU will be disabled.
   ```cpp
    navyConfig.blockCache().enableFifo();
   ```

   * segmented FIFO: once enabled, LRU and FIFO will be disabled.
   ```cpp
   // sFifoSegmentRatio maps to segments in the order from least-important to most-important.
   //  e.g. {1, 1, 1} gives equal share in each of the 3 segments;
   //       {1, 2, 3} gives the 1/6th of the items in the first segment (P0 least important),
   //                 2/6th of the items in the second segment(P1),
   //                 and finally 3/6th of the items in the third segment (P2).
   navyConfig.blockCache().enableSegmentedFifo(sFifoSegmentRatio);
   ```

* reinsertion policy (choose one of the followings but not both):
  * hits based

   If this is enabled, we will reinsert item that had been accessed more than the threshold since the last time it was written into block cache. This can better approximate a LRU than the region-based LRU. Typically users configure this with a region-granularity FIFO policy, or SFIFO policy.  It cannot be enabled when percentage based reinsertion policy has been enabled.
   ```cpp
   navyConfig.blockCache().enableHitsBasedReinsertion(hitsThreshold);
   ```

   * percentage based

   This is used for testing where a certain fraction of evicted items(governed by the percentage) are always reinserted.The percentage value is between 0 and 100 for reinsertion. It cannot be enabled when hits based reinsertion policy has been enabled.
   ```cpp
   navyConfig.blockCache().enablePctBasedReinsertion(pctThreshold);
   ```

* `clean regions` and `in-memory buffer`

  * `clean regions` = `1` (default)

    How many regions do we reserve for future writes. Set this to be equivalent to your per-second write rate. It should ensure your writes will not have to retry to wait for a region reclamation to finish.

  * `in-memory buffer` = `2 * clean regions` (default)

    All writes will first go into a region-sized buffer. Once the buffer is full, we will flush the region to the device. This allows BlockCache to internally pack items closer to each other (saves space) and also improves device read latency (regular sized write IOs means better read performance).

  ```cpp
   navyConfig.blockCache().setCleanRegions(cleanRegions);
  ```

* `region size` = `16777216 (16 Mb)` (default)

 Region size in bytes.

* `data checksum` = `true` (default)

  This controls whether or not BlockCache will verify the item’s value is correct (equivalent to its checksum). This should always be enabled, unless you’re doing your own checksum logic at a higher layer.

### 6. Engine Settings - BigHash
```cpp
navyConfig.bigHash()
      .setSizePctAndMaxItemSize(bigHashSizePct, bigHashSmallItemMaxSize)
      .setBucketSize(bigHashBucketSize)
      .setBucketBfSize(bigHashBucketBfSize);
```

* (**Required**) `size percentage`

  Percentage of space to reserve for BigHash. Set the percentage > 0 to enable BigHash. The remaining part is for BlockCache. The value has to be in the range of [0, 100]. Default value is 0.

* (**Required**) `small item max size` (bytes)

  Maximum size of a small item to be stored in BigHash. Must be less than the bucket size.

* `bucket size` = `4096 (bytes)` (default)

  Bucket size in bytes.

* `bucket bloom filter size` = `8` (default)

  Bloom filter, bytes per bucket. Must be power of two. 0 means bloom filter will not be applied

## NavyConfig Data Output

`NavyConfig` provides a public function `serialize()` so that users can call to print out the configured Navy settings, e.g.

 ```cpp
 XLOG(INFO) << "Using the following navy config"
           << folly::toPrettyJson(
                        folly::toDynamic(navyConfig.serialize()));
 ```

## Admission

HybridCache can leverage an admission policy to control burn rate of the underlying nvm devices (e.g. SSD drives). Using a suitable admission policy for your workloads can often not only improve device longevitiy but also improve the cache hit rate. You can configure an admission policy like the following example:
```cpp
auto policy = std::make_shared<cachelib::RejectFirstAP<LruAllocator>>(/* ... */);

cacheConfig.setNvmCacheAdmissionPolicy(std::move(policy));
```

### Random reject

This policy just rejects `P%` of inserts, picking victims randomly. It lets user to reduce IOPS (and so increase flash life time).

### Reject first

This policy helps if flash cache contains lots of inserted and never accessed items. It maintains a running window (sketch) of keys that were accessed. If a key is inserted for the first time, the policy rejects it. Second inserts get into the cache. A sketch consists of several splits. As times goes, old splits are discarded. With larger split, rejection gets more accurate (less false accepts).

### Dynamic reject (or rate throttle)

This is a smart random reject policy. Users specify the maximum size of data that can be written to the device per day. Policy monitors *write* traffic and as it grows beyond the target (how much can be written up to this time of the day) it starts randomly reject inserts. It prefers to reject larger items to make hit ratio better. This behavior is tunable to allow users to control flash's wearing out.

### ML-based admission policy

CacheLib also supports using ML based admission policy to make intelligent decision on what to admit into nvm devices. However, the use of ML policy requires careful analysis of cache workloads, and set up a training pipeline to train the model on a conintuous basis. Please try out the other admission policies first, and if you're not satisfied with them, then reach out directly to the CacheLib team to discuss using a ML-based policy.
