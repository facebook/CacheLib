/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <any>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/cachebench/util/JSONConfig.h"
#include "cachelib/common/Ticker.h"
#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
// Monitor that is set up after CacheAllocator is created and
// destroyed before CacheAllocator is shut down.
class CacheMonitor {
 public:
  virtual ~CacheMonitor() = default;
};

class CacheMonitorFactory {
 public:
  virtual ~CacheMonitorFactory() = default;
  virtual std::unique_ptr<CacheMonitor> create(LruAllocator& cache) = 0;
  virtual std::unique_ptr<CacheMonitor> create(Lru2QAllocator& cache) = 0;
};

// Parse memory tiers configuration from JSON config
struct MemoryTierConfig : public JSONConfig {
  MemoryTierConfig() {}

  explicit MemoryTierConfig(const folly::dynamic& configJson);

  // Returns MemoryTierCacheConfig parsed from JSON config
  MemoryTierCacheConfig getMemoryTierCacheConfig() {
    MemoryTierCacheConfig config = MemoryTierCacheConfig::fromShm();
    config.setRatio(ratio);
    config.setMemBind(NumaBitMask(memBindNodes));
    return config;
  }

  // Specifies ratio of this memory tier to other tiers
  size_t ratio{0};
  // Allocate memory only from specified NUMA nodes
  std::string memBindNodes{""};
};

struct CacheConfig : public JSONConfig {
  // by defaullt, lru allocator. can be set to LRU-2Q.
  std::string allocator{"LRU"};

  // if set, we will persist the cache across cachebench runs. The directory
  // is used to store some metadata about the cache.
  std::string cacheDir{""};

  uint64_t cacheSizeMB{0};
  uint64_t poolRebalanceIntervalSec{0};
  std::string rebalanceStrategy;
  uint64_t rebalanceMinSlabs{1};
  double rebalanceDiffRatio{0.25};
  bool moveOnSlabRelease{false};

  uint64_t htBucketPower{22}; // buckets in hash table
  uint64_t htLockPower{20};   // locks in hash table

  // Hash table config for chained items
  uint64_t chainedItemHtBucketPower{22};
  uint64_t chainedItemHtLockPower{20};

  // time to sleep between MMContainer reconfigures
  uint64_t mmReconfigureIntervalSecs{0};

  // LRU and 2Q params
  uint64_t lruRefreshSec{60};
  double lruRefreshRatio{0.1};
  bool lruUpdateOnWrite{false};
  bool lruUpdateOnRead{true};
  bool tryLockUpdate{false};
  bool useCombinedLockForIterators{false};

  // LRU param
  uint64_t lruIpSpec{0};

  // 2Q params
  size_t lru2qHotPct{20};
  size_t lru2qColdPct{20};

  double allocFactor{1.5};
  // maximum alloc size generated using the alloc factor above.
  size_t maxAllocSize{1024 * 1024};
  size_t minAllocSize{64};

  std::vector<uint64_t> allocSizes{};

  // These specify the number of pools and how keys will
  // be distributed among the pools
  uint64_t numPools{1};
  std::vector<double> poolSizes{1.0};

  // uses a user specified file for caching. If the path specified is a file
  // or raw device, then navy uses that directly. If the path specificied is a
  // directory, we will create a file inside with appropriate size . If a
  // directory is specified by user, cachebench cleans it up at exit. If it is
  // a file, cachebench preserves the file upon exit. User can also specify a
  // number of devices. Cachelib's flash cache engine (Navy) will use them in a
  // raid0 fashion
  std::vector<std::string> nvmCachePaths{};

  // size of the NVM for caching. When more than one device path is
  // specified, this is the size per device path. When this is non-zero and
  // nvmCachePaths is empty, an in-memory block device is used.
  uint64_t nvmCacheSizeMB{0};

  // if 0, use the default. Note this takes away space from NvmCache
  uint64_t nvmCacheMetadataSizeMB{0};

  // list of device identifiers for the device path that can be used to
  // monitor the physical write amplification. If empty, physical write amp is
  // not computed. This can be specified as nvme1n1 or nvme1n2 etc, confirming
  // to be a physical device identifier;
  std::vector<std::string> writeAmpDeviceList{};

  // Navy specific: block size in bytes
  uint64_t navyBlockSize{512};

  // Navy specific: region size in MB
  uint64_t navyRegionSizeMB{16};

  // If non-empty, configures Navy to use FIFO instead of LRU. If there are
  // more than one values provided, it enables segmented fifo with the
  // appropriate ratios.
  std::vector<unsigned int> navySegmentedFifoSegmentRatio{};

  // Number of shards expressed as power of two for request ordering in
  // Navy. If 0, the default configuration of Navy(20) is used.
  uint64_t navyReqOrderShardsPower{21};

  // percentage of the nvm cache size that is dedicated for objects that are
  // smaller than @navySmallItemMaxSize. This size is dedicated for BigHash
  // engine.
  uint64_t navyBigHashSizePct = 50;

  // bucket size for BigHash. This controls the write amplification for small
  // objects in Navy. Every small object write performs a RMW for a bucket.
  uint64_t navyBigHashBucketSize = 4096;

  // Big Hash bloom filter size in bytes per bucket above.
  uint64_t navyBloomFilterPerBucketSize = 8;

  // Small Item Max Size determines the upper bound of an item size that
  // can be admitted into Big Hash engine.
  uint64_t navySmallItemMaxSize = 2048;

  // total memory limit for in-flight insertion operations for NVM. Once this is
  // reached, requests will be rejected until the memory usage gets under
  // the limit.
  uint64_t navyParcelMemoryMB = 1024;

  // use a hits based reinsertion policy with navy
  uint64_t navyHitsReinsertionThreshold{0};

  // use a probability based reinsertion policy with navy
  uint64_t navyProbabilityReinsertionThreshold{0};

  // number of asynchronous worker thread for navy read operation.
  uint32_t navyReaderThreads{32};

  // number of asynchronous worker thread for navy write operation,
  uint32_t navyWriterThreads{32};

  // Max number of concurrent reads/writes in whole Navy
  uint32_t navyMaxNumReads{0};
  uint32_t navyMaxNumWrites{0};

  // Default stack size of Navy fibers when async IO is enabled
  uint32_t navyStackSizeKB{16};

  // qdepth to be used; override if already set automatically
  // by navyMaxNumReads and navyMaxNumWrites
  uint32_t navyQDepth{0};
  // Use either io_uring or libaio for async IO
  bool navyEnableIoUring{true};

  // buffer of clean regions to be maintained free to ensure writes
  // into navy don't queue behind a reclaim of region.
  uint32_t navyCleanRegions{1};

  // The number of RegionManager threads for reclaim and flush
  uint32_t navyCleanRegionThreads{1};

  // disabled when value is 0
  uint32_t navyAdmissionWriteRateMB{0};

  // maximum pending inserts before rejecting new inserts.
  uint32_t navyMaxConcurrentInserts{1000000};

  // enables data checksuming for navy. metadata checksum is enabled by
  // default
  bool navyDataChecksum{true};

  // by default, only store the size requested by the user into nvm cache
  bool truncateItemToOriginalAllocSizeInNvm = false;

  // by default, we do not encrypt content in Navy
  bool navyEncryption = false;

  // number of navy in-memory buffers
  uint32_t navyNumInmemBuffers{30};

  // By default Navy will only flush to device at most 1MB, if larger than 1MB,
  // Navy will split it into multiple IOs.
  uint32_t deviceMaxWriteSize{1024 * 1024};

  // Enable the FDP Data placement mode in the device, if it is capable.
  bool deviceEnableFDP{false};

  // Don't write to flash if cache TTL is smaller than this value.
  // Not used when its value is 0.  In seconds.
  uint32_t memoryOnlyTTL{0};

  // Use Posix Shm instead of SysVShm
  bool usePosixShm{false};

  // Lock memory in the RAM
  bool lockMemory{false};

  // Memory tiers configs
  std::vector<MemoryTierCacheConfig> memoryTierConfigs{};

  // If enabled, we will use the timestamps from the trace file in the ticker
  // so that the cachebench will observe time based on timestamps from the trace
  // instead of the system time.
  bool useTraceTimeStamp{false};

  // If enabled, when printing CacheStats, also print the NvmCounters (could be
  // spammy).
  bool printNvmCounters{false};

  // When set to positive, CacheBench use this as TimeStampTickers bucket ticks
  // For example, a value of 3600 means the threads will
  // always be processing traces from the same hour at any time.
  // When set to 0, TimeStampTicker is not used.
  uint64_t tickerSynchingSeconds{0};

  // Check if ItemDestructor is triggered properly for every item.
  // Be careful that this will keep a record of every item allocated,
  // and won't be dropped after item is removed from cache, it the size
  // is not bounded by the size of cache.
  bool enableItemDestructorCheck{false};
  // enable the ItemDestructor feature, but not check correctness,
  // this verifies whether the feature affects throughputs.
  bool enableItemDestructor{false};

  // If specified, we will not admit any item into NvmCache if their
  // eviction-age is more than this threshold. 0 means no threshold
  uint32_t nvmAdmissionRetentionTimeThreshold{0};

  //
  // Options below are not to be populated with JSON
  //

  // User is free to implement any encryption that conforms to this API
  // If supplied, all payloads into Navy will be encrypted.
  std::function<std::shared_ptr<navy::DeviceEncryptor>()> createEncryptor;

  // User will implement a function that returns NvmAdmissionPolicy.
  // The reason we return as a std::any is because the interface is a virtual
  // template class, and the actual type is determined by @allocator in
  // this config.
  std::function<std::any(const CacheConfig&)> nvmAdmissionPolicyFactory;

  // User can implement a structure that polls stats from CacheAllocator
  // and saves the states to a backend/file/place they prefer.
  std::shared_ptr<CacheMonitorFactory> cacheMonitorFactory;

  // shared pointer of the ticker to support time stamp based cachebench
  // simulation. Stressor uses this to pass the ticker into the cache.
  std::shared_ptr<cachelib::Ticker> ticker;

  // A callback function to get the number of NVM write bytes. Stressor uses
  // this to pass it into the cache.
  std::function<double()> nvmWriteBytesCallback;

  // A nested dynamic for custom config. Customized configs can be put under
  // this field and be consumed during the initialization of the cache.
  folly::dynamic customConfigJson;

  explicit CacheConfig(const folly::dynamic& configJson);

  CacheConfig() {}

  std::shared_ptr<RebalanceStrategy> getRebalanceStrategy() const;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
