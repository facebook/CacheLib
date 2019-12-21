#pragma once

#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/cachebench/util/JSONConfig.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
struct CacheConfig : public JSONConfig {
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

  uint64_t lruRefreshSec{60};
  double lruRefreshRatio{0.1};
  bool lruUpdateOnWrite{false};
  bool lruUpdateOnRead{true};
  bool tryLockUpdate{false};
  uint64_t lruIpSpec{0};

  double allocFactor{1.5};
  std::vector<uint64_t> allocSizes{};

  // These specify the number of pools and how keys will
  // be distributed among the pools
  uint64_t numPools{1};
  std::vector<double> poolSizes{1.0};

  // Whether or not we preallocate memory to each pool
  bool provisionPool{false};

  // The size of dipper cache to use. Default to not use dipper. This is the
  // overall size of the dipper resources
  uint64_t dipperSizeMB{0};

  // What dipper backend to use. Currently only support navydipper. default
  // disabled
  std::string dipperBackend{""};

  // uses a user specified file for caching. If the path specified is a file,
  // then navy uses that directly. If the path specificied is a directory, we
  // will create a file inside with appropriate size . If a directory is
  // specified by user, cachebench cleans it up at exit. If it is a file,
  // cachebench preserves the file upon exit
  std::string dipperFilePath{};

  // optional path to a device for bigdipper mode. When this is used, we
  // treat device path and file path as is and use them directly for big
  // dipper operations. If this is not specified, we use the file path as
  // device path as well by creating  a new file inside that.
  std::string dipperDevicePath{""};

  // if set, disregards the file path and device path. Uses the in-memory
  // device
  bool dipperNavyUseMemoryDevice{false};

  // list of device identifiers for the device path that can be used to
  // monitor the physical write amplification. If empty, physical write amp is
  // not computed. This can be specified as nvme1n1 or nvme1n2 etc, confirming
  // to be a physical device identifier; where as the dipperDevicePath can be
  // specified as any /dev that could point to a logical raid device.
  std::vector<std::string> writeAmpDeviceList{};

  // if enabled passes down the appropriate options to use DirectIO
  bool dipperUseDirectIO{true};

  // bloom filter size for big dipper configuration.
  uint64_t dipperBloomSizeMB{1024};

  // number of asynchronous workers for dipper.
  uint64_t dipperAsyncThreads{48};

  // if we should explicitly disable dipper level request ordering for
  // backends that do its own ordering. This can not be turned off for
  // backends that need ordering.
  bool dipperDisableReqOrdering{false};

  // only for big dipper.
  uint64_t dipperBucketSizeKB{};

  // Navy specific: device block size, bytes.
  uint64_t dipperNavyBlock{512};

  // If true, Navy will use region-based LRU. False it will be using FIFO.
  bool dipperNavyUseRegionLru{true};

  // Navy specific: size classes. Must be multiples of @dipperNavyBlock.
  std::vector<uint64_t> dipperNavySizeClasses{512,      2 * 512,  3 * 512,
                                              4 * 512,  6 * 512,  8 * 512,
                                              12 * 512, 16 * 512, 32 * 512};

  // Determins how much of the device is given to big hash engine in Navy
  uint64_t dipperNavyBigHashSizePct = 50;

  // Big Hash Bucket Size determines how big each bucket is and what
  // is the phsyical write granularity onto the device.
  uint64_t dipperNavyBigHashBucketSize = 4096;

  // Big Hash bloom filter size per bucket
  uint64_t dipperNavyBloomFilterPerBucketSize = 8;

  // Small Item Max Size determines the upper bound of an item size that
  // can be admitted into Big Hash engine.
  uint64_t dipperNavySmallItemMaxSize = 2048;

  // total memory limit for in-flight parcels. Once this is reached,
  // requests will be rejected until the parcel memory usage gets under the
  // limit.
  uint64_t dipperNavyParcelMemoryMB = 1024;

  // uses stack allocation mode for navy.
  bool dipperNavyUseStackAllocation = false;

  // size of the internal navy read buffer when using stack allocation. stack
  // allocaiton is enabled when the size classes are not specified.
  uint64_t dipperNavyStackAllocReadBufSizeKB = 32;

  // use a hits based reinsertion policy with navy
  uint64_t navyHitsReinsertionThreshold{0};

  // use a probability based reinsertion policy with navy
  uint64_t navyProbabilityReinsertionThreshold{0};

  // 2Q params
  size_t lru2qHotPct{20};
  size_t lru2qColdPct{20};

  // by default, only store the size requested by the user into nvm cache
  bool truncateItemToOriginalAllocSizeInNvm = false;

  explicit CacheConfig(const folly::dynamic& configJson);

  CacheConfig() {}

  std::shared_ptr<RebalanceStrategy> getRebalanceStrategy();
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
