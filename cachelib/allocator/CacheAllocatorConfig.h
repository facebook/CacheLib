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

#include <folly/Optional.h>

#include <chrono>
#include <functional>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/MM2Q.h"
#include "cachelib/allocator/MemoryMonitor.h"
#include "cachelib/allocator/MemoryTierCacheConfig.h"
#include "cachelib/allocator/NvmAdmissionPolicy.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/common/EventInterface.h"
#include "cachelib/common/Throttler.h"

namespace facebook {
namespace cachelib {

// Config class for CacheAllocator.
template <typename CacheT>
class CacheAllocatorConfig {
 public:
  using AccessConfig = typename CacheT::AccessConfig;
  using ChainedItemMovingSync = typename CacheT::ChainedItemMovingSync;
  using RemoveCb = typename CacheT::RemoveCb;
  using ItemDestructor = typename CacheT::ItemDestructor;
  using NvmCacheEncodeCb = typename CacheT::NvmCacheT::EncodeCB;
  using NvmCacheDecodeCb = typename CacheT::NvmCacheT::DecodeCB;
  using NvmCacheDeviceEncryptor = typename CacheT::NvmCacheT::DeviceEncryptor;
  using MoveCb = typename CacheT::MoveCb;
  using NvmCacheConfig = typename CacheT::NvmCacheT::Config;
  using MemoryTierConfigs = std::vector<MemoryTierCacheConfig>;
  using Key = typename CacheT::Key;
  using EventTrackerSharedPtr = std::shared_ptr<typename CacheT::EventTracker>;
  using Item = typename CacheT::Item;

  // Set cache name as a string
  CacheAllocatorConfig& setCacheName(const std::string&);

  // Set cache size in bytes. If size is smaller than 60GB (64'424'509'440),
  // then we will enable full coredump. Otherwise, we will disable it. The
  // reason we disable full coredump for large cache is because it takes a
  // long time to dump, and also we might not have enough local storage.
  CacheAllocatorConfig& setCacheSize(size_t _size);

  // Set default allocation sizes for a cache pool
  CacheAllocatorConfig& setDefaultAllocSizes(std::set<uint32_t> allocSizes);

  // Set default allocation sizes based on arguments
  CacheAllocatorConfig& setDefaultAllocSizes(
      double _allocationClassSizeFactor,
      uint32_t _maxAllocationClassSize,
      uint32_t _minAllocationClassSize,
      bool _reduceFragmentationInAllocationClass);

  // Set the access config for cachelib's access container. Refer to our
  // user guide for how to tune access container (configure hashtable).
  CacheAllocatorConfig& setAccessConfig(AccessConfig config);

  // Set the access config for cachelib's access container based on the
  // number of estimated cache entries.
  CacheAllocatorConfig& setAccessConfig(size_t numEntries);

  // RemoveCallback is invoked for each item that is evicted or removed
  // explicitly from RAM
  CacheAllocatorConfig& setRemoveCallback(RemoveCb cb);

  // ItemDestructor is invoked for each item that is evicted or removed
  // explicitly from cache (both RAM and NVM)
  CacheAllocatorConfig& setItemDestructor(ItemDestructor destructor);

  // Config for NvmCache. If enabled, cachelib will also make use of flash.
  CacheAllocatorConfig& enableNvmCache(NvmCacheConfig config);

  // enable the reject first admission policy through its parameters
  // @param numEntries          the number of entries to track across all splits
  // @param numSplits           the number of splits. we drop a whole split by
  //                            FIFO
  // @param suffixIgnoreLength  the suffix of the key to ignore for tracking.
  //                            disabled when set to 0
  //
  // @param useDramHitSignal    use hits in DRAM as signal for admission
  CacheAllocatorConfig& enableRejectFirstAPForNvm(uint64_t numEntries,
                                                  uint32_t numSplits,
                                                  size_t suffixIgnoreLength,
                                                  bool useDramHitSignal);

  // enable an admission policy for NvmCache. If this is set, other supported
  // options like enableRejectFirstAP etc are overlooked.
  //
  // @throw std::invalid_argument if nullptr is passed.
  CacheAllocatorConfig& setNvmCacheAdmissionPolicy(
      std::shared_ptr<NvmAdmissionPolicy<CacheT>> policy);

  // enables encoding items before they go into nvmcache
  CacheAllocatorConfig& setNvmCacheEncodeCallback(NvmCacheEncodeCb cb);

  // enables decoding items before they get back into ram cache
  CacheAllocatorConfig& setNvmCacheDecodeCallback(NvmCacheDecodeCb cb);

  // enable encryption support for NvmCache. This will encrypt every byte
  // written to the device.
  CacheAllocatorConfig& enableNvmCacheEncryption(
      std::shared_ptr<NvmCacheDeviceEncryptor> encryptor);

  // return if NvmCache encryption is enabled
  bool isNvmCacheEncryptionEnabled() const;

  // If enabled, it means we'll only store user-requested size into NvmCache
  // instead of the full usable size returned by CacheAllocator::getUsableSize()
  CacheAllocatorConfig& enableNvmCacheTruncateAllocSize();

  // return if truncate-alloc-size is enabled. If true, it means we'll only
  // store user-requested size into NvmCache instead of the full usable size
  // returned by CacheAllocator::getUsableSize()
  bool isNvmCacheTruncateAllocSizeEnabled() const;

  // Enable compact cache support. Refer to our user guide for how ccache works.
  CacheAllocatorConfig& enableCompactCache();

  // Configure chained items. Refer to our user guide for how chained items
  // work.
  //
  // @param config  Config for chained item's access container, it's similar to
  //                the main access container but only used for chained items
  // @param lockPower  this controls the number of locks (2^lockPower) for
  //                   synchronizing operations on chained items.
  CacheAllocatorConfig& configureChainedItems(AccessConfig config = {},
                                              uint32_t lockPower = 10);

  // Configure chained items. Refer to our user guide for how chained items
  // work. This function calculates the optimal bucketsPower and locksPower for
  // users based on estimated chained items number.
  //
  // @param numEntries  number of estimated chained items
  // @param lockPower  this controls the number of locks (2^lockPower) for
  //                   synchronizing operations on chained items.
  CacheAllocatorConfig& configureChainedItems(size_t numEntries,
                                              uint32_t lockPower = 10);

  // enable tracking tail hits
  CacheAllocatorConfig& enableTailHitsTracking();

  // Turn on full core dump which includes all the cache memory.
  // This is not recommended for production as it can significantly slow down
  // the coredumping process.
  CacheAllocatorConfig& setFullCoredump(bool enable);

  // Sets the configuration that controls whether nvmcache is recovered if
  // possible when dram cache is not recovered. By default nvmcache is
  // recovered even when dram cache is not recovered.
  CacheAllocatorConfig& setDropNvmCacheOnShmNew(bool enable);

  // Turn off fast shutdown mode, which interrupts any releaseSlab operations
  // in progress, so that workers in the process of releaseSlab may be
  // completed sooner for shutdown to take place fast.
  CacheAllocatorConfig& disableFastShutdownMode();

  // when disabling full core dump, turning this option on will enable
  // cachelib to track recently accessed items and keep them in the partial
  // core dump. See CacheAllocator::madviseRecentItems()
  CacheAllocatorConfig& setTrackRecentItemsForDump(bool enable);

  // Page in all the cache memory asynchronously when cache starts up.
  // This helps ensure the system actually has enough memory to page in all of
  // the cache. We'll fail early if there isn't enough memory.
  //
  // If memory monitor is enabled, this is not usually needed.
  CacheAllocatorConfig& setMemoryLocking(bool enable);

  // This allows cache to be persisted across restarts. One example use case is
  // to preserve the cache when releasing a new version of your service. Refer
  // to our user guide for how to set up cache persistence.
  CacheAllocatorConfig& enableCachePersistence(std::string directory,
                                               void* baseAddr = nullptr);

  // uses posix shm segments instead of the default sys-v shm segments.
  // @throw std::invalid_argument if called without enabling
  // cachePersistence()
  CacheAllocatorConfig& usePosixForShm();

  // Configures cache memory tiers. Each tier represents a cache region inside
  // byte-addressable memory such as DRAM, Pmem, CXLmem.
  // Accepts vector of MemoryTierCacheConfig. Each vector element describes
  // configuration for a single memory cache tier. Tier sizes are specified as
  // ratios, the number of parts of total cache size each tier would occupy.
  CacheAllocatorConfig& configureMemoryTiers(const MemoryTierConfigs& configs);

  // Return reference to MemoryTierCacheConfigs.
  const MemoryTierConfigs& getMemoryTierConfigs() const noexcept;

  // This turns on a background worker that periodically scans through the
  // access container and look for expired items and remove them.
  CacheAllocatorConfig& enableItemReaperInBackground(
      std::chrono::milliseconds interval, util::Throttler::Config config = {});

  // When using free memory monitoring mode, CacheAllocator shrinks the cache
  // size when the system is under memory pressure. Cache will grow back when
  // the memory pressure goes down.
  //
  // When using resident memory monitoring mode, typically when the
  // application runs inside containers, the lowerLimit and upperLimit are the
  // opposite to that of the free memory monitoring mode.
  // "upperLimit" refers to the upper bound of application memory.
  // Cache size will actually decrease once an application reaches past it
  // and grows when drops below "lowerLimit".
  //
  // @param interval  waits for an interval between each run
  // @param config    memory monitoring config
  // @param RebalanceStrategy  an optional strategy to customize where the slab
  //                           to give up when shrinking cache
  CacheAllocatorConfig& enableMemoryMonitor(
      std::chrono::milliseconds interval,
      MemoryMonitor::Config config,
      std::shared_ptr<RebalanceStrategy> = {});

  // Enable pool rebalancing. This allows each pool to internally rebalance
  // slab memory distributed across different allocation classes. For example,
  // if the 64 bytes allocation classes are receiving for allocation requests,
  // eventually CacheAllocator will move more memory to it from other allocation
  // classes. The rebalancing is triggered every specified interval and
  // optionally on allocation failures. For more details, see our user guide.
  CacheAllocatorConfig& enablePoolRebalancing(
      std::shared_ptr<RebalanceStrategy> defaultRebalanceStrategy,
      std::chrono::milliseconds interval,
      bool disableForcedWakeup = false);

  // This lets you change pool size during runtime, and the pool resizer
  // will slowly adjust each pool's memory size to the newly configured sizes.
  CacheAllocatorConfig& enablePoolResizing(
      std::shared_ptr<RebalanceStrategy> resizeStrategy,
      std::chrono::milliseconds interval,
      uint32_t slabsToReleasePerIteration);

  // Enable pool size optimizer, which automatically adjust pool sizes as
  // traffic changes or new memory added.
  // For now we support different intervals between regular pools and compact
  // caches.
  CacheAllocatorConfig& enablePoolOptimizer(
      std::shared_ptr<PoolOptimizeStrategy> optimizeStrategy,
      std::chrono::seconds regularInterval,
      std::chrono::seconds ccacheInterval,
      uint32_t ccacheStepSizePercent);

  // This enables an optimization for Pool rebalancing and resizing.
  // The rough idea is to ensure only the least useful items are evicted when
  // we move slab memory around. Come talk to Cache Library team if you think
  // this can help your service.
  CacheAllocatorConfig& enableMovingOnSlabRelease(
      MoveCb cb,
      ChainedItemMovingSync sync = {},
      uint32_t movingAttemptsLimit = 10);

  // Specify a threshold for detecting slab release stuck
  CacheAllocatorConfig& setSlabReleaseStuckThreashold(
      std::chrono::milliseconds threshold);

  // This customizes how many items we try to evict before giving up.s
  // We may fail to evict if someone else (another thread) is using an item.
  // Setting this to a high limit leads to a higher chance of successful
  // evictions but it can lead to higher allocation latency as well.
  // Unless you're very familiar with caching, come talk to Cache Library team
  // before you start customizing this option.
  CacheAllocatorConfig& setEvictionSearchLimit(uint32_t limit);

  // Specify a threshold for per-item outstanding references, beyond which,
  // shared_ptr will be allocated instead of handles to support having  more
  // outstanding iobuf
  // The default behavior is always using handles
  CacheAllocatorConfig& setRefcountThresholdForConvertingToIOBuf(uint32_t);

  // This throttles various internal operations in cachelib. It may help
  // improve latency in allocation and find paths. Come talk to Cache
  // Library team if you find yourself customizing this.
  CacheAllocatorConfig& setThrottlerConfig(util::Throttler::Config config);

  // Passes in a callback to initialize an event tracker when the allocator
  // starts
  CacheAllocatorConfig& setEventTracker(EventTrackerSharedPtr&&);

  // Set the minimum TTL for an item to be admitted into NVM cache.
  // If nvmAdmissionMinTTL is set to be positive, any item with configured TTL
  // smaller than this will always be rejected by NvmAdmissionPolicy.
  CacheAllocatorConfig& setNvmAdmissionMinTTL(uint64_t ttl);

  // Skip promote children items in chained when parent fail to promote
  CacheAllocatorConfig& setSkipPromoteChildrenWhenParentFailed();

  // We will delay worker start until user explicitly calls
  // CacheAllocator::startCacheWorkers()
  CacheAllocatorConfig& setDelayCacheWorkersStart();

  // skip promote children items in chained when parent fail to promote
  bool isSkipPromoteChildrenWhenParentFailed() const noexcept {
    return skipPromoteChildrenWhenParentFailed;
  }

  // @return whether compact cache is enabled
  bool isCompactCacheEnabled() const noexcept { return enableZeroedSlabAllocs; }

  // @return whether pool resizing is enabled
  bool poolResizingEnabled() const noexcept {
    return poolResizeInterval.count() > 0 && poolResizeSlabsPerIter > 0;
  }

  // @return whether pool rebalancing is enabled
  bool poolRebalancingEnabled() const noexcept {
    return poolRebalanceInterval.count() > 0 &&
           defaultPoolRebalanceStrategy != nullptr;
  }

  // @return whether pool optimizing is enabled
  bool poolOptimizerEnabled() const noexcept {
    return (regularPoolOptimizeInterval.count() > 0 ||
            compactCacheOptimizeInterval.count() > 0) &&
           poolOptimizeStrategy != nullptr;
  }

  // @return whether memory monitor is enabled
  bool memMonitoringEnabled() const noexcept {
    return memMonitorConfig.mode != MemoryMonitor::Disabled &&
           memMonitorInterval.count() > 0;
  }

  // @return whether reaper is enabled
  bool itemsReaperEnabled() const noexcept {
    return reaperInterval.count() > 0;
  }

  const std::string& getCacheDir() const noexcept { return cacheDir; }

  const std::string& getCacheName() const noexcept { return cacheName; }

  size_t getCacheSize() const noexcept { return size; }

  bool isUsingPosixShm() const noexcept { return usePosixShm; }

  // validate the config, and return itself if valid
  const CacheAllocatorConfig& validate() const;

  // check whether the RebalanceStrategy can be used with this config
  bool validateStrategy(
      const std::shared_ptr<RebalanceStrategy>& strategy) const;

  // check whether the PoolOptimizeStrategy can be used with this config
  bool validateStrategy(
      const std::shared_ptr<PoolOptimizeStrategy>& strategy) const;

  // check that memory tier ratios are set properly
  const CacheAllocatorConfig& validateMemoryTiers() const;

  // @return a map representation of the configs
  std::map<std::string, std::string> serialize() const;

  // The max number of memory cache tiers
  // TODO: increase this number when multi-tier configs are enabled
  inline static const size_t kMaxCacheMemoryTiers = 1;

  // Cache name for users to indentify their own cache.
  std::string cacheName{""};

  // Amount of memory for this cache instance (sum of all memory tiers' sizes)
  size_t size = 1 * 1024 * 1024 * 1024;

  // Directory for shared memory related metadata
  std::string cacheDir;

  // if true, uses posix shm; if not, uses sys-v (default)
  bool usePosixShm{false};

  // Attach shared memory to a fixed base address
  void* slabMemoryBaseAddr = nullptr;

  // User defined default alloc sizes. If empty, we'll generate a default one.
  // This set of alloc sizes will be used for pools that user do not supply
  // a custom set of alloc sizes.
  std::set<uint32_t> defaultAllocSizes;

  // whether to detach allocator memory upon a core dump
  bool disableFullCoredump{true};

  // whether to enable fast shutdown, that would interrupt on-going slab
  // release process, or not.
  bool enableFastShutdown{true};

  // if we want to track recent items for dumping them in core when we disable
  // full core dump.
  bool trackRecentItemsForDump{false};

  // if enabled ensures that nvmcache is not persisted when dram cache is not
  // presisted.
  bool dropNvmCacheOnShmNew{false};

  // TODO:
  // BELOW are the config for various cache workers
  // Today, they're set before CacheAllocator is created and stay
  // fixed for the lifetime of CacheAllocator. Going foward, this
  // will be allowed to be changed dynamically during runtime and
  // trigger updates to the cache workers
  // time interval to sleep between iterations of resizing the pools.
  std::chrono::milliseconds poolResizeInterval{std::chrono::seconds(0)};

  // number of slabs to be released per pool that is over the limit in each
  // iteration.
  unsigned int poolResizeSlabsPerIter{5};

  // the rebalance strategy for the pool resizing if enabled.
  std::shared_ptr<RebalanceStrategy> poolResizeStrategy;

  // the strategy to be used when advising memory to pick a donor
  std::shared_ptr<RebalanceStrategy> poolAdviseStrategy;

  // time interval to sleep between iterators of rebalancing the pools.
  std::chrono::milliseconds poolRebalanceInterval{std::chrono::seconds{1}};

  // disable waking up the PoolRebalancer on alloc failures
  bool poolRebalancerDisableForcedWakeUp{false};

  // Free slabs pro-actively if the ratio of number of freeallocs to
  // the number of allocs per slab in a slab class is above this
  // threshold
  // A value of 0 means, this feature is disabled.
  unsigned int poolRebalancerFreeAllocThreshold{0};

  // rebalancing strategy for all pools. By default the strategy will
  // rebalance to avoid alloc fialures.
  std::shared_ptr<RebalanceStrategy> defaultPoolRebalanceStrategy{
      new RebalanceStrategy{}};

  // The slab release process is considered as being stuck if it does not
  // make any progress for the below threshold
  std::chrono::milliseconds slabReleaseStuckThreshold{std::chrono::seconds(60)};

  // time interval to sleep between iterations of pool size optimization,
  // for regular pools and compact caches
  std::chrono::seconds regularPoolOptimizeInterval{0};
  std::chrono::seconds compactCacheOptimizeInterval{0};

  // step size for compact cache size optimization: how many percents of the
  // victim to move
  unsigned int ccacheOptimizeStepSizePercent{1};

  // optimization strategy
  std::shared_ptr<PoolOptimizeStrategy> poolOptimizeStrategy{nullptr};

  // Callback for initializing the eventTracker on CacheAllocator construction.
  EventTrackerSharedPtr eventTracker{nullptr};

  // whether to allow tracking tail hits in MM2Q
  bool trackTailHits{false};

  // Memory monitoring config
  MemoryMonitor::Config memMonitorConfig;

  // time interval to sleep between iterations of monitoring memory.
  // Set to 0 to disable memory monitoring
  std::chrono::milliseconds memMonitorInterval{0};

  // throttler config of items reaper for iteration
  util::Throttler::Config reaperConfig{};

  // time to sleep between each reaping period.
  std::chrono::milliseconds reaperInterval{5000};

  // interval during which we adjust dynamically the refresh ratio.
  std::chrono::milliseconds mmReconfigureInterval{0};

  //
  // TODO:
  // ABOVE are the config for various cache workers
  //

  // the number of tries to search for an item to evict
  // 0 means it's infinite
  unsigned int evictionSearchTries{50};

  // If refcount is larger than this threshold, we will use shared_ptr
  // for handles in IOBuf chains.
  unsigned int thresholdForConvertingToIOBuf{
      std::numeric_limits<unsigned int>::max()};

  // number of attempts to move an item before giving up and try to
  // evict the item
  unsigned int movingTries{10};

  // Config that specifes how throttler will behave
  // How much time it will sleep and how long an interval between each sleep
  util::Throttler::Config throttleConfig{};

  // Access config for chained items, Only used if chained items are enabled
  AccessConfig chainedItemAccessConfig{};

  // User level synchronization function object. This will be held while
  // executing the moveCb. This is only needed when using and moving is
  // enabled for chained items.
  ChainedItemMovingSync movingSync{};

  // determines how many locks we have for synchronizing chained items
  // add/pop and between moving during slab rebalancing
  uint32_t chainedItemsLockPower{10};

  // Configuration for the main access container which manages the lookup
  // for all normal items
  AccessConfig accessConfig{};

  // user defined callback invoked when an item is being evicted or freed from
  // RAM
  RemoveCb removeCb{};

  // user defined item destructor invoked when an item is being
  // evicted or freed from cache (both RAM and NVM)
  ItemDestructor itemDestructor{};

  // user defined call back to move the item. This is executed while holding
  // the user provided movingSync. For items without chained allocations,
  // there is no specific need for explicit movingSync and user can skip
  // providing a movingSync and do explicit synchronization just in the
  // moveCb if needed to protect the data being moved with concurrent
  // readers.
  MoveCb moveCb{};

  // custom user provided admission policy
  std::shared_ptr<NvmAdmissionPolicy<CacheT>> nvmCacheAP{nullptr};

  // Config for nvmcache type
  folly::Optional<NvmCacheConfig> nvmConfig;

  // configuration for reject first admission policy to nvmcache. 0 indicates
  // a disabled policy.
  uint64_t rejectFirstAPNumEntries{0};
  uint32_t rejectFirstAPNumSplits{20};

  // if non zero specifies the suffix of the key to be ignored when tracking.
  // this enables tracking group of keys that have common prefix.
  size_t rejectFirstSuffixIgnoreLength{0};

  // if enabled, uses the fact that item got a hit in DRAM as a signal to
  // admit
  bool rejectFirstUseDramHitSignal{true};

  // Must enable this in order to call `allocateZeroedSlab`.
  // Otherwise, it will throw.
  // This is required for compact cache
  bool enableZeroedSlabAllocs = false;

  // Asynchronously page in the cache memory before they are accessed by the
  // application. This can ensure all of cache memory is accounted for in the
  // RSS even if application does not access all of it, avoiding any
  // surprises (i.e. OOM).
  //
  // This can only be turned on the first time we're creating the cache.
  // This option has no effect when attaching to existing cache.
  bool lockMemory{false};

  // These configs configure how MemoryAllocator will be generating
  // allocation class sizes for each pool by default
  double allocationClassSizeFactor{1.25};
  uint32_t maxAllocationClassSize{Slab::kSize};
  uint32_t minAllocationClassSize{72};
  bool reduceFragmentationInAllocationClass{false};

  // The minimum TTL an item need to have in order to be admitted into NVM
  // cache.
  uint64_t nvmAdmissionMinTTL{0};

  // Skip promote children items in chained when parent fail to promote
  bool skipPromoteChildrenWhenParentFailed{false};

  // If true, we will delay worker start until user explicitly calls
  // CacheAllocator::startCacheWorkers()
  bool delayCacheWorkersStart{false};

  friend CacheT;

 private:
  void mergeWithPrefix(
      std::map<std::string, std::string>& configMap,
      const std::map<std::string, std::string>& configMapToMerge,
      const std::string& prefix) const;
  std::string stringifyAddr(const void* addr) const;
  std::string stringifyRebalanceStrategy(
      const std::shared_ptr<RebalanceStrategy>& strategy) const;

  // Configuration for memory tiers.
  MemoryTierConfigs memoryTierConfigs{
      {MemoryTierCacheConfig::fromShm().setRatio(1)}};
};

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setCacheName(
    const std::string& _cacheName) {
  cacheName = _cacheName;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setCacheSize(size_t _size) {
  size = _size;
  constexpr size_t maxCacheSizeWithCoredump = 64'424'509'440; // 60GB
  if (size <= maxCacheSizeWithCoredump) {
    return setFullCoredump(true);
  }
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setDefaultAllocSizes(
    std::set<uint32_t> allocSizes) {
  defaultAllocSizes = allocSizes;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setDefaultAllocSizes(
    double _allocationClassSizeFactor,
    uint32_t _maxAllocationClassSize,
    uint32_t _minAllocationClassSize,
    bool _reduceFragmentationInAllocationClass) {
  allocationClassSizeFactor = _allocationClassSizeFactor;
  maxAllocationClassSize = _maxAllocationClassSize;
  minAllocationClassSize = _minAllocationClassSize;
  reduceFragmentationInAllocationClass = _reduceFragmentationInAllocationClass;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setAccessConfig(
    AccessConfig config) {
  accessConfig = std::move(config);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setAccessConfig(
    size_t numEntries) {
  AccessConfig config{};
  config.sizeBucketsPowerAndLocksPower(numEntries);
  accessConfig = std::move(config);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setRemoveCallback(
    RemoveCb cb) {
  removeCb = std::move(cb);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setItemDestructor(
    ItemDestructor destructor) {
  itemDestructor = std::move(destructor);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableRejectFirstAPForNvm(
    uint64_t numEntries,
    uint32_t numSplits,
    size_t suffixIgnoreLength,
    bool useDramHitSignal) {
  if (numEntries == 0) {
    throw std::invalid_argument(
        "Enalbing reject first AP needs non zero numEntries");
  }
  rejectFirstAPNumEntries = numEntries;
  rejectFirstAPNumSplits = numSplits;
  rejectFirstSuffixIgnoreLength = suffixIgnoreLength;
  rejectFirstUseDramHitSignal = useDramHitSignal;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableNvmCache(
    NvmCacheConfig config) {
  nvmConfig.assign(config);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setNvmCacheAdmissionPolicy(
    std::shared_ptr<NvmAdmissionPolicy<T>> policy) {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmCache admission policy callback can not be set unless nvmcache is "
        "used");
  }

  if (!policy) {
    throw std::invalid_argument("Setting a null admission policy");
  }

  nvmCacheAP = std::move(policy);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setNvmCacheEncodeCallback(
    NvmCacheEncodeCb cb) {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmCache filter callback can not be set unless nvmcache is used");
  }
  nvmConfig->encodeCb = std::move(cb);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setNvmCacheDecodeCallback(
    NvmCacheDecodeCb cb) {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmCache filter callback can not be set unless nvmcache is used");
  }
  nvmConfig->decodeCb = std::move(cb);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableNvmCacheEncryption(
    std::shared_ptr<NvmCacheDeviceEncryptor> encryptor) {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmCache encrytion/decrytion callbacks can not be set unless nvmcache "
        "is used");
  }
  if (!encryptor) {
    throw std::invalid_argument("Set a nullptr encryptor is NOT allowed");
  }
  nvmConfig->deviceEncryptor = std::move(encryptor);
  return *this;
}

template <typename T>
bool CacheAllocatorConfig<T>::isNvmCacheEncryptionEnabled() const {
  return nvmConfig && nvmConfig->deviceEncryptor;
}

template <typename T>
CacheAllocatorConfig<T>&
CacheAllocatorConfig<T>::enableNvmCacheTruncateAllocSize() {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmCache mode can not be adjusted unless nvmcache is used");
  }
  nvmConfig->truncateItemToOriginalAllocSizeInNvm = true;
  return *this;
}

template <typename T>
bool CacheAllocatorConfig<T>::isNvmCacheTruncateAllocSizeEnabled() const {
  return nvmConfig && nvmConfig->truncateItemToOriginalAllocSizeInNvm;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableCompactCache() {
  enableZeroedSlabAllocs = true;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::configureChainedItems(
    AccessConfig config, uint32_t lockPower) {
  chainedItemAccessConfig = config;
  chainedItemsLockPower = lockPower;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::configureChainedItems(
    size_t numEntries, uint32_t lockPower) {
  AccessConfig config{};
  config.sizeBucketsPowerAndLocksPower(numEntries);
  chainedItemAccessConfig = std::move(config);
  chainedItemsLockPower = lockPower;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableTailHitsTracking() {
  trackTailHits = true;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setFullCoredump(bool enable) {
  disableFullCoredump = !enable;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setDropNvmCacheOnShmNew(
    bool enable) {
  dropNvmCacheOnShmNew = enable;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::disableFastShutdownMode() {
  enableFastShutdown = false;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setTrackRecentItemsForDump(
    bool enable) {
  trackRecentItemsForDump = enable;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setMemoryLocking(
    bool enable) {
  lockMemory = enable;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableCachePersistence(
    std::string cacheDirectory, void* baseAddr) {
  cacheDir = cacheDirectory;
  slabMemoryBaseAddr = baseAddr;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::usePosixForShm() {
  if (cacheDir.empty()) {
    throw std::invalid_argument(
        "Posix shm can be set only when cache persistence is enabled");
  }
  usePosixShm = true;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableItemReaperInBackground(
    std::chrono::milliseconds interval, util::Throttler::Config config) {
  reaperInterval = interval;
  reaperConfig = config;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::configureMemoryTiers(
    const MemoryTierConfigs& config) {
  if (config.size() > kMaxCacheMemoryTiers) {
    throw std::invalid_argument(folly::sformat(
        "Too many memory tiers. The number of supported tiers is {}.",
        kMaxCacheMemoryTiers));
  }
  if (!config.size()) {
    throw std::invalid_argument(
        "There must be at least one memory tier config.");
  }
  memoryTierConfigs = config;
  return *this;
}

template <typename T>
const typename CacheAllocatorConfig<T>::MemoryTierConfigs&
CacheAllocatorConfig<T>::getMemoryTierConfigs() const noexcept {
  return memoryTierConfigs;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableMemoryMonitor(
    std::chrono::milliseconds interval,
    MemoryMonitor::Config config,
    std::shared_ptr<RebalanceStrategy> adviseStrategy) {
  memMonitorInterval = interval;
  memMonitorConfig = std::move(config);
  poolAdviseStrategy = adviseStrategy;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enablePoolOptimizer(
    std::shared_ptr<PoolOptimizeStrategy> strategy,
    std::chrono::seconds regularPoolInterval,
    std::chrono::seconds compactCacheInterval,
    uint32_t ccacheStepSizePercent) {
  if (validateStrategy(strategy)) {
    regularPoolOptimizeInterval = regularPoolInterval;
    compactCacheOptimizeInterval = compactCacheInterval;
    poolOptimizeStrategy = strategy;
    ccacheOptimizeStepSizePercent = ccacheStepSizePercent;
  } else {
    throw std::invalid_argument(
        "Invalid pool optimize strategy for the cache allocator.");
  }
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enablePoolRebalancing(
    std::shared_ptr<RebalanceStrategy> defaultRebalanceStrategy,
    std::chrono::milliseconds interval,
    bool disableForcedWakeup) {
  if (validateStrategy(defaultRebalanceStrategy)) {
    defaultPoolRebalanceStrategy = defaultRebalanceStrategy;
    poolRebalanceInterval = interval;
    poolRebalancerDisableForcedWakeUp = disableForcedWakeup;
  } else {
    throw std::invalid_argument(
        "Invalid rebalance strategy for the cache allocator.");
  }
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enablePoolResizing(
    std::shared_ptr<RebalanceStrategy> resizeStrategy,
    std::chrono::milliseconds interval,
    uint32_t slabsToReleasePerIteration) {
  if (validateStrategy(resizeStrategy)) {
    poolResizeStrategy = resizeStrategy;
    poolResizeInterval = interval;
    poolResizeSlabsPerIter = slabsToReleasePerIteration;
  } else {
    throw std::invalid_argument(
        "Invalid pool resizing strategy for the cache allocator.");
  }
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableMovingOnSlabRelease(
    MoveCb cb, ChainedItemMovingSync sync, uint32_t movingAttemptsLimit) {
  moveCb = cb;
  movingSync = sync;
  movingTries = movingAttemptsLimit;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setSlabReleaseStuckThreashold(
    std::chrono::milliseconds threshold) {
  slabReleaseStuckThreshold = threshold;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setEvictionSearchLimit(
    uint32_t limit) {
  evictionSearchTries = limit;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>&
CacheAllocatorConfig<T>::setRefcountThresholdForConvertingToIOBuf(
    uint32_t threshold) {
  thresholdForConvertingToIOBuf = threshold;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setThrottlerConfig(
    util::Throttler::Config config) {
  throttleConfig = config;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setEventTracker(
    EventTrackerSharedPtr&& otherEventTracker) {
  eventTracker = std::move(otherEventTracker);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setNvmAdmissionMinTTL(
    uint64_t ttl) {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmAdmissionMinTTL can not be set unless nvmcache is used");
  }

  nvmAdmissionMinTTL = ttl;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>&
CacheAllocatorConfig<T>::setSkipPromoteChildrenWhenParentFailed() {
  skipPromoteChildrenWhenParentFailed = true;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setDelayCacheWorkersStart() {
  delayCacheWorkersStart = true;
  return *this;
}

template <typename T>
const CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::validate() const {
  // we can track tail hits only if MMType is MM2Q
  if (trackTailHits && T::MMType::kId != MM2Q::kId) {
    throw std::invalid_argument(
        "Tail hits tracking cannot be enabled on MMTypes except MM2Q.");
  }

  size_t maxCacheSize = CompressedPtr::getMaxAddressableSize();
  // Configured cache size should not exceed the maximal addressable space for
  // cache.
  if (size > maxCacheSize) {
    throw std::invalid_argument(folly::sformat(
        "config cache size: {}  exceeds max addressable space for cache: {}",
        size,
        maxCacheSize));
  }

  // we don't allow user to enable both RemoveCB and ItemDestructor
  if (removeCb && itemDestructor) {
    throw std::invalid_argument(
        "It's not allowed to enable both RemoveCB and ItemDestructor.");
  }

  return validateMemoryTiers();
}

template <typename T>
bool CacheAllocatorConfig<T>::validateStrategy(
    const std::shared_ptr<RebalanceStrategy>& strategy) const {
  if (!strategy) {
    return true;
  }

  auto type = strategy->getType();
  return type != RebalanceStrategy::NumTypes &&
         (type != RebalanceStrategy::MarginalHits || trackTailHits);
}

template <typename T>
bool CacheAllocatorConfig<T>::validateStrategy(
    const std::shared_ptr<PoolOptimizeStrategy>& strategy) const {
  if (!strategy) {
    return true;
  }

  auto type = strategy->getType();
  return type != PoolOptimizeStrategy::NumTypes &&
         (type != PoolOptimizeStrategy::MarginalHits || trackTailHits);
}

template <typename T>
const CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::validateMemoryTiers()
    const {
  size_t parts = 0;
  for (const auto& tierConfig : memoryTierConfigs) {
    if (!tierConfig.getRatio()) {
      throw std::invalid_argument("Tier ratio must be an integer number >=1.");
    }
    parts += tierConfig.getRatio();
  }

  if (parts > size) {
    throw std::invalid_argument(
        "Sum of tier ratios must be less than total cache size.");
  }
  return *this;
}

template <typename T>
std::map<std::string, std::string> CacheAllocatorConfig<T>::serialize() const {
  std::map<std::string, std::string> configMap;

  configMap["size"] = std::to_string(size);
  configMap["cacheDir"] = cacheDir;
  configMap["posixShm"] = usePosixShm ? "set" : "empty";

  configMap["defaultAllocSizes"] = "";
  // Stringify std::set
  for (auto& elem : defaultAllocSizes) {
    if (configMap["defaultAllocSizes"] != "") {
      configMap["defaultAllocSizes"] += ", ";
    }
    configMap["defaultAllocSizes"] += std::to_string(elem);
  }
  configMap["disableFullCoredump"] = std::to_string(disableFullCoredump);
  configMap["dropNvmCacheOnShmNew"] = std::to_string(dropNvmCacheOnShmNew);
  configMap["trackRecentItemsForDump"] =
      std::to_string(trackRecentItemsForDump);
  configMap["poolResizeInterval"] = util::toString(poolResizeInterval);
  configMap["poolResizeSlabsPerIter"] = std::to_string(poolResizeSlabsPerIter);

  configMap["poolRebalanceInterval"] = util::toString(poolRebalanceInterval);
  configMap["slabReleaseStuckThreshold"] =
      util::toString(slabReleaseStuckThreshold);
  configMap["trackTailHits"] = std::to_string(trackTailHits);
  // Stringify enum
  switch (memMonitorConfig.mode) {
  case MemoryMonitor::FreeMemory:
    configMap["memMonitorMode"] = "Free Memory";
    break;
  case MemoryMonitor::ResidentMemory:
    configMap["memMonitorMode"] = "Resident Memory";
    break;
  case MemoryMonitor::Disabled:
    configMap["memMonitorMode"] = "Disabled";
    break;
  default:
    configMap["memMonitorMode"] = "Unknown";
  }
  configMap["memMonitorInterval"] = util::toString(memMonitorInterval);
  configMap["memAdvisePercentPerIter"] =
      std::to_string(memMonitorConfig.maxAdvisePercentPerIter);
  configMap["memReclaimPercentPerIter"] =
      std::to_string(memMonitorConfig.maxReclaimPercentPerIter);
  configMap["memMaxAdvisePercent"] =
      std::to_string(memMonitorConfig.maxAdvisePercent);
  configMap["memLowerLimitGB"] = std::to_string(memMonitorConfig.lowerLimitGB);
  configMap["memUpperLimitGB"] = std::to_string(memMonitorConfig.upperLimitGB);
  configMap["reclaimRateLimitWindowSecs"] =
      std::to_string(memMonitorConfig.reclaimRateLimitWindowSecs.count());
  configMap["reaperInterval"] = util::toString(reaperInterval);
  configMap["mmReconfigureInterval"] = util::toString(mmReconfigureInterval);
  configMap["evictionSearchTries"] = std::to_string(evictionSearchTries);
  configMap["thresholdForConvertingToIOBuf"] =
      std::to_string(thresholdForConvertingToIOBuf);
  configMap["movingTries"] = std::to_string(movingTries);
  configMap["chainedItemsLockPower"] = std::to_string(chainedItemsLockPower);
  configMap["removeCb"] = removeCb ? "set" : "empty";
  configMap["nvmAP"] = nvmCacheAP ? "custom" : "empty";
  configMap["nvmAPRejectFirst"] = rejectFirstAPNumEntries ? "set" : "empty";
  configMap["moveCb"] = moveCb ? "set" : "empty";
  configMap["enableZeroedSlabAllocs"] = std::to_string(enableZeroedSlabAllocs);
  configMap["lockMemory"] = std::to_string(lockMemory);
  configMap["allocationClassSizeFactor"] =
      std::to_string(allocationClassSizeFactor);
  configMap["maxAllocationClassSize"] = std::to_string(maxAllocationClassSize);
  configMap["minAllocationClassSize"] = std::to_string(minAllocationClassSize);
  configMap["reduceFragmentationInAllocationClass"] =
      std::to_string(reduceFragmentationInAllocationClass);
  configMap["slabMemoryBaseAddr"] = stringifyAddr(slabMemoryBaseAddr);
  configMap["poolResizeStrategy"] =
      stringifyRebalanceStrategy(poolResizeStrategy);
  configMap["poolAdviseStrategy"] =
      stringifyRebalanceStrategy(poolAdviseStrategy);
  configMap["defaultPoolRebalanceStrategy"] =
      stringifyRebalanceStrategy(defaultPoolRebalanceStrategy);
  configMap["eventTracker"] = eventTracker ? "set" : "empty";
  configMap["nvmAdmissionMinTTL"] = std::to_string(nvmAdmissionMinTTL);
  configMap["delayCacheWorkersStart"] =
      delayCacheWorkersStart ? "true" : "false";
  mergeWithPrefix(configMap, throttleConfig.serialize(), "throttleConfig");
  mergeWithPrefix(configMap,
                  chainedItemAccessConfig.serialize(),
                  "chainedItemAccessConfig");
  mergeWithPrefix(configMap, accessConfig.serialize(), "accessConfig");
  mergeWithPrefix(configMap, reaperConfig.serialize(), "reaperConfig");
  if (nvmConfig)
    mergeWithPrefix(configMap, nvmConfig->serialize(), "nvmConfig");

  return configMap;
}

template <typename T>
void CacheAllocatorConfig<T>::mergeWithPrefix(
    std::map<std::string, std::string>& configMap,
    const std::map<std::string, std::string>& configMapToMerge,
    const std::string& prefix) const {
  for (auto const& kv : configMapToMerge) {
    configMap[prefix + "::" + kv.first] = kv.second;
  }
}

template <typename T>
std::string CacheAllocatorConfig<T>::stringifyAddr(const void* addr) const {
  if (addr == nullptr)
    return "";
  const std::string HEX = "0123456789abcdef";
  uintptr_t num = (uintptr_t)slabMemoryBaseAddr;
  std::string res = "";
  while (num) {
    res = HEX[(num & 0xf)] + res;
    num >>= 4;
  }
  return res;
}

template <typename T>
std::string CacheAllocatorConfig<T>::stringifyRebalanceStrategy(
    const std::shared_ptr<RebalanceStrategy>& strategy) const {
  if (!strategy)
    return "empty";
  switch (strategy->getType()) {
  case RebalanceStrategy::PickNothingOrTest:
    return "PickNothingOrTest";
  case RebalanceStrategy::Random:
    return "Random";
  case RebalanceStrategy::MarginalHits:
    return "MarginalHits";
  case RebalanceStrategy::FreeMem:
    return "FreeMem";
  case RebalanceStrategy::HitsPerSlab:
    return "HitsPerSlab";
  case RebalanceStrategy::LruTailAge:
    return "LruTailAge";
  case RebalanceStrategy::PoolResize:
    return "PoolResize";
  case RebalanceStrategy::StressRebalance:
    return "StressRebalance";
  default:
    return "undefined";
  }
}
} // namespace cachelib
} // namespace facebook
