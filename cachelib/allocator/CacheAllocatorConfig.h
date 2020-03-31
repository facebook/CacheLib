#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>

#include <folly/Optional.h>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/EventInterface.h"
#include "cachelib/allocator/MM2Q.h"
#include "cachelib/allocator/MemoryMonitor.h"
#include "cachelib/allocator/NvmAdmissionPolicy.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/Util.h"
#include "cachelib/common/Throttler.h"

namespace facebook {
namespace cachelib {

template <typename CacheT>
class CacheAllocatorConfig {
 public:
  using AccessConfig = typename CacheT::AccessConfig;
  using ChainedItemMovingSync = typename CacheT::ChainedItemMovingSync;
  using RemoveCb = typename CacheT::RemoveCb;
  using NvmCacheFilterCb = typename CacheT::NvmCacheFilterCb;
  using NvmCacheEncodeCb = typename CacheT::NvmCacheT::EncodeCB;
  using NvmCacheDecodeCb = typename CacheT::NvmCacheT::DecodeCB;
  using NvmCacheDeviceEncryptor = typename CacheT::NvmCacheT::DeviceEncryptor;
  using MoveCb = typename CacheT::MoveCb;
  using NvmCacheConfig = typename CacheT::NvmCacheT::Config;
  using Key = typename CacheT::Key;
  using EventTrackerSharedPtr = std::shared_ptr<typename CacheT::EventTracker>;

  // Set cache name as a string
  CacheAllocatorConfig& setCacheName(const std::string&);

  // Set cache size in bytes
  CacheAllocatorConfig& setCacheSize(size_t _size);

  // Set default allocation sizes for a cache pool
  CacheAllocatorConfig& setDefaultAllocSizes(std::set<uint32_t> allocSizes);

  // Set default allocation sizes based on arguments
  CacheAllocatorConfig& setDefaultAllocSizes(
      double _allocationClassSizeFactor,
      uint32_t _maxAllocationClassSize,
      uint32_t _minAllocationClassSize,
      bool _reduceFragmentationInAllocationClass);

  // Set the access config for cachelib's access container
  // https://our.internmc.facebook.com/intern/wiki/Cachelib-tutorial/#accessconfig
  CacheAllocatorConfig& setAccessConfig(AccessConfig config);

  // RemoveCallback is invoked for each item that is evicted or removed
  // explicitly
  CacheAllocatorConfig& setRemoveCallback(RemoveCb cb);

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

  // enables filtering items that go into nvmcache
  CacheAllocatorConfig& setNvmCacheFilterCallback(NvmCacheFilterCb cb);

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

  // Enable compact cache support
  // https://our.internmc.facebook.com/intern/wiki/Cachelib-tutorial/#compact-cache
  CacheAllocatorConfig& enableCompactCache();

  // Configure chained items. Get more details at:
  // https://our.internmc.facebook.com/intern/wiki/Cachelib-tutorial/#chained-allocations
  //
  // @param config  Config for chained item's access container, it's similar to
  //                the main access container but only used for chained items
  // @param lockPower  this controls the number of locks (2^lockPower) for
  //                   synchronizing operations on chained items.
  CacheAllocatorConfig& configureChainedItems(AccessConfig config = {},
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
  // to preserve the cache when releasing a new version of your service.
  // https://our.internmc.facebook.com/intern/wiki/Cachelib-tutorial/#cache-persistence
  CacheAllocatorConfig& enableCachePersistence(std::string directory,
                                               void* baseAddr = nullptr);

  // uses posix shm segments insteaad of the default sys-v shm segments.
  CacheAllocatorConfig& usePosixForShm();

  // This controls whether or not removing expired items on calling find()
  CacheAllocatorConfig& setItemReaperOnFind(bool enable);

  // This turns on a background worker that periodically scans through the
  // access container and look for expired items and remove them.
  CacheAllocatorConfig& enableItemReaperInBackground(
      std::chrono::milliseconds interval,
      uint32_t iterationPerRound,
      util::Throttler::Config config = {},
      bool waitUntilEvictions = true);

  // enables a faster mechanism to reap the expired items when they are
  // smaller fraction of the cost.
  CacheAllocatorConfig& enableSlabWalkReaper();

  // Enables free memory monitoring. This lets CacheAllocator shrink the cache
  // size when the system is under memory pressure. Cache will grow back when
  // the memory pressure goes down.
  // @param interval  waits for an interval between each run
  // @param advisedPercentPerIteration  amount of memory to shrink/grow per
  //                                    iteration. This is computed like:
  //             (advisedPercentPerIteration / 100) * (upperLimit - lowerLimit)
  // @param lowerLimit  the lower bound of free memory in the system, once
  //                    this is reached, the memory monitor will start shrinking
  //                    cache size
  // @param uppperLimit the upper bound of free memory in the system, once
  //                    this is reached, the memory monitor will start growing
  //                    cache size until the initial configured cache size
  // @param RebalanceStrategy  an optional strategy to customize where the slab
  //                           to give up when shrinking cache
  CacheAllocatorConfig& enableFreeMemoryMonitor(
      std::chrono::milliseconds interval,
      uint32_t advisePercentPerIteration,
      uint32_t maxAdvisePercentage,
      uint32_t lowerLimit,
      uint32_t upperLimit,
      std::shared_ptr<RebalanceStrategy> = {});

  CacheAllocatorConfig& enableTestMemoryMonitor(
      std::chrono::milliseconds interval);

  // This is similar to the above, but for use cases running on Tupperware
  // However, lowerLimit and upperLimit are the opposite to the above.
  // "upperLimit" here reffers to the upper bound of application memory.
  // Cache size will actually decrease once an application reaches past it.
  // Vice-versa for "lowerLimit"
  CacheAllocatorConfig& enableResidentMemoryMonitor(
      std::chrono::milliseconds interval,
      uint32_t advisePercentPerIteration,
      uint32_t maxAdvisePercentage,
      uint32_t lowerLimit,
      uint32_t upperLimit,
      std::shared_ptr<RebalanceStrategy> = {});

  // Enable pool rebalancing. This allows each pool to internally rebalance
  // slab memory distributed across different allocation classes. For example,
  // if the 64 bytes allocation classes are receiving for allocation requests,
  // eventually CacheAllocator will move more memory to it from other allocation
  // classes. For more details, see:
  // https://our.internmc.facebook.com/intern/wiki/Cachelib-tutorial/#rebalancing
  CacheAllocatorConfig& enablePoolRebalancing(
      std::shared_ptr<RebalanceStrategy> defaultRebalanceStrategy,
      std::chrono::milliseconds interval);

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

  // This customizes how many items we try to evict before giving up.
  // We may fail to evict if someone else (another thread) is using an item.
  // Setting this to a high limit leads to a higher chance of successful
  // evictions but it can lead to higher allocation latency as well.
  // Unless you're very familiar with caching, come talk to Cache Library team
  // before you start customizing this option.
  CacheAllocatorConfig& setEvictionSearchLimit(uint32_t limit);

  // This throttles various internal operations in cachelib. It may help improve
  // latency in allocation and find paths. Come talk to Cache Library team if
  // you find yourself customizing this.
  CacheAllocatorConfig& setThrottlerConfig(util::Throttler::Config config);

  // Disable eviction. Not recommended unless you want to use cachelib as
  // a in-memory storage. If you find yourself needing this, please come talk
  // to Cache Library team. There is usually a better solutuon.
  CacheAllocatorConfig& disableCacheEviction();

  // Set a callback to be invoked after a cache worker has finished its round
  // of work.
  CacheAllocatorConfig& setCacheWorkerPostWorkHandler(std::function<void()> cb);

  // Passes in a callback to initialize an event tracker when the allocator
  // starts
  CacheAllocatorConfig& setEventTracker(EventTrackerSharedPtr&&);

  bool isCompactCacheEnabled() const noexcept { return enableZeroedSlabAllocs; }

  bool poolResizingEnabled() const noexcept {
    return poolResizeInterval.count() > 0 && poolResizeSlabsPerIter > 0;
  }

  bool poolRebalancingEnabled() const noexcept {
    return poolRebalanceInterval.count() > 0 &&
           defaultPoolRebalanceStrategy != nullptr;
  }

  bool poolOptimizerEnabled() const noexcept {
    return (regularPoolOptimizeInterval.count() > 0 ||
            compactCacheOptimizeInterval.count() > 0) &&
           poolOptimizeStrategy != nullptr;
  }

  bool memMonitoringEnabled() const noexcept {
    return memMonitorMode != MemoryMonitor::Disabled &&
           memMonitorInterval.count() > 0;
  }

  bool canAllocatePermanent() const noexcept {
    return ((poolRebalancingEnabled() || poolResizingEnabled()) && moveCb) ||
           (!poolRebalancingEnabled() && !poolResizingEnabled());
  }

  bool itemsReaperEnabled() const noexcept {
    return reaperInterval.count() > 0;
  }

  bool mmReconfigureEnabled() const noexcept {
    return mmReconfigureInterval.count() > 0;
  }

  const std::string& getCacheDir() const noexcept { return cacheDir; }

  // validate the config, and return itself if valid
  const CacheAllocatorConfig& validate() const {
    // we can track tail hits only if MMType is MM2Q
    if (trackTailHits && CacheT::MMType::kId != MM2Q::kId) {
      throw std::invalid_argument(
          "Tail hits tracking cannot be enabled on MMTypes except MM2Q.");
    }
    return *this;
  }

  // check whether the RebalanceStrategy can be used with this config
  bool validateStrategy(
      const std::shared_ptr<RebalanceStrategy>& strategy) const;

  // check whether the PoolOptimizeStrategy can be used with this config
  bool validateStrategy(
      const std::shared_ptr<PoolOptimizeStrategy>& strategy) const;

  std::map<std::string, std::string> serialize() const;

  // Cache name for users to indentify their own cache.
  std::string cacheName{""};

  // Amount of memory for this cache instance
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

  // Free slabs pro-actively if the ratio of number of freeallocs to
  // the number of allocs per slab in a slab class is above this
  // threshold
  // A value of 0 means, this feature is disabled.
  unsigned int poolRebalancerFreeAllocThreshold{0};

  // rebalancing strategy for all pools. By default the strategy will
  // rebalance to avoid alloc fialures.
  std::shared_ptr<RebalanceStrategy> defaultPoolRebalanceStrategy{
      new RebalanceStrategy{}};

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

  // Memory monitoring mode. Enable memory monitoring by setting this to
  // MemoryMonitor::ResidentMemory or MemoryMonitor::FreeMemory mode.
  MemoryMonitor::Mode memMonitorMode{MemoryMonitor::Disabled};

  // time interval to sleep between iterations of monitoring memory.
  // Set to 0 to disable memory monitoring
  std::chrono::milliseconds memMonitorInterval{0};

  // percentage of memUpperLimit - memLowerLimit to be advised away or
  // recliamed in an iteration.
  unsigned int memAdviseReclaimPercentPerIter{5};

  // maximum percentage of item cache that can be advised away
  unsigned int memMaxAdvisePercent{20};

  // lower limit for free/resident memory in GBs.
  // Note: the lower/upper limit is used in exactly opposite ways for the
  // FreeMemory versus ResidentMemory mode.
  // 1. In the ResidentMemory mode, when the resident memory usage drops
  // below this limit, advised away slabs are reclaimed in proportion to
  // the size of pools, to increase cache size and raise resident memory
  // above this limit.
  // 2. In the FreeMemory mode, when the system free memory drops below this
  // limit, slabs are advised away from pools in proportion to their size to
  // raise system free memory above this limit.
  unsigned int memLowerLimit{10};

  // upper limit for free/resident memory in GBs.
  // Note: the lower/upper limit is used in exactly opposite ways for the
  // FreeMemory versus ResidentMemory mode.
  // 1. In the ResidentMemory mode, when the resident memory usage exceeds
  // this limit, slabs are advised away from pools in proportion to their
  // size to reduce resident memory usage below this limit.
  // 2. In the FreeMemory mode, when the system free memory exceeds
  // this limit and if there are slabs that were advised away earlier,
  // they're reclaimed by pools in proportion to their sizes to reduce the
  // system free memory below this limit.
  unsigned int memUpperLimit{15};

  // enable removing TTL-expired items in cache rightaway during find
  bool reapExpiredItemsOnFind{true};

  // throttler config of items reaper for iteration
  util::Throttler::Config reaperConfig{};

  // time to sleep between each reaping period.
  std::chrono::milliseconds reaperInterval{};

  // number of items to iterate in each reaping period.
  uint32_t reapIterationEachTime{10000};

  // if true, reaper activates only after evictions happened. If evictions
  // disabled (@disableEviction), this is ignored and reaper doesn't wait for
  // evictions to happen (like this flag is false).
  bool reaperWaitUntilEvictions{true};

  // enables a faster mechanism to reap the expired items when they are
  // smaller fraction of the cost.
  bool reaperSlabWalkMode{false};

  // interval during which we adjust dynamically the refresh ratio.
  std::chrono::milliseconds mmReconfigureInterval{0};

  // user-defined post work handler that will be executed once for
  // every cache worker
  std::function<void()> cacheWorkerPostWorkHandler{};
  //
  // TODO:
  // ABOVE are the config for various cache workers
  //

  // if turned on, cache allocator will not evict any item when the
  // system is out of memory. The user must free previously allocated
  // items to make more room.
  bool disableEviction = false;

  // the number of tries to search for an item to evict
  // 0 means it's infinite
  unsigned int evictionSearchTries{50};

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

  // user defined callback invoked when an item is being evicted or freed
  RemoveCb removeCb{};

  // user defined call back to move the item. This is executed while holding
  // the user provided movingSync. For items without chained allocations,
  // there is no specific need for explicit movingSync and user can skip
  // providing a movingSync and do explicit synchronization just in the
  // moveCb if needed to protect the data being moved with concurrent
  // readers.
  MoveCb moveCb{};

  // custom user provided admission policy
  std::shared_ptr<NvmAdmissionPolicy<CacheT>> nvmCacheAP{nullptr};

  // user defined call back for filtering out items that go into the nvm
  // cache. this will keep certain items only in DRAM.
  NvmCacheFilterCb filterCb{};

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

  friend CacheT;

 private:
  void mergeWithPrefix(
      std::map<std::string, std::string>& configMap,
      const std::map<std::string, std::string>& configMapToMerge,
      const std::string& prefix) const;
  std::string stringifyAddr(const void* addr) const;
  std::string stringifyRebalanceStrategy(
      const std::shared_ptr<RebalanceStrategy>& strategy) const;
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
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setRemoveCallback(
    RemoveCb cb) {
  removeCb = std::move(cb);
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
        "NvmCache filter callback can not be set unless nvmcache is used");
  }

  if (!policy) {
    throw std::invalid_argument("Setting a null admission policy");
  }

  nvmCacheAP = std::move(policy);
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setNvmCacheFilterCallback(
    NvmCacheFilterCb cb) {
  if (!nvmConfig) {
    throw std::invalid_argument(
        "NvmCache filter callback can not be set unless nvmcache is used");
  }
  filterCb = std::move(cb);
  return *this;
}

// enables encoding items before they go into nvmcache
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

// enables decoding items before they get back into ram cache
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
  if (!cacheDir.empty()) {
    usePosixShm = true;
  }
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setItemReaperOnFind(
    bool enable) {
  reapExpiredItemsOnFind = enable;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableItemReaperInBackground(
    std::chrono::milliseconds interval,
    uint32_t iterationPerRound,
    util::Throttler::Config config,
    bool waitUntilEvictions) {
  reaperInterval = interval;
  reapIterationEachTime = iterationPerRound;
  reaperConfig = config;
  reaperWaitUntilEvictions = waitUntilEvictions;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableSlabWalkReaper() {
  if (!itemsReaperEnabled()) {
    throw std::invalid_argument(
        "Enable reaper before enabling super charged mode");
  }
  reaperSlabWalkMode = true;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::disableCacheEviction() {
  disableEviction = true;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableFreeMemoryMonitor(
    std::chrono::milliseconds interval,
    uint32_t advisePercentPerIteration,
    uint32_t maxAdvisePercentage,
    uint32_t lowerFreeMemLimit,
    uint32_t upperFreeMemLimit,
    std::shared_ptr<RebalanceStrategy> adviseStrategy) {
  memMonitorMode = MemoryMonitor::FreeMemory;
  memMonitorInterval = interval;
  memAdviseReclaimPercentPerIter = advisePercentPerIteration;
  memMaxAdvisePercent = maxAdvisePercentage;
  memLowerLimit = lowerFreeMemLimit;
  memUpperLimit = upperFreeMemLimit;
  poolAdviseStrategy = adviseStrategy;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::enableResidentMemoryMonitor(
    std::chrono::milliseconds interval,
    uint32_t advisePercentPerIteration,
    uint32_t maxAdvisePercentage,
    uint32_t lowerResidentMemoryLimit,
    uint32_t upperResidentMemoryLimit,
    std::shared_ptr<RebalanceStrategy> adviseStrategy) {
  memMonitorMode = MemoryMonitor::ResidentMemory;
  memMonitorInterval = interval;
  memAdviseReclaimPercentPerIter = advisePercentPerIteration;
  memMaxAdvisePercent = maxAdvisePercentage;
  memLowerLimit = lowerResidentMemoryLimit;
  memUpperLimit = upperResidentMemoryLimit;
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
    std::chrono::milliseconds interval) {
  if (validateStrategy(defaultRebalanceStrategy)) {
    defaultPoolRebalanceStrategy = defaultRebalanceStrategy;
    poolRebalanceInterval = interval;
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
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setEvictionSearchLimit(
    uint32_t limit) {
  evictionSearchTries = limit;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setThrottlerConfig(
    util::Throttler::Config config) {
  throttleConfig = config;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setCacheWorkerPostWorkHandler(
    std::function<void()> cb) {
  cacheWorkerPostWorkHandler = cb;
  return *this;
}

template <typename T>
CacheAllocatorConfig<T>& CacheAllocatorConfig<T>::setEventTracker(
    EventTrackerSharedPtr&& otherEventTracker) {
  eventTracker = std::move(otherEventTracker);
  return *this;
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
std::map<std::string, std::string> CacheAllocatorConfig<T>::serialize() const {
  std::map<std::string, std::string> configMap;

  configMap["size"] = std::to_string(size);
  configMap["cacheDir"] = cacheDir;

  configMap["defaultAllocSizes"] = "";
  // Stringify std::set
  for (auto& elem : defaultAllocSizes) {
    if (configMap["defaultAllocSizes"] != "") {
      configMap["defaultAllocSizes"] += ", ";
    }
    configMap["defaultAllocSizes"] += std::to_string(elem);
  }
  configMap["disableFullCoredump"] = std::to_string(disableFullCoredump);
  configMap["trackRecentItemsForDump"] =
      std::to_string(trackRecentItemsForDump);
  configMap["poolResizeInterval"] = util::toString(poolResizeInterval);
  configMap["poolResizeSlabsPerIter"] = std::to_string(poolResizeSlabsPerIter);

  configMap["poolRebalanceInterval"] = util::toString(poolRebalanceInterval);
  configMap["trackTailHits"] = std::to_string(trackTailHits);
  // Stringify enum
  switch (memMonitorMode) {
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
  configMap["memAdviseReclaimPercentPerIter"] =
      std::to_string(memAdviseReclaimPercentPerIter);
  configMap["memMaxAdvisePercent"] = std::to_string(memMaxAdvisePercent);
  configMap["memLowerLimit"] = std::to_string(memLowerLimit);
  configMap["memUpperLimit"] = std::to_string(memUpperLimit);
  configMap["reapExpiredItemsOnFind"] = std::to_string(reapExpiredItemsOnFind);
  configMap["reaperInterval"] = util::toString(reaperInterval);
  configMap["reapIterationEachTime"] = std::to_string(reapIterationEachTime);
  configMap["reaperWaitUntilEvictions"] =
      std::to_string(reaperWaitUntilEvictions);
  configMap["reaperSlabWalkMode"] = std::to_string(reaperSlabWalkMode);
  configMap["mmReconfigureInterval"] = util::toString(mmReconfigureInterval);
  configMap["cacheWorkerPostWorkHandler"] =
      (cacheWorkerPostWorkHandler ? "set" : "empty");
  configMap["disableEviction"] = std::to_string(disableEviction);
  configMap["evictionSearchTries"] = std::to_string(evictionSearchTries);
  configMap["movingTries"] = std::to_string(movingTries);
  configMap["chainedItemsLockPower"] = std::to_string(chainedItemsLockPower);
  configMap["removeCb"] = removeCb ? "set" : "empty";
  configMap["filterCb"] = filterCb ? "set" : "empty";
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
