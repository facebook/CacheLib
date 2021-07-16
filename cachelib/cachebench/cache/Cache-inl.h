#include <folly/DynamicConverter.h>
#include <folly/Format.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <iostream>

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/cachebench/cache/ItemRecords.h"
#include "cachelib/cachebench/util/NandWrites.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

template <typename Allocator>
uint64_t Cache<Allocator>::fetchNandWrites() const {
  size_t total = 0;
  for (const auto& d : config_.writeAmpDeviceList) {
    try {
      total += facebook::hw::nandWriteBytes(d);
    } catch (const std::exception& e) {
      XLOGF(ERR, "Exception fetching nand writes for {}. Msg: {}", d, e.what());
      return 0;
    }
  }
  return total;
}

template <typename Allocator>
Cache<Allocator>::Cache(CacheConfig config,
                        ChainedItemMovingSync movingSync,
                        std::string cacheDir)
    : config_(config),
      cacheDir_(cacheDir),
      nandBytesBegin_{fetchNandWrites()},
      itemRecords_(config_.enableItemDestructorCheck) {
  constexpr size_t MB = 1024ULL * 1024ULL;

  typename Allocator::Config allocatorConfig;

  allocatorConfig.enablePoolRebalancing(
      config_.getRebalanceStrategy(),
      std::chrono::seconds(config_.poolRebalanceIntervalSec));

  if (config_.moveOnSlabRelease && movingSync != nullptr) {
    allocatorConfig.enableMovingOnSlabRelease(
        [](Item& oldItem, Item& newItem, Item* parentPtr) {
          XDCHECK(oldItem.isChainedItem() == (parentPtr != nullptr));
          std::memcpy(newItem.getWritableMemory(), oldItem.getMemory(),
                      oldItem.getSize());
        },
        movingSync);
  }

  if (config_.allocSizes.empty()) {
    XDCHECK(config_.minAllocSize >= sizeof(CacheValue));
    allocatorConfig.setDefaultAllocSizes(util::generateAllocSizes(
        config_.allocFactor, config_.maxAllocSize, config_.minAllocSize, true));
  } else {
    std::set<uint32_t> allocSizes;
    for (uint64_t s : config_.allocSizes) {
      XDCHECK(s >= sizeof(CacheValue));
      allocSizes.insert(s);
    }
    allocatorConfig.setDefaultAllocSizes(std::move(allocSizes));
  }

  // Set hash table config
  allocatorConfig.setAccessConfig(typename Allocator::AccessConfig{
      static_cast<uint32_t>(config_.htBucketPower),
      static_cast<uint32_t>(config_.htLockPower)});

  allocatorConfig.configureChainedItems(typename Allocator::AccessConfig{
      static_cast<uint32_t>(config_.chainedItemHtBucketPower),
      static_cast<uint32_t>(config_.chainedItemHtLockPower)});

  allocatorConfig.setCacheSize(config_.cacheSizeMB * (MB));

  auto cleanupGuard = folly::makeGuard([&] {
    if (shouldCleanupFiles_) {
      for (const auto& f : nvmCacheFilePaths_) {
        util::removePath(f);
      }
    }
  });

  if (config_.enableItemDestructorCheck) {
    // TODO (zixuan) use ItemDestructor once feature is finished
    auto removeCB = [&](const typename Allocator::RemoveCbData& data) {
      if (!itemRecords_.validate(data)) {
        ++invalidDestructor_;
      }
      // in the end of test, total destructor invocations must be equal to total
      // size of itemRecords_ (also is the number of new allocations)
      ++totalDestructor_;
    };
    allocatorConfig.setRemoveCallback(removeCB);
  } else if (config_.enableItemDestructor) {
    // TODO (zixuan) use ItemDestructor once feature is finished
    auto removeCB = [&](const typename Allocator::RemoveCbData&) {};
    allocatorConfig.setRemoveCallback(removeCB);
  }

  // Set up Navy
  if (!isRamOnly()) {
    typename Allocator::NvmCacheConfig nvmConfig;

    nvmConfig.navyConfig.setBlockCacheDataChecksum(config_.navyDataChecksum);

    nvmConfig.enableFastNegativeLookups = true;

    if (config_.nvmCachePaths.size() == 1) {
      // if we get a directory, create a file. we will clean it up. If we
      // already have a file, user provided it. We will also keep it around
      // after the tests.
      auto path = config_.nvmCachePaths[0];
      if (cachelib::util::isDir(path)) {
        const auto uniqueSuffix = folly::sformat("nvmcache_{}_{}", ::getpid(),
                                                 folly::Random::rand32());
        path = path + "/" + uniqueSuffix;
        util::makeDir(path);
        shouldCleanupFiles_ = true;
        nvmConfig.navyConfig.setSimpleFile(path + "/navy_cache",
                                           config_.nvmCacheSizeMB * MB,
                                           true /*truncateFile*/);
      } else {
        nvmConfig.navyConfig.setSimpleFile(path, config_.nvmCacheSizeMB * MB);
      }
      nvmCacheFilePaths_.push_back(path);
    } else if (config_.nvmCachePaths.size() > 1) {
      // set up a software raid-0 across each nvm cache path.
      nvmConfig.navyConfig.setRaidFiles(config_.nvmCachePaths,
                                        config_.nvmCacheSizeMB * MB);
    } else {
      // use memory to mock NVM.
      nvmConfig.navyConfig.setMemoryFile(config_.nvmCacheSizeMB * MB);
    }

    if (config_.navyNumInmemBuffers > 0) {
      nvmConfig.navyConfig.setBlockCacheNumInMemBuffers(
          config_.navyNumInmemBuffers);
    }

    if (config_.navyReqOrderShardsPower != 0) {
      nvmConfig.navyConfig.setNavyReqOrderingShards(
          config_.navyReqOrderShardsPower);
    }

    // by default lru. if more than one fifo ratio is present, we use
    // segmented fifo. otherwise, simple fifo.
    if (!config_.navySegmentedFifoSegmentRatio.empty()) {
      if (config.navySegmentedFifoSegmentRatio.size() == 1) {
        nvmConfig.navyConfig.blockCache().enableFifo();
      } else {
        nvmConfig.navyConfig.blockCache().enableSegmentedFifo(
            config_.navySegmentedFifoSegmentRatio);
      }
    }
    nvmConfig.navyConfig.setBlockSize(config_.navyBlockSize);
    nvmConfig.navyConfig.setBlockCacheRegionSize(16 * MB);

    if (!config_.navySizeClasses.empty()) {
      nvmConfig.navyConfig.setBlockCacheSizeClasses(config_.navySizeClasses);
    }

    if (config_.navyBigHashSizePct > 0) {
      nvmConfig.navyConfig.setBigHash(config_.navyBigHashSizePct,
                                      config_.navyBigHashBucketSize,
                                      config_.navyBloomFilterPerBucketSize,
                                      config_.navySmallItemMaxSize);
    }

    nvmConfig.navyConfig.setMaxParcelMemoryMB(config_.navyParcelMemoryMB);

    if (config_.navyHitsReinsertionThreshold > 0) {
      nvmConfig.navyConfig.setBlockCacheReinsertionHitsThreshold(
          static_cast<uint8_t>(config_.navyHitsReinsertionThreshold));
    }
    if (config_.navyProbabilityReinsertionThreshold > 0) {
      nvmConfig.navyConfig.setBlockCacheReinsertionProbabilityThreshold(
          config_.navyProbabilityReinsertionThreshold);
    }

    nvmConfig.navyConfig.setReaderAndWriterThreads(config_.navyReaderThreads,
                                                   config_.navyWriterThreads);

    nvmConfig.navyConfig.setBlockCacheCleanRegions(config_.navyCleanRegions);
    if (config_.navyAdmissionWriteRateMB > 0) {
      nvmConfig.navyConfig.setAdmissionPolicy("dynamic_random");
      nvmConfig.navyConfig.setAdmissionWriteRate(
          config_.navyAdmissionWriteRateMB * MB);
    }
    nvmConfig.navyConfig.setMaxConcurrentInserts(
        config_.navyMaxConcurrentInserts);

    nvmConfig.truncateItemToOriginalAllocSizeInNvm =
        config_.truncateItemToOriginalAllocSizeInNvm;

    XLOG(INFO) << "Using the following nvm config"
               << folly::toPrettyJson(
                      folly::toDynamic(nvmConfig.navyConfig.serialize()));
    allocatorConfig.enableNvmCache(nvmConfig);

    if (config_.navyEncryption && config_.createEncryptor) {
      allocatorConfig.enableNvmCacheEncryption(config_.createEncryptor());
    }
    if (!config_.mlNvmAdmissionPolicy.empty() &&
        config_.nvmAdmissionPolicyFactory) {
      try {
        nvmAdmissionPolicy_ =
            std::any_cast<std::shared_ptr<NvmAdmissionPolicy<Allocator>>>(
                config_.nvmAdmissionPolicyFactory(config_));
        allocatorConfig.setNvmCacheAdmissionPolicy(nvmAdmissionPolicy_);
      } catch (const std::bad_any_cast& e) {
        XLOG(ERR) << "CAST ERROR " << e.what();
        throw;
      }
    }

    allocatorConfig.setNvmAdmissionMinTTL(config_.memoryOnlyTTL);
  }

  allocatorConfig.cacheName = "cachebench";

  if (!cacheDir_.empty()) {
    allocatorConfig.cacheDir = cacheDir_;
    cache_ =
        std::make_unique<Allocator>(Allocator::SharedMemNew, allocatorConfig);
  } else {
    cache_ = std::make_unique<Allocator>(allocatorConfig);
  }
  allocatorConfig_ = allocatorConfig;
  const size_t numBytes = cache_->getCacheMemoryStats().cacheSize;
  for (uint64_t i = 0; i < config_.numPools; ++i) {
    const double& ratio = config_.poolSizes[i];
    const size_t poolSize = static_cast<size_t>(numBytes * ratio);
    typename Allocator::MMConfig mmConfig =
        makeMMConfig<typename Allocator::MMConfig>(config_);
    const PoolId pid = cache_->addPool(
        folly::sformat("pool_{}", i), poolSize, {} /* allocSizes */, mmConfig,
        nullptr /* rebalanceStrategy */, nullptr /* resizeStrategy */,
        true /* ensureSufficientMem */);
    pools_.push_back(pid);
  }

  if (config_.cacheMonitorFactory) {
    monitor_ = config_.cacheMonitorFactory->create(*cache_);
  }

  cleanupGuard.dismiss();
}

template <typename Allocator>
Cache<Allocator>::~Cache() {
  try {
    monitor_.reset();

    // Reset cache first which will drain all nvm operations if present
    cache_.reset();

    if (shouldCleanupFiles_) {
      for (const auto& f : nvmCacheFilePaths_) {
        util::removePath(f);
      }
    }
  } catch (...) {
  }
}

template <typename Allocator>
void Cache<Allocator>::reattach() {
  cache_ =
      std::make_unique<Allocator>(Allocator::SharedMemAttach, allocatorConfig_);
}

template <typename Allocator>
void Cache<Allocator>::enableConsistencyCheck(
    const std::vector<std::string>& keys) {
  XDCHECK(valueTracker_ == nullptr);
  valueTracker_ =
      std::make_unique<ValueTracker>(ValueTracker::wrapStrings(keys));
  for (const std::string& key : keys) {
    invalidKeys_[key] = false;
  }
}
template <typename Allocator>
bool Cache<Allocator>::checkGet(ValueTracker::Index opId,
                                const ItemHandle& it) {
  LogEventStream es;
  auto found = it != nullptr;
  uint64_t expected = 0;
  if (found) {
    if (it->hasChainedItem()) {
      expected = genHashForChain(it);
    } else {
      expected = getUint64FromItem(*it);
    }
  }
  if (!valueTracker_->endGet(opId, expected, found, &es)) {
    std::cout << (es.format() + it->toString() + "\n");
    inconsistencyCount_.fetch_add(1, std::memory_order::memory_order_acquire);
    return true;
  }
  return false;
}

template <typename Allocator>
Stats Cache<Allocator>::getStats() const {
  PoolStats aggregate = cache_->getPoolStats(pools_[0]);
  for (size_t pid = 1; pid < pools_.size(); pid++) {
    aggregate += cache_->getPoolStats(static_cast<PoolId>(pid));
  }

  const auto cacheStats = cache_->getGlobalCacheStats();
  const auto rebalanceStats = cache_->getSlabReleaseStats();
  const auto navyStats = cache_->getNvmCacheStatsMap();

  Stats ret;
  ret.numEvictions = aggregate.numEvictions();
  ret.numItems = aggregate.numItems();
  ret.allocAttempts = cacheStats.allocAttempts;
  ret.allocFailures = cacheStats.allocFailures;

  ret.numCacheGets = cacheStats.numCacheGets;
  ret.numCacheGetMiss = cacheStats.numCacheGetMiss;
  ret.numNvmGets = cacheStats.numNvmGets;
  ret.numNvmGetMiss = cacheStats.numNvmGetMiss;
  ret.numNvmGetCoalesced = cacheStats.numNvmGetCoalesced;
  ret.numNvmRejectsByExpiry = cacheStats.numNvmRejectsByExpiry;
  ret.numNvmRejectsByClean = cacheStats.numNvmRejectsByClean;

  ret.numNvmPuts = cacheStats.numNvmPuts;
  ret.numNvmPutErrs = cacheStats.numNvmPutErrs;
  ret.numNvmAbortedPutOnTombstone = cacheStats.numNvmAbortedPutOnTombstone;
  ret.numNvmAbortedPutOnInflightGet = cacheStats.numNvmAbortedPutOnInflightGet;
  ret.numNvmPutFromClean = cacheStats.numNvmPutFromClean;
  ret.numNvmUncleanEvict = cacheStats.numNvmUncleanEvict;
  ret.numNvmCleanEvict = cacheStats.numNvmCleanEvict;
  ret.numNvmCleanDoubleEvict = cacheStats.numNvmCleanDoubleEvict;
  ret.numNvmEvictions = cacheStats.numNvmEvictions;

  ret.numNvmDeletes = cacheStats.numNvmDeletes;
  ret.numNvmSkippedDeletes = cacheStats.numNvmSkippedDeletes;

  ret.slabsReleased = rebalanceStats.numSlabReleaseForRebalance;
  ret.moveAttemptsForSlabRelease = rebalanceStats.numMoveAttempts;
  ret.moveSuccessesForSlabRelease = rebalanceStats.numMoveSuccesses;
  ret.evictionAttemptsForSlabRelease = rebalanceStats.numEvictionAttempts;
  ret.evictionSuccessesForSlabRelease = rebalanceStats.numEvictionSuccesses;

  ret.inconsistencyCount = getInconsistencyCount();
  ret.isNvmCacheDisabled = isNvmCacheDisabled();
  ret.invalidDestructorCount = invalidDestructor_;
  ret.unDestructedItemCount =
      static_cast<int64_t>(itemRecords_.count()) - totalDestructor_;

  ret.cacheAllocateLatencyNs = cacheStats.allocateLatencyNs;
  ret.cacheFindLatencyNs = cacheFindLatency_.estimate();

  // Populate counters.
  // TODO: Populate more counters that are interesting to cachebench.
  if (config_.printNvmCounters) {
    ret.nvmCounters = cache_->getNvmCacheStatsMap();
  }

  // nvm stats from navy
  if (!isRamOnly() && !navyStats.empty()) {
    auto lookup = [&navyStats](const std::string& key) {
      return navyStats.find(key) != navyStats.end()
                 ? static_cast<uint64_t>(navyStats.at(key))
                 : 0;
    };
    ret.numNvmItems = lookup("navy_bh_items") + lookup("navy_bc_items");
    ret.numNvmBytesWritten = lookup("navy_device_bytes_written");
    uint64_t now = fetchNandWrites();
    if (now > nandBytesBegin_) {
      ret.numNvmNandBytesWritten = now - nandBytesBegin_;
    }
    double bhLogicalBytes = lookup("navy_bh_logical_written");
    double bcLogicalBytes = lookup("navy_bc_logical_written");
    ret.numNvmLogicalBytesWritten =
        static_cast<size_t>(bhLogicalBytes + bcLogicalBytes);
    ret.nvmReadLatencyMicrosP50 = lookup("navy_device_read_latency_us_p50");
    ret.nvmReadLatencyMicrosP90 = lookup("navy_device_read_latency_us_p90");
    ret.nvmReadLatencyMicrosP99 = lookup("navy_device_read_latency_us_p99");
    ret.nvmReadLatencyMicrosP999 = lookup("navy_device_read_latency_us_p999");
    ret.nvmReadLatencyMicrosP9999 = lookup("navy_device_read_latency_us_p9999");
    ret.nvmReadLatencyMicrosP99999 =
        lookup("navy_device_read_latency_us_p99999");
    ret.nvmReadLatencyMicrosP999999 =
        lookup("navy_device_read_latency_us_p999999");
    ret.nvmReadLatencyMicrosP100 = lookup("navy_device_read_latency_us_p100");
    ret.nvmWriteLatencyMicrosP50 = lookup("navy_device_write_latency_us_p50");
    ret.nvmWriteLatencyMicrosP90 = lookup("navy_device_write_latency_us_p90");
    ret.nvmWriteLatencyMicrosP99 = lookup("navy_device_write_latency_us_p99");
    ret.nvmWriteLatencyMicrosP999 = lookup("navy_device_write_latency_us_p999");
    ret.nvmWriteLatencyMicrosP9999 =
        lookup("navy_device_write_latency_us_p9999");
    ret.nvmWriteLatencyMicrosP99999 =
        lookup("navy_device_write_latency_us_p99999");
    ret.nvmWriteLatencyMicrosP999999 =
        lookup("navy_device_write_latency_us_p999999");
    ret.nvmWriteLatencyMicrosP100 = lookup("navy_device_write_latency_us_p100");

    // track any non-zero check sum errors or io errors
    for (const auto& [k, v] : navyStats) {
      if (v != 0 && ((k.find("checksum_error") != std::string::npos) ||
                     (k.find("io_error") != std::string::npos))) {
        ret.nvmErrors.insert(std::make_pair(k, v));
      }
    }
  }

  return ret;
}

template <typename Allocator>
void Cache<Allocator>::clearCache() {
  if (config_.enableItemDestructorCheck) {
    // all items leftover in the cache must be removed
    // at the end of the test to trigger ItemDestrutor
    // so that we are able to check if ItemDestructor
    // are called once and only once for every item.
    auto keys = itemRecords_.getKeys();
    XLOGF(DBG, "clearCache keys: {}", keys.size());
    for (const auto& key : keys) {
      cache_->remove(key);
    }
    cache_->flushNvmCache();
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
