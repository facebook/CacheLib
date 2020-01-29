#include <iostream>

#include <sys/stat.h>
#include <sys/types.h>

#include <boost/filesystem.hpp>

#include <folly/Format.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>

#include "cachelib/allocator/Util.h"
#include "cachelib/cachebench/util/NandWrites.h"
#include "dipper/dipper_registry.h"
#include "dipper/navy_dipper/navyif.h"

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
    : config_(config), cacheDir_(cacheDir), nandBytesBegin_{fetchNandWrites()} {
  constexpr size_t MB = 1024ULL * 1024ULL;

  typename Allocator::Config allocatorConfig;

  allocatorConfig.enablePoolRebalancing(
      config_.getRebalanceStrategy(),
      std::chrono::seconds(config_.poolRebalanceIntervalSec));

  if (config_.moveOnSlabRelease && movingSync != nullptr) {
    allocatorConfig.enableMovingOnSlabRelease(
        [](Item& oldItem, Item& newItem) {
          std::memcpy(newItem.getMemory(), oldItem.getMemory(),
                      oldItem.getSize());
        },
        movingSync);
  }

  if (config_.allocSizes.empty()) {
    allocatorConfig.setDefaultAllocSizes(
        util::generateAllocSizes(config_.allocFactor));
  } else {
    std::set<uint32_t> allocSizes;
    for (uint64_t s : config_.allocSizes) {
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
      boost::filesystem::remove_all(config_.dipperFilePath);
    }
  });

  // Set up Navy
  if (!config_.dipperBackend.empty()) {
    CHECK_EQ(config_.dipperBackend, "navy_dipper");

    typename Allocator::NvmCacheConfig nvmConfig;
    nvmConfig.dipperOptions = folly::dynamic::object;
    nvmConfig.dipperOptions["dipper_backend"] = config_.dipperBackend;
    nvmConfig.dipperOptions["dipper_force_reinit"] = true;
    nvmConfig.dipperOptions["dipper_compression"] =
        static_cast<int>(dipper::DipperCompressionMethod::DCM_NONE);

    nvmConfig.dipperOptions["dipper_async_threads"] =
        config_.dipperAsyncThreads;

    facebook::dipper::registerBackend<facebook::dipper::NavyDipperFactory>();
    if (config_.dipperNavyUseMemoryDevice) {
      // nothing to do here.
    } else if (!config_.dipperDevicePath.empty()) {
      nvmConfig.dipperOptions["dipper_navy_file_name"] =
          config_.dipperDevicePath;
    } else {
      CHECK(!config_.dipperFilePath.empty());
      // if we get a directory, create a file. we will clean it up. If we
      // already have a file, user provided it. So we will also keep it around
      // after the tests.
      if (cachelib::util::isDir(config_.dipperFilePath)) {
        const auto path = boost::filesystem::unique_path(
            std::string("nvmcache_" + config_.dipperBackend) +
            ".%%%%-%%%%-%%%%");
        config_.dipperFilePath = (config_.dipperFilePath / path).string();
        boost::filesystem::create_directories(config_.dipperFilePath);
        shouldCleanupFiles_ = true;
        nvmConfig.dipperOptions["dipper_navy_truncate_file"] = true;
        nvmConfig.dipperOptions["dipper_navy_file_name"] =
            config_.dipperFilePath + "/navy_cache";
      } else {
        nvmConfig.dipperOptions["dipper_navy_file_name"] =
            config_.dipperFilePath;
      }
    }

    nvmConfig.dipperOptions["dipper_navy_file_size"] =
        config_.dipperSizeMB * MB;

    if (config_.dipperNavyReqOrderShardsPower != 0) {
      nvmConfig.dipperOptions["dipper_navy_req_order_shards_power"] =
          config_.dipperNavyReqOrderShardsPower;
      nvmConfig.dipperOptions["dipper_request_ordering"] = false;
    } else {
      nvmConfig.dipperOptions["dipper_request_ordering"] = true;
    }

    nvmConfig.dipperOptions["dipper_navy_direct_io"] =
        config_.dipperUseDirectIO;
    nvmConfig.dipperOptions["dipper_navy_lru"] = config_.dipperNavyUseRegionLru;
    nvmConfig.dipperOptions["dipper_navy_block_size"] = config_.dipperNavyBlock;
    nvmConfig.dipperOptions["dipper_navy_region_size"] = 16 * MB;

    if (config.dipperNavyUseStackAllocation ||
        config_.dipperNavySizeClasses.empty()) {
      nvmConfig.dipperOptions["dipper_navy_read_buffer"] =
          config_.dipperNavyStackAllocReadBufSizeKB * 1024;
    } else {
      nvmConfig.dipperOptions["dipper_navy_size_classes"] =
          folly::dynamic::array(config_.dipperNavySizeClasses.begin(),
                                config_.dipperNavySizeClasses.end());
    }

    if (config_.dipperNavyBigHashSizePct > 0) {
      nvmConfig.dipperOptions["dipper_navy_bighash_size_pct"] =
          config_.dipperNavyBigHashSizePct;
      nvmConfig.dipperOptions["dipper_navy_bighash_bucket_size"] =
          config_.dipperNavyBigHashBucketSize;
      nvmConfig.dipperOptions["dipper_navy_bighash_bucket_bf_size"] =
          config_.dipperNavyBloomFilterPerBucketSize;
      nvmConfig.dipperOptions["dipper_navy_small_item_max_size"] =
          config_.dipperNavySmallItemMaxSize;
    }

    nvmConfig.dipperOptions["dipper_navy_max_parcel_memory_mb"] =
        config_.dipperNavyParcelMemoryMB;

    if (config_.navyHitsReinsertionThreshold > 0) {
      nvmConfig.dipperOptions["dipper_navy_reinsertion_hits_threshold"] =
          config_.navyHitsReinsertionThreshold;
    }
    if (config_.navyProbabilityReinsertionThreshold > 0) {
      nvmConfig.dipperOptions["dipper_navy_reinsertion_probability_threshold"] =
          config_.navyProbabilityReinsertionThreshold;
    }

    nvmConfig.dipperOptions["dipper_navy_reader_threads"] =
        config_.navyReaderThreads;
    nvmConfig.dipperOptions["dipper_navy_writer_threads"] =
        config_.navyWriterThreads;
    nvmConfig.dipperOptions["dipper_navy_clean_regions"] =
        config_.navyCleanRegions;
    if (config_.navyAdmissionWriteRateMB > 0) {
      nvmConfig.dipperOptions["dipper_navy_adm_policy"] = "dynamic_random";
      nvmConfig.dipperOptions["dipper_navy_adm_write_rate"] =
          config_.navyAdmissionWriteRateMB * MB;
    }
    nvmConfig.dipperOptions["dipper_navy_max_concurrent_inserts"] =
        config_.navyMaxConcurrentInserts;

    nvmConfig.truncateItemToOriginalAllocSizeInNvm =
        config_.truncateItemToOriginalAllocSizeInNvm;

    XLOG(INFO) << "Using the following nvm config"
               << folly::toPrettyJson(nvmConfig.dipperOptions);
    allocatorConfig.enableNvmCache(nvmConfig);

    usesNvm_ = true;
  }

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
    const size_t poolSize = numBytes * ratio;
    typename Allocator::MMConfig mmConfig =
        makeMMConfig<typename Allocator::MMConfig>(config_);
    const PoolId pid = cache_->addPool(
        folly::sformat("pool_{}", i), poolSize, {} /* allocSizes */, mmConfig,
        nullptr /* rebalanceStrategy */, nullptr /* resizeStrategy */,
        true /* ensureSufficientMem */);
    pools_.push_back(pid);

#ifdef CACHEBENCH_FB_ENV
    CacheAdmin::Config adminConfig;
    adminConfig.cacheName = "cachebench";

    // These tell us how the cache is doing. Upload them to make evaluating
    // workloads easier.
    adminConfig.serviceDataStatsInterval = std::chrono::seconds{30};
    adminConfig.poolsStatsInterval = std::chrono::seconds{30};
    adminConfig.poolRebalancerStatsInterval = std::chrono::seconds{30};
    adminConfig.acStatsInterval = std::chrono::seconds{30};

    // Following stats are for production services so we don't need them.
    adminConfig.allocatorConfigInterval = std::chrono::seconds{0};
    adminConfig.itemStatsInterval = std::chrono::seconds{0};
    adminConfig.globalOdsInterval = std::chrono::seconds{0};
    admin_ = std::make_unique<CacheAdmin>(*cache_, adminConfig);

    // Log working set traces for in-depth post-run analysis
    admin_->enableWorkingSetAnalysis(*cache_, adminConfig);
#endif
  }

  cleanupGuard.dismiss();
}

template <typename Allocator>
Cache<Allocator>::~Cache() {
  try {
#ifdef CACHEBENCH_FB_ENV
    admin_.reset();
#endif

    // Reset cache first which will drain all nvm operations if present
    cache_.reset();

    if (!config_.dipperBackend.empty() && shouldCleanupFiles_) {
      boost::filesystem::remove_all(config_.dipperFilePath);
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
    aggregate += cache_->getPoolStats(pid);
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

  ret.slabsReleased = rebalanceStats.numSlabReleaseForRebalance;
  ret.moveAttemptsForSlabRelease = rebalanceStats.numMoveAttempts;
  ret.moveSuccessesForSlabRelease = rebalanceStats.numMoveSuccesses;
  ret.evictionAttemptsForSlabRelease = rebalanceStats.numEvictionAttempts;
  ret.evictionSuccessesForSlabRelease = rebalanceStats.numEvictionSuccesses;

  ret.inconsistencyCount = getInconsistencyCount();
  ret.isNvmCacheDisabled = isNvmCacheDisabled();

  // nvm stats from navy
  if (config_.dipperBackend == "navy_dipper" && !navyStats.empty()) {
    auto lookup = [&navyStats](const std::string& key) {
      return navyStats.find(key) != navyStats.end() ? navyStats.at(key) : 0;
    };
    ret.numNvmItems = lookup("navy_bh_items") + lookup("navy_bc_items");
    ret.numNvmBytesWritten = lookup("navy_device_bytes_written");
    uint64_t now = fetchNandWrites();
    if (now > nandBytesBegin_) {
      ret.numNvmNandBytesWritten = now - nandBytesBegin_;
    }
    double bhLogicalBytes = lookup("navy_bh_logical_written");
    double bcLogicalBytes = lookup("navy_bc_logical_written");
    ret.numNvmLogicalBytesWritten = bhLogicalBytes + bcLogicalBytes;
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
  }

  return ret;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
