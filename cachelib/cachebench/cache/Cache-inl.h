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
Cache<Allocator>::Cache(const CacheConfig& config,
                        ChainedItemMovingSync movingSync,
                        std::string cacheDir,
                        bool touchValue)
    : config_(config),
      touchValue_(touchValue),
      nandBytesBegin_{fetchNandWrites()},
      itemRecords_(config_.enableItemDestructorCheck) {
  constexpr size_t MB = 1024ULL * 1024ULL;

  allocatorConfig_.enablePoolRebalancing(
      config_.getRebalanceStrategy(),
      std::chrono::seconds(config_.poolRebalanceIntervalSec));

  if (config_.moveOnSlabRelease && movingSync != nullptr) {
    allocatorConfig_.enableMovingOnSlabRelease(
        [](Item& oldItem, Item& newItem, Item* parentPtr) {
          XDCHECK(oldItem.isChainedItem() == (parentPtr != nullptr));
          std::memcpy(newItem.getMemory(), oldItem.getMemory(),
                      oldItem.getSize());
        },
        movingSync);
  }

  if (config_.allocSizes.empty()) {
    XDCHECK(config_.minAllocSize >= sizeof(CacheValue));
    allocatorConfig_.setDefaultAllocSizes(util::generateAllocSizes(
        config_.allocFactor, config_.maxAllocSize, config_.minAllocSize, true));
  } else {
    std::set<uint32_t> allocSizes;
    for (uint64_t s : config_.allocSizes) {
      XDCHECK(s >= sizeof(CacheValue));
      allocSizes.insert(s);
    }
    allocatorConfig_.setDefaultAllocSizes(std::move(allocSizes));
  }

  // Set hash table config
  allocatorConfig_.setAccessConfig(typename Allocator::AccessConfig{
      static_cast<uint32_t>(config_.htBucketPower),
      static_cast<uint32_t>(config_.htLockPower)});

  allocatorConfig_.configureChainedItems(typename Allocator::AccessConfig{
      static_cast<uint32_t>(config_.chainedItemHtBucketPower),
      static_cast<uint32_t>(config_.chainedItemHtLockPower)});

  allocatorConfig_.setCacheSize(config_.cacheSizeMB * (MB));

  if (!cacheDir.empty()) {
    allocatorConfig_.cacheDir = cacheDir;
  }

  if (config_.usePosixShm) {
    allocatorConfig_.usePosixForShm();
  }

  if (!config_.memoryTierConfigs.empty()) {
    allocatorConfig_.configureMemoryTiers(config_.memoryTierConfigs);
  }

  auto cleanupGuard = folly::makeGuard([&] {
    if (!nvmCacheFilePath_.empty()) {
      util::removePath(nvmCacheFilePath_);
    }
  });

  if (config_.enableItemDestructorCheck) {
    auto removeCB = [&](const typename Allocator::DestructorData& data) {
      if (!itemRecords_.validate(data)) {
        ++invalidDestructor_;
      }
      // in the end of test, total destructor invocations must be equal to total
      // size of itemRecords_ (also is the number of new allocations)
      ++totalDestructor_;
    };
    allocatorConfig_.setItemDestructor(removeCB);
  } else if (config_.enableItemDestructor) {
    auto removeCB = [&](const typename Allocator::DestructorData&) {};
    allocatorConfig_.setItemDestructor(removeCB);
  }

  // Set up Navy
  if (!isRamOnly()) {
    typename Allocator::NvmCacheConfig nvmConfig;

    nvmConfig.enableFastNegativeLookups = true;

    if (config_.nvmCachePaths.size() == 1) {
      // if we get a directory, create a file. we will clean it up. If we
      // already have a file, user provided it. We will also keep it around
      // after the tests.
      auto path = config_.nvmCachePaths[0];
      bool isDir;
      bool isBlk = false;
      try {
        isDir = cachelib::util::isDir(path);
        if (!isDir) {
          isBlk = cachelib::util::isBlk(path);
        }
      } catch (const std::system_error& e) {
        XLOGF(INFO, "nvmCachePath {} does not exist", path);
        isDir = false;
      }

      if (isDir) {
        const auto uniqueSuffix = folly::sformat("nvmcache_{}_{}", ::getpid(),
                                                 folly::Random::rand32());
        path = path + "/" + uniqueSuffix;
        util::makeDir(path);
        nvmCacheFilePath_ = path;
        XLOGF(INFO, "Configuring NVM cache: directory {} size {} MB", path,
              config_.nvmCacheSizeMB);
        nvmConfig.navyConfig.setSimpleFile(path + "/navy_cache",
                                           config_.nvmCacheSizeMB * MB,
                                           true /*truncateFile*/);
      } else {
        XLOGF(INFO, "Configuring NVM cache: simple file {} size {} MB", path,
              config_.nvmCacheSizeMB);
        nvmConfig.navyConfig.setSimpleFile(path, config_.nvmCacheSizeMB * MB,
                                           !isBlk /* truncateFile */);
      }
    } else if (config_.nvmCachePaths.size() > 1) {
      XLOGF(INFO, "Configuring NVM cache: RAID-0 ({} devices) size {} MB",
            config_.nvmCachePaths.size(), config_.nvmCacheSizeMB);
      // set up a software raid-0 across each nvm cache path.
      nvmConfig.navyConfig.setRaidFiles(config_.nvmCachePaths,
                                        config_.nvmCacheSizeMB * MB);
    } else {
      // use memory to mock NVM.
      XLOGF(INFO, "Configuring NVM cache: memory file size {} MB",
            config_.nvmCacheSizeMB);
      nvmConfig.navyConfig.setMemoryFile(config_.nvmCacheSizeMB * MB);
    }
    nvmConfig.navyConfig.setDeviceMetadataSize(config_.nvmCacheMetadataSizeMB *
                                               MB);

    if (config_.navyReqOrderShardsPower != 0) {
      nvmConfig.navyConfig.setNavyReqOrderingShards(
          config_.navyReqOrderShardsPower);
    }
    nvmConfig.navyConfig.setBlockSize(config_.navyBlockSize);

    // configure BlockCache
    auto& bcConfig = nvmConfig.navyConfig.blockCache()
                         .setDataChecksum(config_.navyDataChecksum)
                         .setCleanRegions(config_.navyCleanRegions)
                         .setRegionSize(config_.navyRegionSizeMB * MB);

    // by default lru. if more than one fifo ratio is present, we use
    // segmented fifo. otherwise, simple fifo.
    if (!config_.navySegmentedFifoSegmentRatio.empty()) {
      if (config.navySegmentedFifoSegmentRatio.size() == 1) {
        bcConfig.enableFifo();
      } else {
        bcConfig.enableSegmentedFifo(config_.navySegmentedFifoSegmentRatio);
      }
    }

    if (config_.navyHitsReinsertionThreshold > 0) {
      bcConfig.enableHitsBasedReinsertion(
          static_cast<uint8_t>(config_.navyHitsReinsertionThreshold));
    }
    if (config_.navyProbabilityReinsertionThreshold > 0) {
      bcConfig.enablePctBasedReinsertion(
          config_.navyProbabilityReinsertionThreshold);
    }

    // configure BigHash if enabled
    if (config_.navyBigHashSizePct > 0) {
      nvmConfig.navyConfig.bigHash()
          .setSizePctAndMaxItemSize(config_.navyBigHashSizePct,
                                    config_.navySmallItemMaxSize)
          .setBucketSize(config_.navyBigHashBucketSize)
          .setBucketBfSize(config_.navyBloomFilterPerBucketSize);
    }

    nvmConfig.navyConfig.setMaxParcelMemoryMB(config_.navyParcelMemoryMB);

    nvmConfig.navyConfig.setReaderAndWriterThreads(config_.navyReaderThreads,
                                                   config_.navyWriterThreads);

    if (config_.navyAdmissionWriteRateMB > 0) {
      nvmConfig.navyConfig.enableDynamicRandomAdmPolicy().setAdmWriteRate(
          config_.navyAdmissionWriteRateMB * MB);
    }
    nvmConfig.navyConfig.setMaxConcurrentInserts(
        config_.navyMaxConcurrentInserts);

    nvmConfig.truncateItemToOriginalAllocSizeInNvm =
        config_.truncateItemToOriginalAllocSizeInNvm;

    nvmConfig.navyConfig.setDeviceMaxWriteSize(config_.deviceMaxWriteSize);

    XLOG(INFO) << "Using the following nvm config"
               << folly::toPrettyJson(
                      folly::toDynamic(nvmConfig.navyConfig.serialize()));
    allocatorConfig_.enableNvmCache(nvmConfig);

    if (config_.navyEncryption && config_.createEncryptor) {
      allocatorConfig_.enableNvmCacheEncryption(config_.createEncryptor());
    }
    if (config_.nvmAdmissionPolicyFactory) {
      try {
        nvmAdmissionPolicy_ =
            std::any_cast<std::shared_ptr<NvmAdmissionPolicy<Allocator>>>(
                config_.nvmAdmissionPolicyFactory(config_));
        allocatorConfig_.setNvmCacheAdmissionPolicy(nvmAdmissionPolicy_);
      } catch (const std::bad_any_cast& e) {
        XLOG(ERR) << "CAST ERROR " << e.what();
        throw;
      }
    } else if (config_.nvmAdmissionRetentionTimeThreshold > 0) {
      nvmAdmissionPolicy_ = std::make_shared<RetentionAP<Allocator>>(
          config_.nvmAdmissionRetentionTimeThreshold);
      allocatorConfig_.setNvmCacheAdmissionPolicy(nvmAdmissionPolicy_);
    }

    allocatorConfig_.setNvmAdmissionMinTTL(config_.memoryOnlyTTL);
  }

  allocatorConfig_.cacheName = "cachebench";

  bool isRecovered = false;
  if (!allocatorConfig_.cacheDir.empty()) {
    try {
      cache_ = std::make_unique<Allocator>(Allocator::SharedMemAttach,
                                           allocatorConfig_);
      XLOG(INFO, folly::sformat(
                     "Successfully attached to existing cache. Cache dir: {}",
                     cacheDir));
      isRecovered = true;
    } catch (const std::exception& ex) {
      XLOG(INFO, folly::sformat("Failed to attach for reason: {}", ex.what()));
      cache_ = std::make_unique<Allocator>(Allocator::SharedMemNew,
                                           allocatorConfig_);
    }
  } else {
    cache_ = std::make_unique<Allocator>(allocatorConfig_);
  }

  if (isRecovered) {
    auto poolIds = cache_->getPoolIds();
    for (auto poolId : poolIds) {
      pools_.push_back(poolId);
    }
  } else {
    const size_t numBytes = cache_->getCacheMemoryStats().ramCacheSize;
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

    auto res = cache_->shutDown();
    if (res == Allocator::ShutDownStatus::kSuccess) {
      XLOG(INFO, folly::sformat("Shut down succeeded. Metadata is at: {}",
                                allocatorConfig_.cacheDir));
    } else {
      XLOG(INFO, folly::sformat(
                     "Shut down failed. Metadata is at: {}. Return code: {}",
                     allocatorConfig_.cacheDir, static_cast<int>(res)));
    }

    // Reset cache first which will drain all nvm operations if present
    cache_.reset();

    if (!nvmCacheFilePath_.empty()) {
      util::removePath(nvmCacheFilePath_);
    }
  } catch (...) {
  }
}

template <typename Allocator>
void Cache<Allocator>::reAttach() {
  cache_ =
      std::make_unique<Allocator>(Allocator::SharedMemAttach, allocatorConfig_);
}

template <typename Allocator>
void Cache<Allocator>::shutDown() {
  monitor_.reset();
  cache_->shutDown();
}

template <typename Allocator>
void Cache<Allocator>::cleanupSharedMem() {
  if (!allocatorConfig_.cacheDir.empty()) {
    cache_->cleanupStrayShmSegments(allocatorConfig_.cacheDir,
                                    allocatorConfig_.usePosixShm);
    util::removePath(allocatorConfig_.cacheDir);
  }
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
typename Cache<Allocator>::RemoveRes Cache<Allocator>::remove(Key key) {
  if (!consistencyCheckEnabled()) {
    return cache_->remove(key);
  }

  auto opId = valueTracker_->beginDelete(key);
  auto rv = cache_->remove(key);
  valueTracker_->endDelete(opId);
  return rv;
}

template <typename Allocator>
typename Cache<Allocator>::RemoveRes Cache<Allocator>::remove(
    const ReadHandle& it) {
  if (!consistencyCheckEnabled()) {
    return cache_->remove(it);
  }

  auto opId = valueTracker_->beginDelete(it->getKey());
  auto rv = cache_->remove(it->getKey());
  valueTracker_->endDelete(opId);
  return rv;
}

template <typename Allocator>
typename Cache<Allocator>::WriteHandle Cache<Allocator>::allocateChainedItem(
    const ReadHandle& parent, size_t size) {
  auto handle = cache_->allocateChainedItem(parent, CacheValue::getSize(size));
  if (handle) {
    CacheValue::initialize(handle->getMemory());
  }
  return handle;
}

template <typename Allocator>
void Cache<Allocator>::addChainedItem(WriteHandle& parent, WriteHandle child) {
  itemRecords_.updateItemVersion(*parent);
  cache_->addChainedItem(parent, std::move(child));
}

template <typename Allocator>
typename Cache<Allocator>::WriteHandle Cache<Allocator>::replaceChainedItem(
    Item& oldItem, WriteHandle newItemHandle, Item& parent) {
  itemRecords_.updateItemVersion(parent);
  return cache_->replaceChainedItem(oldItem, std::move(newItemHandle), parent);
}

template <typename Allocator>
bool Cache<Allocator>::insert(WriteHandle& handle) {
  // Insert is not supported in consistency checking mode because consistency
  // checking assumes a Set always succeeds and overrides existing value.
  XDCHECK(!consistencyCheckEnabled());
  itemRecords_.addItemRecord(handle);
  return cache_->insert(handle);
}

template <typename Allocator>
typename Cache<Allocator>::WriteHandle Cache<Allocator>::allocate(
    PoolId pid, folly::StringPiece key, size_t size, uint32_t ttlSecs) {
  WriteHandle handle;
  try {
    handle = cache_->allocate(pid, key, CacheValue::getSize(size), ttlSecs);
    if (handle) {
      CacheValue::initialize(handle->getMemory());
    }
  } catch (const std::invalid_argument& e) {
    XLOGF(DBG, "Unable to allocate, reason: {}", e.what());
  }

  return handle;
}
template <typename Allocator>
typename Cache<Allocator>::WriteHandle Cache<Allocator>::insertOrReplace(
    WriteHandle& handle) {
  itemRecords_.addItemRecord(handle);

  if (!consistencyCheckEnabled()) {
    try {
      return cache_->insertOrReplace(handle);
    } catch (const cachelib::exception::RefcountOverflow& ex) {
      XLOGF(DBG, "overflow exception: {}", ex.what());
    }
  }

  auto checksum = getUint64FromItem(*handle);
  auto opId = valueTracker_->beginSet(handle->getKey(), checksum);
  auto rv = cache_->insertOrReplace(handle);
  valueTracker_->endSet(opId);
  return rv;
}

template <typename Allocator>
void Cache<Allocator>::touchValue(const ReadHandle& it) const {
  XDCHECK(touchValueEnabled());

  auto ptr = reinterpret_cast<const uint8_t*>(getMemory(it));

  /* The accumulate call is intended to access all bytes of the value
   * and nothing more. */
  auto sum = std::accumulate(ptr, ptr + getSize(it), 0ULL);
  folly::doNotOptimizeAway(sum);
}

template <typename Allocator>
bool Cache<Allocator>::couldExist(Key key) {
  if (!consistencyCheckEnabled()) {
    return cache_->couldExistFast(key);
  }

  // TODO: implement consistency checking.
  // For couldExist, we need a weaker version of consistnecy check. The
  // following are a list of conditions that wouldn't be valid with only
  // sequence of get operations.
  //  couldExist == true -> get == miss
  //  couldExist == false -> couldExist == true
  return cache_->couldExistFast(key);
}

template <typename Allocator>
typename Cache<Allocator>::ReadHandle Cache<Allocator>::find(Key key) {
  auto findFn = [&]() {
    util::LatencyTracker tracker;
    if (FLAGS_report_api_latency) {
      tracker = util::LatencyTracker(cacheFindLatency_);
    }
    // find from cache and wait for the result to be ready.
    auto it = cache_->find(key);
    it.wait();

    if (touchValueEnabled()) {
      touchValue(it);
    }

    return it;
  };

  if (!consistencyCheckEnabled()) {
    return findFn();
  }

  auto opId = valueTracker_->beginGet(key);
  auto it = findFn();
  if (checkGet(opId, it)) {
    invalidKeys_[key.str()].store(true, std::memory_order_relaxed);
  }
  return it;
}

template <typename Allocator>
folly::SemiFuture<typename Cache<Allocator>::ReadHandle>
Cache<Allocator>::asyncFind(Key key) {
  auto findFn = [&]() {
    util::LatencyTracker tracker;
    if (FLAGS_report_api_latency) {
      tracker = util::LatencyTracker(cacheFindLatency_);
    }
    // find from cache, don't wait for the result to be ready.
    auto it = cache_->find(key);

    if (it.isReady()) {
      if (touchValueEnabled()) {
        touchValue(it);
      }

      return std::move(it).toSemiFuture();
    }

    // if the handle is not ready, return a SemiFuture with deferValue for
    // touchValue
    return std::move(it).toSemiFuture().deferValue(
        [this, t = std::move(tracker)](auto handle) {
          if (touchValueEnabled()) {
            touchValue(handle);
          }
          return handle;
        });
  };

  if (!consistencyCheckEnabled()) {
    return findFn();
  }

  auto opId = valueTracker_->beginGet(key);
  auto sf = findFn();

  if (sf.isReady()) {
    if (checkGet(opId, sf.value())) {
      invalidKeys_[key.str()].store(true, std::memory_order_relaxed);
    }

    return sf;
  }

  // if the handle is not ready, return a SemiFuture with deferValue for
  // checking consistency
  return std::move(sf).deferValue(
      [this, opId = std::move(opId), key = std::move(key)](auto handle) {
        if (checkGet(opId, handle)) {
          invalidKeys_[key.str()].store(true, std::memory_order_relaxed);
        }

        return handle;
      });
}

template <typename Allocator>
typename Cache<Allocator>::WriteHandle Cache<Allocator>::findToWrite(Key key) {
  auto findToWriteFn = [&]() {
    util::LatencyTracker tracker;
    if (FLAGS_report_api_latency) {
      tracker = util::LatencyTracker(cacheFindLatency_);
    }
    // find from cache and wait for the result to be ready.
    auto it = cache_->findToWrite(key);
    it.wait();

    if (touchValueEnabled()) {
      touchValue(it);
    }

    return it;
  };

  if (!consistencyCheckEnabled()) {
    return findToWriteFn();
  }

  auto opId = valueTracker_->beginGet(key);
  auto it = findToWriteFn();
  if (checkGet(opId, it)) {
    invalidKeys_[key.str()].store(true, std::memory_order_relaxed);
  }
  return it;
}

template <typename Allocator>
bool Cache<Allocator>::checkGet(ValueTracker::Index opId,
                                const ReadHandle& it) {
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
    inconsistencyCount_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
  return false;
}

template <typename Allocator>
double Cache<Allocator>::getNvmBytesWritten() const {
  const auto statsMap = cache_->getNvmCacheStatsMap();
  const auto& ratesMap = statsMap.getRates();
  if (const auto& it = ratesMap.find("navy_device_bytes_written");
      it != ratesMap.end()) {
    return it->second;
  }
  XLOG(INFO) << "Bytes written not found";
  return 0;
}

template <typename Allocator>
Stats Cache<Allocator>::getStats() const {
  PoolStats aggregate = cache_->getPoolStats(pools_[0]);
  auto usageFraction =
      1.0 - (static_cast<double>(aggregate.freeMemoryBytes())) /
                aggregate.poolUsableSize;
  Stats ret;
  ret.poolUsageFraction.push_back(usageFraction);
  for (size_t pid = 1; pid < pools_.size(); pid++) {
    auto poolStats = cache_->getPoolStats(static_cast<PoolId>(pid));
    usageFraction = 1.0 - (static_cast<double>(poolStats.freeMemoryBytes())) /
                              poolStats.poolUsableSize;
    ret.poolUsageFraction.push_back(usageFraction);
    aggregate += poolStats;
  }

  const auto cacheStats = cache_->getGlobalCacheStats();
  const auto rebalanceStats = cache_->getSlabReleaseStats();
  const auto navyStats = cache_->getNvmCacheStatsMap().toMap();

  ret.numEvictions = aggregate.numEvictions();
  ret.numItems = aggregate.numItems();
  ret.evictAttempts = cacheStats.evictionAttempts;
  ret.allocAttempts = cacheStats.allocAttempts;
  ret.allocFailures = cacheStats.allocFailures;

  ret.numCacheGets = cacheStats.numCacheGets;
  ret.numCacheGetMiss = cacheStats.numCacheGetMiss;
  ret.numCacheEvictions = cacheStats.numCacheEvictions;
  ret.numRamDestructorCalls = cacheStats.numRamDestructorCalls;
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
  ret.numNvmDestructorCalls = cacheStats.numNvmDestructorCalls;
  ret.numNvmEvictions = cacheStats.numNvmEvictions;

  ret.numNvmDeletes = cacheStats.numNvmDeletes;
  ret.numNvmSkippedDeletes = cacheStats.numNvmSkippedDeletes;

  ret.slabsReleased = rebalanceStats.numSlabReleaseForRebalance;
  ret.numAbortedSlabReleases = cacheStats.numAbortedSlabReleases;
  ret.numReaperSkippedSlabs = cacheStats.numReaperSkippedSlabs;
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
    ret.nvmCounters = cache_->getNvmCacheStatsMap().toMap();
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
    ret.numNvmItemRemovedSetSize = lookup("items_tracked_for_destructor");

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
bool Cache<Allocator>::hasNvmCacheWarmedUp() const {
  const auto nvmStats = cache_->getNvmCacheStatsMap();
  const auto& ratesMap = nvmStats.getRates();
  const auto it = ratesMap.find("navy_bc_evictions");
  if (it == ratesMap.end()) {
    return false;
  }
  return it->second > 0;
}

template <typename Allocator>
void Cache<Allocator>::clearCache(uint64_t errorLimit) {
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
    itemRecords_.findUndestructedItem(std::cout, errorLimit);
  }
}

template <typename Allocator>
uint32_t Cache<Allocator>::getSize(const Item* item) const noexcept {
  if (item == nullptr) {
    return 0;
  }
  return item->template getMemoryAs<CacheValue>()->getDataSize(item->getSize());
}

template <typename Allocator>
uint64_t Cache<Allocator>::genHashForChain(const ReadHandle& handle) const {
  auto chainedAllocs = cache_->viewAsChainedAllocs(handle);
  uint64_t hash = getUint64FromItem(*handle);
  for (const auto& item : chainedAllocs.getChain()) {
    hash = folly::hash::hash_128_to_64(hash, getUint64FromItem(item));
  }
  return hash;
}

template <typename Allocator>
void Cache<Allocator>::trackChainChecksum(const ReadHandle& handle) {
  XDCHECK(consistencyCheckEnabled());
  auto checksum = genHashForChain(handle);
  auto opId = valueTracker_->beginSet(handle->getKey(), checksum);
  valueTracker_->endSet(opId);
}

template <typename Allocator>
void Cache<Allocator>::setUint64ToItem(WriteHandle& handle,
                                       uint64_t num) const {
  XDCHECK(handle);
  auto ptr = handle->template getMemoryAs<CacheValue>();
  ptr->setConsistencyNum(num);
}

template <typename Allocator>
void Cache<Allocator>::setStringItem(WriteHandle& handle,
                                     const std::string& str) {
  auto dataSize = getSize(handle);
  if (dataSize < 1)
    return;

  auto ptr = reinterpret_cast<char*>(getMemory(handle));
  std::strncpy(ptr, str.c_str(), dataSize);

  // Make sure the copied string ends with null char
  if (str.size() + 1 > dataSize) {
    ptr[dataSize - 1] = '\0';
  }
}

template <typename Allocator>
void Cache<Allocator>::updateItemRecordVersion(WriteHandle& it) {
  itemRecords_.updateItemVersion(*it);
  cache_->invalidateNvm(*it);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
