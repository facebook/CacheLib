/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
Cache<Allocator>::Cache(const CacheConfig& config,
                        ChainedItemMovingSync movingSync,
                        std::string cacheDir)
    : config_(config),
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
          std::memcpy(newItem.getWritableMemory(), oldItem.getMemory(),
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

  auto cleanupGuard = folly::makeGuard([&] {
    if (!nvmCacheFilePath_.empty()) {
      util::removePath(nvmCacheFilePath_);
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
    allocatorConfig_.setRemoveCallback(removeCB);
  } else if (config_.enableItemDestructor) {
    // TODO (zixuan) use ItemDestructor once feature is finished
    auto removeCB = [&](const typename Allocator::RemoveCbData&) {};
    allocatorConfig_.setRemoveCallback(removeCB);
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
      if (cachelib::util::isDir(path)) {
        const auto uniqueSuffix = folly::sformat("nvmcache_{}_{}", ::getpid(),
                                                 folly::Random::rand32());
        path = path + "/" + uniqueSuffix;
        util::makeDir(path);
        nvmCacheFilePath_ = path;
        nvmConfig.navyConfig.setSimpleFile(path + "/navy_cache",
                                           config_.nvmCacheSizeMB * MB,
                                           true /*truncateFile*/);
      } else {
        nvmConfig.navyConfig.setSimpleFile(path, config_.nvmCacheSizeMB * MB);
      }
    } else if (config_.nvmCachePaths.size() > 1) {
      // set up a software raid-0 across each nvm cache path.
      nvmConfig.navyConfig.setRaidFiles(config_.nvmCachePaths,
                                        config_.nvmCacheSizeMB * MB);
    } else {
      // use memory to mock NVM.
      nvmConfig.navyConfig.setMemoryFile(config_.nvmCacheSizeMB * MB);
    }

    if (config_.navyReqOrderShardsPower != 0) {
      nvmConfig.navyConfig.setNavyReqOrderingShards(
          config_.navyReqOrderShardsPower);
    }
    nvmConfig.navyConfig.setBlockSize(config_.navyBlockSize);

    // configure BlockCache
    auto& bcConfig = nvmConfig.navyConfig.blockCache()
                         .setDataChecksum(config_.navyDataChecksum)
                         .setCleanRegions(config_.navyCleanRegions,
                                          false /*enable in-mem buffer*/)
                         .setRegionSize(config_.navyRegionSizeMB * MB);

    // We have to use the old API to separately set the in-mem buffer
    // due to T102105644
    // TODO: cleanup the old API after we understand why 2*cleanRegions will
    // cause cogwheel tests issue
    if (config_.navyNumInmemBuffers > 0) {
      nvmConfig.navyConfig.setBlockCacheNumInMemBuffers(
          config_.navyNumInmemBuffers);
    }

    // by default lru. if more than one fifo ratio is present, we use
    // segmented fifo. otherwise, simple fifo.
    if (!config_.navySegmentedFifoSegmentRatio.empty()) {
      if (config.navySegmentedFifoSegmentRatio.size() == 1) {
        bcConfig.enableFifo();
      } else {
        bcConfig.enableSegmentedFifo(config_.navySegmentedFifoSegmentRatio);
      }
    }

    if (!config_.navySizeClasses.empty()) {
      bcConfig.useSizeClasses(config_.navySizeClasses);
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
    if (!config_.mlNvmAdmissionPolicy.empty() &&
        config_.nvmAdmissionPolicyFactory) {
      try {
        nvmAdmissionPolicy_ =
            std::any_cast<std::shared_ptr<NvmAdmissionPolicy<Allocator>>>(
                config_.nvmAdmissionPolicyFactory(config_));
        allocatorConfig_.setNvmCacheAdmissionPolicy(nvmAdmissionPolicy_);
      } catch (const std::bad_any_cast& e) {
        XLOG(ERR) << "CAST ERROR " << e.what();
        throw;
      }
    }

    allocatorConfig_.setNvmAdmissionMinTTL(config_.memoryOnlyTTL);
  }

  allocatorConfig_.cacheName = "cachebench";

  if (!cacheDir.empty()) {
    allocatorConfig_.cacheDir = cacheDir;
    cache_ =
        std::make_unique<Allocator>(Allocator::SharedMemNew, allocatorConfig_);
  } else {
    cache_ = std::make_unique<Allocator>(allocatorConfig_);
  }

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
    const ItemHandle& it) {
  if (!consistencyCheckEnabled()) {
    return cache_->remove(it);
  }

  auto opId = valueTracker_->beginDelete(it->getKey());
  auto rv = cache_->remove(it->getKey());
  valueTracker_->endDelete(opId);
  return rv;
}

template <typename Allocator>
typename Cache<Allocator>::ItemHandle Cache<Allocator>::allocateChainedItem(
    const ItemHandle& parent, size_t size) {
  auto handle = cache_->allocateChainedItem(parent, CacheValue::getSize(size));
  if (handle) {
    CacheValue::initialize(handle->getWritableMemory());
  }
  return handle;
}

template <typename Allocator>
void Cache<Allocator>::addChainedItem(const ItemHandle& parent,
                                      ItemHandle child) {
  itemRecords_.updateItemVersion(*parent);
  cache_->addChainedItem(parent, std::move(child));
}

template <typename Allocator>
typename Cache<Allocator>::ItemHandle Cache<Allocator>::replaceChainedItem(
    Item& oldItem, ItemHandle newItemHandle, Item& parent) {
  itemRecords_.updateItemVersion(parent);
  return cache_->replaceChainedItem(oldItem, std::move(newItemHandle), parent);
}

template <typename Allocator>
bool Cache<Allocator>::insert(const ItemHandle& handle) {
  // Insert is not supported in consistency checking mode because consistency
  // checking assumes a Set always succeeds and overrides existing value.
  XDCHECK(!consistencyCheckEnabled());
  itemRecords_.addItemRecord(handle);
  return cache_->insert(handle);
}

template <typename Allocator>
typename Cache<Allocator>::ItemHandle Cache<Allocator>::allocate(
    PoolId pid, folly::StringPiece key, size_t size, uint32_t ttlSecs) {
  ItemHandle handle;
  try {
    handle = cache_->allocate(pid, key, CacheValue::getSize(size), ttlSecs);
    if (handle) {
      CacheValue::initialize(handle->getWritableMemory());
    }
  } catch (const std::invalid_argument& e) {
    XLOGF(DBG, "Unable to allocate, reason: {}", e.what());
  }

  return handle;
}
template <typename Allocator>
typename Cache<Allocator>::ItemHandle Cache<Allocator>::insertOrReplace(
    const ItemHandle& handle) {
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
typename Cache<Allocator>::ItemHandle Cache<Allocator>::find(Key key,
                                                             AccessMode mode) {
  auto findFn = [&]() {
    util::LatencyTracker tracker;
    if (FLAGS_report_api_latency) {
      tracker = util::LatencyTracker(cacheFindLatency_);
    }
    // find from cache and wait for the result to be ready.
    auto it = cache_->find(key, mode);
    it.wait();
    return it;
  };

  if (!consistencyCheckEnabled()) {
    return findFn();
  }

  auto opId = valueTracker_->beginGet(key);
  auto it = findFn();
  if (checkGet(opId, it)) {
    invalidKeys_[key.str()].store(true, std::memory_order_acquire);
  }
  return it;
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
  ret.numAbortedSlabReleases = cacheStats.numAbortedSlabReleases;
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

template <typename Allocator>
uint32_t Cache<Allocator>::getSize(const Item* item) const noexcept {
  if (item == nullptr) {
    return 0;
  }
  return item->template getMemoryAs<CacheValue>()->getDataSize(item->getSize());
}

template <typename Allocator>
uint64_t Cache<Allocator>::genHashForChain(const ItemHandle& handle) const {
  auto chainedAllocs = cache_->viewAsChainedAllocs(handle);
  uint64_t hash = getUint64FromItem(*handle);
  for (const auto& item : chainedAllocs.getChain()) {
    hash = folly::hash::hash_128_to_64(hash, getUint64FromItem(item));
  }
  return hash;
}

template <typename Allocator>
void Cache<Allocator>::trackChainChecksum(const ItemHandle& handle) {
  XDCHECK(consistencyCheckEnabled());
  auto checksum = genHashForChain(handle);
  auto opId = valueTracker_->beginSet(handle->getKey(), checksum);
  valueTracker_->endSet(opId);
}

template <typename Allocator>
void Cache<Allocator>::setUint64ToItem(ItemHandle& handle, uint64_t num) const {
  XDCHECK(handle);
  auto ptr = handle->template getWritableMemoryAs<CacheValue>();
  ptr->setConsistencyNum(num);
}

template <typename Allocator>
void Cache<Allocator>::setStringItem(ItemHandle& handle,
                                     const std::string& str) const {
  auto ptr = reinterpret_cast<uint8_t*>(getWritableMemory(handle));
  std::memcpy(ptr, str.data(), std::min<size_t>(str.size(), getSize(handle)));
}

template <typename Allocator>
void Cache<Allocator>::updateItemRecordVersion(ItemHandle& it) {
  itemRecords_.updateItemVersion(*it);
  cache_->invalidateNvm(*it);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
