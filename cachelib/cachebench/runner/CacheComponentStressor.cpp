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

#include "cachelib/cachebench/runner/CacheComponentStressor.h"

#include <folly/Random.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/logging/xlog.h>

#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/interface/components/RAMCacheComponent.h"

using namespace facebook::cachelib::interface;

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace {
void setupConfig(LruAllocatorConfig& allocatorConfig,
                 RAMCacheComponent::PoolConfig& poolConfig,
                 const CacheConfig& config) {
  constexpr size_t MB = 1024ULL * 1024ULL;

  allocatorConfig.enablePoolRebalancing(
      config.getRebalanceStrategy(),
      std::chrono::seconds(config.poolRebalanceIntervalSec));

  if (config.allocSizes.empty()) {
    XDCHECK(config.minAllocSize >= sizeof(CacheValue));
    allocatorConfig.setDefaultAllocSizes(util::generateAllocSizes(
        config.allocFactor, config.maxAllocSize, config.minAllocSize, true));
  } else {
    std::set<uint32_t> allocSizes;
    for (uint64_t s : config.allocSizes) {
      XDCHECK(s >= sizeof(CacheValue));
      allocSizes.insert(s);
    }
    allocatorConfig.setDefaultAllocSizes(std::move(allocSizes));
  }

  // Set hash table config
  allocatorConfig.setAccessConfig(typename LruAllocator::AccessConfig{
      static_cast<uint32_t>(config.htBucketPower),
      static_cast<uint32_t>(config.htLockPower)});

  allocatorConfig.configureChainedItems(typename LruAllocator::AccessConfig{
      static_cast<uint32_t>(config.chainedItemHtBucketPower),
      static_cast<uint32_t>(config.chainedItemHtLockPower)});

  allocatorConfig.setCacheSize(config.cacheSizeMB * (MB));

  if (!config.cacheDir.empty()) {
    allocatorConfig.cacheDir = config.cacheDir;
  }

  if (config.usePosixShm) {
    allocatorConfig.usePosixForShm();
  }

  allocatorConfig.setMemoryLocking(config.lockMemory);

  if (!config.memoryTierConfigs.empty()) {
    allocatorConfig.configureMemoryTiers(config.memoryTierConfigs);
  }

  if (config.enableItemDestructor) {
    auto removeCB = [&](const typename LruAllocator::DestructorData&) {};
    allocatorConfig.setItemDestructor(removeCB);
  }

  allocatorConfig.cacheName = kCachebenchCacheName;

  // Set up pool config
  poolConfig.name_ = "pool_1";
  poolConfig.size_ = (config.cacheSizeMB * MB) - (4 * MB);
  poolConfig.mmConfig_ = makeMMConfig<typename LruAllocator::MMConfig>(config);
  poolConfig.ensureProvisionable_ = true;
}
} // namespace

CacheComponentStressor::CacheComponentStressor(
    const CacheConfig& cacheConfig,
    StressorConfig config,
    std::unique_ptr<GeneratorBase>&& generator)
    : CacheStressorBase(std::move(config), std::move(generator)) {
  validate(cacheConfig);

  LruAllocatorConfig lruConfig;
  RAMCacheComponent::PoolConfig poolConfig;
  setupConfig(lruConfig, poolConfig, cacheConfig);

  auto cache =
      RAMCacheComponent::create(std::move(lruConfig), std::move(poolConfig));
  XCHECK(cache.hasValue()) << cache.error();
  cache_ = std::make_unique<RAMCacheComponent>(std::move(cache).value());
}

void CacheComponentStressor::start() {
  setStartTime();
  std::cout << fmt::format("Total {:.2f}M ops to be run",
                           config_.numThreads * config_.numOps / 1e6)
            << std::endl;

  stressWorker_ = std::thread([this] {
    folly::CPUThreadPoolExecutor workers(
        config_.numThreads,
        std::make_shared<folly::NamedThreadFactory>(
            "cache_component_stressor_"));

    for (uint64_t i = 0; i < config_.numThreads; ++i) {
      folly::coro::co_withExecutor(&workers,
                                   stressCoroutine(throughputStats_.at(i)))
          .start();
    }
    workers.join();

    setEndTime();
  });
}

// TODO use CacheStressor::getCacheStats()
Stats CacheComponentStressor::getCacheStats() const {
  auto& cache = reinterpret_cast<RAMCacheComponent*>(cache_.get())->get();
  Stats ret;

  const auto cacheStats = cache.getGlobalCacheStats();
  auto poolId = *cache.getPoolIds().begin();
  PoolStats aggregate = cache.getPoolStats(poolId);
  auto usageFraction =
      1.0 - (static_cast<double>(aggregate.freeMemoryBytes())) /
                aggregate.poolUsableSize;
  ret.poolUsageFraction.push_back(usageFraction);

  std::map<PoolId, std::map<ClassId, ACStats>> allocationClassStats{};

  auto poolStats = cache.getPoolStats(poolId);
  for (auto [cid, stats] : poolStats.mpStats.acStats) {
    allocationClassStats[poolId][cid] = stats;
  }

  const auto rebalanceStats = cache.getSlabReleaseStats();

  ret.allocationClassStats = allocationClassStats;
  ret.numEvictions = aggregate.numEvictions();
  ret.numItems = aggregate.numItems();
  ret.evictAttempts = cacheStats.evictionAttempts;
  ret.allocAttempts = cacheStats.allocAttempts;
  ret.allocFailures = cacheStats.allocFailures;

  ret.backgndEvicStats.nEvictedItems = cacheStats.evictionStats.numMovedItems;
  ret.backgndEvicStats.nTraversals = cacheStats.evictionStats.runCount;
  ret.backgndEvicStats.nClasses = cacheStats.evictionStats.totalClasses;
  ret.backgndEvicStats.evictionSize = cacheStats.evictionStats.totalBytesMoved;

  ret.backgndPromoStats.nPromotedItems =
      cacheStats.promotionStats.numMovedItems;
  ret.backgndPromoStats.nTraversals = cacheStats.promotionStats.runCount;

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

  ret.isNvmCacheDisabled = true;

  ret.cacheAllocateLatencyNs = cacheStats.allocateLatencyNs;

  return ret;
}

void CacheComponentStressor::validate(const CacheConfig& cacheConfig) const {
  if (config_.usesChainedItems()) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support chained items");
  } else if (cacheConfig.useTraceTimeStamp) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support trace timestamps");
  } else if (cacheConfig.nvmCacheSizeMB > 0) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support NVM caching");
  } else if (cacheConfig.numPools > 1 ||
             config_.opPoolDistribution.size() != 1 ||
             config_.keyPoolDistribution.size() != 1) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support multiple pools");
  } else if (config_.opRatePerSec > 0 || config_.opDelayBatch > 0 ||
             config_.opDelayNs > 0) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support rate limiting");
  } else if (config_.checkConsistency) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support consistency checking");
  } else if (cacheConfig.enableItemDestructorCheck) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support item destructor checking");
  }
}

folly::coro::Task<void> CacheComponentStressor::stressCoroutine(
    ThroughputStats& stats) {
  std::mt19937_64 gen(folly::Random::rand64());
  std::discrete_distribution<> opPoolDist(config_.opPoolDistribution.begin(),
                                          config_.opPoolDistribution.end());
  std::optional<uint64_t> lastRequestId = std::nullopt;

  for (uint64_t i = 0; i < config_.numOps && !shouldTestStop(); ++i) {
    try {
      ++stats.ops;

      const auto pid = static_cast<PoolId>(opPoolDist(gen));
      const auto& req = wg_->getReq(pid, gen, lastRequestId);
      OpType op = req.getOp();
      std::string_view key = req.key;
      std::string oneHitKey;
      if (op == OpType::kLoneGet || op == OpType::kLoneSet) {
        oneHitKey = Request::getUniqueKey();
        key = oneHitKey;
      }

      OpResultType result = co_await executeOperation(stats, req, gen);

      lastRequestId = req.requestId;
      if (req.requestId) {
        wg_->notifyResult(*req.requestId, result);
      }
    } catch (const cachebench::EndOfTrace&) {
      break;
    }
  }

  wg_->markFinish();
}

folly::coro::Task<OpResultType> CacheComponentStressor::executeOperation(
    ThroughputStats& stats, const Request& req, std::mt19937_64& gen) {
  OpType op = req.getOp();
  std::string_view key = req.key;
  std::string oneHitKey;
  if (op == OpType::kLoneGet || op == OpType::kLoneSet) {
    oneHitKey = Request::getUniqueKey();
    key = oneHitKey;
  }

  switch (op) {
  case OpType::kLoneSet:
  case OpType::kSet: {
    if (config_.onlySetIfMiss) {
      auto findResult = co_await cache_->find(Key{key});
      if (findResult.hasValue() && findResult.value().has_value()) {
        co_return OpResultType::kNop;
      }
    }
    co_return co_await setKey(stats, key, *(req.sizeBegin), req.ttlSecs,
                              req.admFeatureMap, req.itemValue);
  }

  case OpType::kLoneGet:
  case OpType::kGet: {
    co_return co_await getKey(stats, key, req);
  }

  case OpType::kDel: {
    co_return co_await deleteKey(stats, key);
  }

  case OpType::kAddChained: // not supported, fall through
  case OpType::kUpdate:
  case OpType::kCouldExist:
  default:
    throw std::runtime_error(
        folly::sformat("invalid operation generated: {}", (int)op));
  }
}

folly::coro::Task<OpResultType> CacheComponentStressor::setKey(
    ThroughputStats& stats,
    const std::string_view key,
    size_t size,
    uint32_t ttlSecs,
    const std::unordered_map<std::string, std::string>& featureMap,
    const std::string& itemValue) {
  // Check admission policy if present
  if (config_.admPolicy && !config_.admPolicy->accept(featureMap)) {
    co_return OpResultType::kSetSkip;
  }

  ++stats.set;

  // Get current time for creation timestamp
  uint32_t creationTime = static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  auto allocResult =
      co_await cache_->allocate(Key{key}, size, creationTime, ttlSecs);
  if (!allocResult.hasValue()) {
    ++stats.setFailure;
    co_return OpResultType::kSetFailure;
  }

  auto handle = std::move(allocResult.value());
  if (config_.populateItem) {
    populateItem(handle, itemValue);
  }

  auto insertResult = co_await cache_->insertOrReplace(std::move(handle));
  if (!insertResult.hasValue()) {
    ++stats.setFailure;
    co_return OpResultType::kSetFailure;
  }

  co_return OpResultType::kSetSuccess;
}

folly::coro::Task<OpResultType> CacheComponentStressor::getKey(
    ThroughputStats& stats, const std::string_view key, const Request& req) {
  ++stats.get;

  auto result = co_await cache_->find(Key{key});
  if (!result.hasValue()) { // Get error
    ++stats.getMiss;
    co_return OpResultType::kGetMiss;
  }

  if (!result.value().has_value()) {
    ++stats.getMiss;
    if (config_.enableLookaside) {
      // Allocate and insert on miss
      co_await setKey(stats, key, *(req.sizeBegin), req.ttlSecs,
                      req.admFeatureMap, req.itemValue);
    }
    co_return OpResultType::kGetMiss;
  }
  co_return OpResultType::kGetHit;
}

folly::coro::Task<OpResultType> CacheComponentStressor::deleteKey(
    ThroughputStats& stats, const std::string_view key) {
  ++stats.del;

  auto result = co_await cache_->remove(Key{key});
  if (!result.hasValue() || !result.value()) {
    ++stats.delNotFound;
  }

  co_return OpResultType::kNop;
}

void CacheComponentStressor::populateItem(AllocatedHandle& handle,
                                          const std::string& itemValue) {
  // For the generic interface, we'll just store the itemValue or hardcoded
  // string. NOTE: This is simplified compared to Cache<Allocator> which has
  // special methods for setting uint64 and string values with consistency
  // checking
  char* data = reinterpret_cast<char*>(handle->getMemory());
  uint32_t dataSize = handle->getMemorySize();

  // Copy the item value into the handle's memory
  auto& value = itemValue.empty() ? hardcodedString_ : itemValue;
  size_t copySize = std::min(value.size(), static_cast<size_t>(dataSize));
  XCHECK_GT(copySize, 0ULL);
  std::strncpy(data, value.c_str(), copySize);
  data[copySize - 1] = '\0';
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
