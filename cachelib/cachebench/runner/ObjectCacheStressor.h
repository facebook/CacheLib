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

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/TokenBucket.h>
#include <folly/system/ThreadName.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/cache/CacheStats.h"
#include "cachelib/cachebench/runner/CacheStressorBase.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/JSONConfig.h"
#include "cachelib/cachebench/util/Sleep.h"
#include "cachelib/object_cache/ObjectCache.h"

namespace facebook::cachelib::cachebench {
namespace detail {

struct ObjectCacheValue {
  explicit ObjectCacheValue(std::string payload) : data(std::move(payload)) {}

  void touch() const {
    const auto* ptr = reinterpret_cast<const unsigned char*>(data.data());
    auto sum = std::accumulate(ptr, ptr + data.size(), 0ULL);
    folly::doNotOptimizeAway(sum);
  }

  void update(std::mt19937_64& gen) {
    if (data.empty()) {
      return;
    }
    // Flip one random payload byte so kUpdate performs a real mutation.
    data[gen() % data.size()] ^= 0x1;
  }

  size_t objectSize() const { return sizeof(ObjectCacheValue) + data.size(); }

  std::string data;
};

struct ObjectCacheBenchConfig : public JSONConfig {
  explicit ObjectCacheBenchConfig(const folly::dynamic& configJson) {
    if (!configJson.isObject()) {
      return;
    }

    JSONSetVal(configJson, l1EntriesLimit);
    JSONSetVal(configJson, l1NumShards);
    JSONSetVal(configJson, maxKeySizeBytes);
    JSONSetVal(configJson, l1HashTablePower);
    JSONSetVal(configJson, l1LockPower);
    JSONSetVal(configJson, evictionSearchLimit);
    JSONSetVal(configJson, totalObjectSizeLimit);
    JSONSetVal(configJson, sizeControllerIntervalMs);
    JSONSetVal(configJson, objectSizeTrackingEnabled);
    JSONSetVal(configJson, shardName);
  }

  void validate() const {
    if (l1NumShards == 0) {
      throw std::invalid_argument(
          "ObjectCacheStressor requires l1NumShards > 0.");
    }
    if (maxKeySizeBytes == 0) {
      throw std::invalid_argument(
          "ObjectCacheStressor requires maxKeySizeBytes > 0.");
    }
    if ((totalObjectSizeLimit == 0) != (sizeControllerIntervalMs == 0)) {
      throw std::invalid_argument(
          "ObjectCacheStressor requires both totalObjectSizeLimit and "
          "sizeControllerIntervalMs for size-controller mode.");
    }
  }

  size_t l1EntriesLimit{0};
  size_t l1NumShards{1};
  uint32_t maxKeySizeBytes{255};
  uint32_t l1HashTablePower{0};
  uint32_t l1LockPower{0};
  uint32_t evictionSearchLimit{50};
  size_t totalObjectSizeLimit{0};
  uint32_t sizeControllerIntervalMs{0};
  bool objectSizeTrackingEnabled{false};
  std::string shardName;
};

inline std::string buildPayload(size_t size,
                                folly::StringPiece seed,
                                bool populateItem) {
  if (!populateItem || size == 0) {
    return std::string(size, '\0');
  }

  const auto source = seed.empty() ? folly::StringPiece{"cachebench"} : seed;
  std::string payload;
  payload.reserve(size);
  while (payload.size() < size) {
    const auto copySize = std::min(source.size(), size - payload.size());
    payload.append(source.data(), copySize);
  }
  return payload;
}

inline folly::StringPiece asStringPiece(const std::string& value) {
  return {value.data(), value.size()};
}

} // namespace detail

template <typename Allocator>
class ObjectCacheStressor : public CacheStressorBase {
 public:
  using ObjectCacheT = objcache2::ObjectCache<Allocator>;

  ObjectCacheStressor(CacheConfig cacheConfig,
                      StressorConfig config,
                      std::unique_ptr<GeneratorBase>&& generator)
      : CacheStressorBase(std::move(config), std::move(generator)),
        cacheConfig_(std::move(cacheConfig)),
        objectCacheConfig_{cacheConfig_.customConfigJson},
        objectSizeTrackingEnabled_{
            objectCacheConfig_.objectSizeTrackingEnabled ||
            objectCacheConfig_.totalObjectSizeLimit > 0},
        cache_{createObjectCache(validate(cacheConfig_))} {
    if (config_.opRatePerSec > 0) {
      rateLimiter_ = std::make_unique<folly::BasicTokenBucket<>>(
          config_.opRatePerSec, config_.opRateBurstSize > 0
                                    ? config_.opRateBurstSize
                                    : config_.opRatePerSec);
    }
  }

  void start() override {
    setStartTime();
    std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                config_.numThreads * config_.numOps / 1e6)
              << std::endl;

    stressWorker_ = std::thread([this] {
      std::vector<std::thread> workers;
      workers.reserve(config_.numThreads);

      for (uint64_t i = 0; i < config_.numThreads; ++i) {
        workers.emplace_back([this, throughputStats = &throughputStats_.at(i),
                              threadName = folly::sformat("objcache_{}", i)]() {
          folly::setThreadName(threadName);
          stressByDiscreteDistribution(*throughputStats);
        });
      }

      for (auto& worker : workers) {
        worker.join();
      }
      setEndTime();
    });
  }

  std::unique_ptr<StatsBase> getCacheStats() const override {
    auto retPtr = std::make_unique<Stats>();
    auto& ret = *retPtr;

    std::optional<PoolStats> aggregate;
    for (auto poolId : cache_->getPoolIds()) {
      auto poolStats = cache_->getPoolStats(poolId);
      const auto usageFraction =
          1.0 - static_cast<double>(poolStats.freeMemoryBytes()) /
                    poolStats.poolUsableSize;
      ret.poolUsageFraction.push_back(usageFraction);
      for (const auto& [cid, stats] : poolStats.mpStats.acStats) {
        ret.allocationClassStats[poolId][cid] = stats;
      }
      if (aggregate.has_value()) {
        *aggregate += poolStats;
      } else {
        aggregate = poolStats;
      }
    }

    const auto cacheStats = cache_->getGlobalCacheStats();
    const auto rebalanceStats = cache_->getSlabReleaseStats();

    if (aggregate.has_value()) {
      ret.numEvictions = aggregate->numEvictions();
    }
    ret.numItems = cache_->getNumEntries();
    ret.evictAttempts = cacheStats.evictionAttempts;
    ret.allocAttempts = cacheStats.allocAttempts;
    ret.allocFailures = cacheStats.allocFailures;

    ret.backgndEvicStats.nEvictedItems = cacheStats.evictionStats.numMovedItems;
    ret.backgndEvicStats.nTraversals = cacheStats.evictionStats.runCount;
    ret.backgndEvicStats.nClasses = cacheStats.evictionStats.totalClasses;
    ret.backgndEvicStats.evictionSize =
        cacheStats.evictionStats.totalBytesMoved;

    ret.backgndPromoStats.nPromotedItems =
        cacheStats.promotionStats.numMovedItems;
    ret.backgndPromoStats.nTraversals = cacheStats.promotionStats.runCount;

    ret.numCacheGets = cacheStats.numCacheGets;
    ret.numCacheGetMiss = cacheStats.numCacheGetMiss;
    ret.numCacheEvictions = cacheStats.numCacheEvictions;
    ret.numRamDestructorCalls = cacheStats.numRamDestructorCalls;

    ret.slabsReleased = rebalanceStats.numSlabReleaseForRebalance;
    ret.numAbortedSlabReleases = cacheStats.numAbortedSlabReleases;
    ret.numReaperSkippedSlabs = cacheStats.numReaperSkippedSlabs;
    ret.moveAttemptsForSlabRelease = rebalanceStats.numMoveAttempts;
    ret.moveSuccessesForSlabRelease = rebalanceStats.numMoveSuccesses;
    ret.evictionAttemptsForSlabRelease = rebalanceStats.numEvictionAttempts;
    ret.evictionSuccessesForSlabRelease = rebalanceStats.numEvictionSuccesses;

    ret.cacheAllocateLatencyNs = cacheStats.allocateLatencyNs;

    return retPtr;
  }

 private:
  const CacheConfig& validate(const CacheConfig& cacheConfig) const {
    objectCacheConfig_.validate();

    if (cacheConfig.numPools != 1) {
      throw std::invalid_argument(
          "ObjectCacheStressor supports one cachebench pool. Use "
          "customConfigJson.l1NumShards for internal sharding.");
    }
    if (config_.opPoolDistribution.size() != 1 ||
        config_.keyPoolDistribution.size() != 1) {
      throw std::invalid_argument(
          "ObjectCacheStressor supports one workload pool.");
    }
    if (config_.usesChainedItems()) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support chained items.");
    }
    for (const auto& dist : config_.poolDistributions) {
      if (dist.couldExistRatio > 0) {
        throw std::invalid_argument(
            "ObjectCacheStressor does not support couldExist operations.");
      }
    }
    if (config_.checkConsistency) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support consistency checking.");
    }
    if (config_.checkNvmCacheWarmUp) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support NVM warmup checks.");
    }
    if (cacheConfig.poolRebalanceIntervalSec > 0 ||
        cacheConfig.moveOnSlabRelease) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support pool rebalancing settings.");
    }
    if (!cacheConfig.cacheDir.empty()) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support cacheDir persistence.");
    }
    if (cacheConfig.nvmCacheSizeMB > 0 || !cacheConfig.nvmCachePaths.empty()) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support object-cache NVM yet.");
    }
    if (cacheConfig.useTraceTimeStamp) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support trace timestamps.");
    }
    if (cacheConfig.enableItemDestructorCheck) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support cachebench item destructor "
          "checks.");
    }
    if (cacheConfig.usePosixShm || cacheConfig.lockMemory ||
        !cacheConfig.memoryTierConfigs.empty()) {
      throw std::invalid_argument(
          "ObjectCacheStressor does not support custom allocator memory "
          "configuration yet.");
    }
    return cacheConfig;
  }

  std::unique_ptr<ObjectCacheT> createObjectCache(
      const CacheConfig& cacheConfig) const {
    typename ObjectCacheT::Config objectCacheConfig;
    objectCacheConfig.setCacheName(kCachebenchCacheName.str())
        .setItemDestructor([](objcache2::ObjectCacheDestructorData data) {
          data.deleteObject<detail::ObjectCacheValue>();
        })
        .setNumShards(objectCacheConfig_.l1NumShards)
        .setMaxKeySizeBytes(objectCacheConfig_.maxKeySizeBytes)
        .setAccessConfig(objectCacheConfig_.l1HashTablePower != 0
                             ? objectCacheConfig_.l1HashTablePower
                             : static_cast<uint32_t>(cacheConfig.htBucketPower),
                         objectCacheConfig_.l1LockPower != 0
                             ? objectCacheConfig_.l1LockPower
                             : static_cast<uint32_t>(cacheConfig.htLockPower))
        .setEvictionPolicyConfig(
            makeMMConfig<typename ObjectCacheT::EvictionPolicyConfig>(
                cacheConfig));

    const auto l1EntriesLimit = objectCacheConfig_.l1EntriesLimit != 0
                                    ? objectCacheConfig_.l1EntriesLimit
                                    : deriveL1EntriesLimit();
    objectCacheConfig.objectSizeTrackingEnabled =
        objectCacheConfig_.objectSizeTrackingEnabled;
    objectCacheConfig.setCacheCapacity(
        l1EntriesLimit, objectCacheConfig_.totalObjectSizeLimit,
        objectCacheConfig_.sizeControllerIntervalMs);

    objectCacheConfig.setShardName(objectCacheConfig_.shardName);
    objectCacheConfig.setEvictionSearchLimit(
        objectCacheConfig_.evictionSearchLimit);
    return ObjectCacheT::create(std::move(objectCacheConfig));
  }

  size_t deriveL1EntriesLimit() const {
    if (cacheConfig_.cacheSizeMB == 0) {
      throw std::invalid_argument(
          "ObjectCacheStressor requires cacheSizeMB > 0 or "
          "customConfigJson.l1EntriesLimit > 0.");
    }
    constexpr size_t kMB = 1024ULL * 1024;
    const auto cacheBytes = cacheConfig_.cacheSizeMB * kMB;
    const auto l1AllocSize =
        ObjectCacheT::getL1AllocSize(objectCacheConfig_.maxKeySizeBytes);
    return std::max<size_t>(1, cacheBytes / l1AllocSize);
  }

  void stressByDiscreteDistribution(ThroughputStats& stats) {
    std::mt19937_64 gen(folly::Random::rand64());
    std::discrete_distribution<> opPoolDist(config_.opPoolDistribution.begin(),
                                            config_.opPoolDistribution.end());
    const uint64_t opDelayBatch = config_.opDelayBatch;
    const uint64_t opDelayNs = config_.opDelayNs;
    const std::chrono::nanoseconds opDelay(opDelayNs);

    const bool needDelay = opDelayBatch != 0 && opDelayNs != 0;
    uint64_t opCounter = 0;
    auto throttleFn = [&] {
      if (needDelay && ++opCounter == opDelayBatch) {
        opCounter = 0;
        highPrecisionSleep(opDelay);
      }
      limitRate();
    };

    std::optional<uint64_t> lastRequestId = std::nullopt;
    for (uint64_t i = 0; i < config_.numOps && !shouldTestStop(); ++i) {
      try {
        SCOPE_EXIT { throttleFn(); };
        ++stats.ops;

        const auto pid = static_cast<PoolId>(opPoolDist(gen));
        const Request& req = wg_->getReq(pid, gen, lastRequestId);
        OpType op = req.getOp();
        folly::StringPiece key{req.key.data(), req.key.size()};
        std::string oneHitKey;
        if (op == OpType::kLoneGet || op == OpType::kLoneSet) {
          oneHitKey = Request::getUniqueKey();
          key = detail::asStringPiece(oneHitKey);
        }

        OpResultType result{OpResultType::kNop};
        switch (op) {
        case OpType::kLoneSet:
        case OpType::kSet:
          if (config_.onlySetIfMiss &&
              cache_->template find<detail::ObjectCacheValue>(key)) {
            continue;
          }
          result = setKey(stats, key, *req.sizeBegin, req.ttlSecs,
                          req.getAdmFeatureMap(), req.itemValue);
          break;
        case OpType::kLoneGet:
        case OpType::kGet:
          result = getKey(stats, key, *req.sizeBegin, req.ttlSecs,
                          req.getAdmFeatureMap(), req.itemValue);
          break;
        case OpType::kDel:
          result = deleteKey(stats, key);
          break;
        case OpType::kUpdate:
          result = updateKey(stats, key, gen);
          break;
        case OpType::kAddChained:
        case OpType::kCouldExist:
        case OpType::kSize:
        default:
          throw std::runtime_error(
              folly::sformat("invalid operation generated: {}", (int)op));
        }

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

  OpResultType setKey(
      ThroughputStats& stats,
      folly::StringPiece key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap,
      const std::string& itemValue) {
    if (config_.admPolicy && !config_.admPolicy->accept(featureMap)) {
      return OpResultType::kSetSkip;
    }

    ++stats.set;
    auto value =
        std::make_unique<detail::ObjectCacheValue>(detail::buildPayload(
            size,
            itemValue.empty() ? detail::asStringPiece(hardcodedString_)
                              : detail::asStringPiece(itemValue),
            config_.populateItem));
    const auto objectSize =
        objectSizeTrackingEnabled_ ? value->objectSize() : 0;
    auto [status, ptr, replaced] =
        cache_->template insertOrReplace<detail::ObjectCacheValue>(
            key, std::move(value), objectSize, ttlSecs);
    if (status != ObjectCacheT::AllocStatus::kSuccess || ptr == nullptr) {
      ++stats.setFailure;
      return OpResultType::kSetFailure;
    }
    return OpResultType::kSetSuccess;
  }

  OpResultType getKey(
      ThroughputStats& stats,
      folly::StringPiece key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap,
      const std::string& itemValue) {
    ++stats.get;
    auto found = cache_->template find<detail::ObjectCacheValue>(key);
    if (!found) {
      ++stats.getMiss;
      if (config_.enableLookaside) {
        setKey(stats, key, size, ttlSecs, featureMap, itemValue);
      }
      return OpResultType::kGetMiss;
    }

    if (config_.touchValue) {
      found->touch();
    }
    return OpResultType::kGetHit;
  }

  OpResultType updateKey(ThroughputStats& stats,
                         folly::StringPiece key,
                         std::mt19937_64& gen) {
    ++stats.get;
    ++stats.update;
    auto found = cache_->template findToWrite<detail::ObjectCacheValue>(key);
    if (!found) {
      ++stats.getMiss;
      ++stats.updateMiss;
      return OpResultType::kNop;
    }

    found->update(gen);
    return OpResultType::kNop;
  }

  OpResultType deleteKey(ThroughputStats& stats, folly::StringPiece key) {
    ++stats.del;
    if (!cache_->remove(key)) {
      ++stats.delNotFound;
    }
    return OpResultType::kNop;
  }

  void limitRate() {
    if (rateLimiter_) {
      rateLimiter_->consumeWithBorrowAndWait(1);
    }
  }

  const CacheConfig cacheConfig_;
  const detail::ObjectCacheBenchConfig objectCacheConfig_;
  const bool objectSizeTrackingEnabled_{false};
  std::unique_ptr<ObjectCacheT> cache_;
  std::unique_ptr<folly::BasicTokenBucket<>> rateLimiter_;
};

} // namespace facebook::cachelib::cachebench
