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

#include "cachelib/cachebench/cache/CacheStats.h"
#include "cachelib/cachebench/cache/components/Components.h"
#include "cachelib/cachebench/util/Exceptions.h"

using namespace facebook::cachelib::interface;

namespace facebook {
namespace cachelib {
namespace cachebench {

CacheComponentStressor::CacheComponentStressor(
    const CacheConfig& cacheConfig,
    StressorConfig config,
    std::unique_ptr<GeneratorBase>&& generator)
    : CacheStressorBase(std::move(config), std::move(generator)),
      cache_(createCacheComponent(validate(cacheConfig))) {}

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

std::unique_ptr<StatsBase> CacheComponentStressor::getCacheStats() const {
  return std::make_unique<ComponentStats>(cache_->getStats());
}

const CacheConfig& CacheComponentStressor::validate(
    const CacheConfig& cacheConfig) const {
  if (config_.usesChainedItems()) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support chained items");
  } else if (cacheConfig.useTraceTimeStamp) {
    throw std::invalid_argument(
        "CacheComponentStressor does not support trace timestamps");
  } else if (cacheConfig.nvmCacheSizeMB > 0) {
    throw std::invalid_argument(
        "Use cacheSizeMB to set cache size, even for flash caching");
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
  return cacheConfig;
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
      // Save requestId before co_await: req is a reference to a thread-local
      // that can be overwritten if the coroutine migrates threads.
      auto requestId = req.requestId;

      OpResultType result = co_await executeOperation(stats, req, gen);

      lastRequestId = requestId;
      if (requestId) {
        wg_->notifyResult(*requestId, result);
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
  // Extract all fields from req before any co_await. The req reference points
  // to a thread-local that can be overwritten after coroutine thread migration.
  std::string ownedKey(req.key);
  if (op == OpType::kLoneGet || op == OpType::kLoneSet) {
    ownedKey = Request::getUniqueKey();
  }
  std::string_view key = ownedKey;

  size_t size = *(req.sizeBegin);
  uint32_t ttlSecs = req.ttlSecs;
  auto admFeatureMap = req.admFeatureMap;
  auto itemValue = req.itemValue;

  switch (op) {
  case OpType::kLoneSet:
  case OpType::kSet: {
    if (config_.onlySetIfMiss) {
      auto findResult = co_await cache_->find(Key{key});
      if (findResult.hasValue() && findResult.value().has_value()) {
        co_return OpResultType::kNop;
      }
    }
    co_return co_await setKey(stats, key, size, ttlSecs, admFeatureMap,
                              itemValue);
  }

  case OpType::kLoneGet:
  case OpType::kGet: {
    co_return co_await getKey(stats, key, size, ttlSecs, admFeatureMap,
                              itemValue);
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
    ThroughputStats& stats,
    const std::string_view key,
    size_t size,
    uint32_t ttlSecs,
    const std::unordered_map<std::string, std::string>& featureMap,
    const std::string& itemValue) {
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
      co_await setKey(stats, key, size, ttlSecs, featureMap, itemValue);
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
