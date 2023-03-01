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

#include <folly/Random.h>
#include <folly/TokenBucket.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseThread.h>

#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_set>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/cache/TimeStampTicker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr uint32_t kNvmAsyncCacheWarmUpCheckRate = 1000;

// Implementation of stressor that uses a workload generator to stress an
// instance of the cache.  All item's value in AsyncCacheStressor follows
// CacheValue schema, which contains a few integers for sanity checks use. So it
// is invalid to use item.getMemory and item.getSize APIs.
template <typename Allocator>
class AsyncCacheStressor : public Stressor {
 public:
  using CacheT = Cache<Allocator>;
  using Key = typename CacheT::Key;
  using WriteHandle = typename CacheT::WriteHandle;

  // @param cacheConfig   the config to instantiate the cache instance
  // @param config        stress test config
  // @param generator     workload  generator
  AsyncCacheStressor(CacheConfig cacheConfig,
                     StressorConfig config,
                     std::unique_ptr<GeneratorBase>&& generator)
      : config_(std::move(config)),
        throughputStats_(config_.numThreads),
        wg_(std::move(generator)),
        hardcodedString_(genHardcodedString()),
        endTime_{std::chrono::system_clock::time_point::max()} {
    // if either consistency check is enabled or if we want to move
    // items during slab release, we want readers and writers of chained
    // allocs to be synchronized
    typename CacheT::ChainedItemMovingSync movingSync;
    if (config_.usesChainedItems() &&
        (cacheConfig.moveOnSlabRelease || config_.checkConsistency)) {
      lockEnabled_ = true;

      struct CacheStressSyncObj : public CacheT::SyncObj {
        std::unique_lock<folly::SharedMutex> lock;

        CacheStressSyncObj(AsyncCacheStressor& s, std::string itemKey)
            : lock{s.chainedItemAcquireUniqueLock(itemKey)} {}
      };
      movingSync = [this](typename CacheT::Item::Key key) {
        return std::make_unique<CacheStressSyncObj>(*this, key.str());
      };
    }

    if (cacheConfig.useTraceTimeStamp &&
        cacheConfig.tickerSynchingSeconds > 0) {
      // When using trace based replay for generating the workload,
      // TimeStampTicker allows syncing the notion of time between the
      // cache and the workload generator based on timestamps in the trace.
      ticker_ = std::make_shared<TimeStampTicker>(
          config.numThreads, cacheConfig.tickerSynchingSeconds,
          [wg = wg_.get()](double elapsedSecs) {
            wg->renderWindowStats(elapsedSecs, std::cout);
          });
      cacheConfig.ticker = ticker_;
    }

    cache_ = std::make_unique<CacheT>(cacheConfig, movingSync, "",
                                      config_.touchValue);
    if (config_.opPoolDistribution.size() > cache_->numPools()) {
      throw std::invalid_argument(folly::sformat(
          "more pools specified in the test than in the cache. "
          "test: {}, cache: {}",
          config_.opPoolDistribution.size(), cache_->numPools()));
    }
    if (config_.keyPoolDistribution.size() != cache_->numPools()) {
      throw std::invalid_argument(folly::sformat(
          "different number of pools in the test from in the cache. "
          "test: {}, cache: {}",
          config_.keyPoolDistribution.size(), cache_->numPools()));
    }

    if (config_.checkConsistency) {
      cache_->enableConsistencyCheck(wg_->getAllKeys());
    }
    if (config_.opRatePerSec > 0) {
      rateLimiter_ = std::make_unique<folly::BasicTokenBucket<>>(
          config_.opRatePerSec, config_.opRatePerSec);
    }
  }

  ~AsyncCacheStressor() override { finish(); }

  // Start the stress test by spawning the worker threads and waiting for them
  // to finish the stress operations.
  void start() override {
    {
      std::lock_guard<std::mutex> l(timeMutex_);
      startTime_ = std::chrono::system_clock::now();
    }
    std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                config_.numThreads * config_.numOps / 1e6)
              << std::endl;

    stressWorker_ = std::thread([this] {
      std::vector<std::thread> workers;
      for (uint64_t i = 0; i < config_.numThreads; ++i) {
        workers.push_back(
            std::thread([this, throughputStats = &throughputStats_.at(i)]() {
              stressByDiscreteDistribution(*throughputStats);
            }));
      }
      for (auto& worker : workers) {
        worker.join();
      }
      {
        std::lock_guard<std::mutex> l(timeMutex_);
        endTime_ = std::chrono::system_clock::now();
      }
    });
  }

  // Block until all stress workers are finished.
  void finish() override {
    if (stressWorker_.joinable()) {
      stressWorker_.join();
    }
    wg_->markShutdown();
    cache_->clearCache(config_.maxInvalidDestructorCount);
  }

  // abort the stress run by indicating to the workload generator and
  // delegating to the base class abort() to stop the test.
  void abort() override {
    wg_->markShutdown();
    Stressor::abort();
  }

  // obtain stats from the cache instance.
  Stats getCacheStats() const override { return cache_->getStats(); }

  // obtain aggregated throughput stats for the stress run so far.
  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats res{};
    for (const auto& stats : throughputStats_) {
      res += stats;
    }

    return res;
  }

  void renderWorkloadGeneratorStats(uint64_t elapsedTimeNs,
                                    std::ostream& out) const override {
    wg_->renderStats(elapsedTimeNs, out);
  }

  void renderWorkloadGeneratorStats(
      uint64_t elapsedTimeNs, folly::UserCounters& counters) const override {
    wg_->renderStats(elapsedTimeNs, counters);
  }

  uint64_t getTestDurationNs() const override {
    std::lock_guard<std::mutex> l(timeMutex_);
    return std::chrono::nanoseconds{
        std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
        .count();
  }

 private:
  static std::string genHardcodedString() {
    const std::string s = "The quick brown fox jumps over the lazy dog. ";
    std::string val;
    for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
      val += s;
    }
    return val;
  }

  folly::SharedMutex& getLock(Key key) {
    auto bucket = MurmurHash2{}(key.data(), key.size()) % locks_.size();
    return locks_[bucket];
  }

  // This function handles the async operations for kGet and kLoneGet. It calls
  // 'asyncFind' and then 'onReadyFn' if the returned handle is ready. If the
  // returned handle is not ready, it puts a SemiFuture to an event base thread
  // which later calls 'onReadyFn' when the handle is ready.
  void asyncGet(PoolId pid,
                ThroughputStats& stats,
                const Request* req,
                folly::EventBase* evb,
                const std::string* key) {
    ++stats.get;
    auto lock = chainedItemAcquireSharedLock(*key);

    if (ticker_) {
      ticker_->updateTimeStamp(req->timestamp);
    }
    // TODO currently pure lookaside, we should
    // add a distribution over sequences of requests/access patterns
    // e.g. get-no-set and set-no-get

    auto onReadyFn = [&, req, key = *key,
                      l = std::move(lock)](auto hdl) mutable {
      auto result = OpResultType::kGetMiss;

      if (hdl == nullptr) {
        ++stats.getMiss;
        result = OpResultType::kGetMiss;

        if (config_.enableLookaside) {
          // allocate and insert on miss
          // upgrade access privledges, (lock_upgrade is not
          // appropriate here)
          l.unlock();
          auto xlock = chainedItemAcquireUniqueLock(key);
          setKey(pid, stats, &key, *(req->sizeBegin), req->ttlSecs,
                 req->admFeatureMap);
        }
      } else {
        result = OpResultType::kGetHit;
      }

      if (req->requestId) {
        // req might be deleted after calling notifyResult()
        wg_->notifyResult(*req->requestId, result);
      }
    };

    cache_->recordAccess(*key);
    auto sf = cache_->asyncFind(*key);
    if (sf.isReady()) {
      // If the handle is ready, call onReadyFn directly to process the handle
      onReadyFn(std::move(sf).value());
      return;
    }

    std::move(sf)
        .deferValue(std::move(onReadyFn))
        .via(folly::Executor::getKeepAliveToken(evb));

    return;
  }

  // This function handles the async operations for kAddChained. It calls
  // 'asyncFind' and then 'onReadyFn' if the returned handle is ready. If the
  // returned handle is not ready, it puts a SemiFuture to an event base thread
  // which later calls 'onReadyFn' when the handle is ready.
  void asyncAddChained(PoolId pid,
                       ThroughputStats& stats,
                       const Request* req,
                       folly::EventBase* evb,
                       const std::string* key) {
    ++stats.get;
    auto lock = chainedItemAcquireUniqueLock(*key);

    // This was moved outside the lambda, as otherwise gcc-8.x crashes with an
    // internal compiler error here (suspected regression in folly).
    XDCHECK(req->sizeBegin + 1 != req->sizeEnd);

    auto onReadyFn = [&, req, key, l = std::move(lock), pid](auto hdl) {
      WriteHandle wHdl;
      if (hdl == nullptr) {
        ++stats.getMiss;

        ++stats.set;
        wHdl = cache_->allocate(pid, *key, *(req->sizeBegin), req->ttlSecs);
        if (!wHdl) {
          ++stats.setFailure;
          return;
        }
        populateItem(wHdl);
        cache_->insertOrReplace(wHdl);
      } else {
        wHdl = std::move(hdl).toWriteHandle();
      }
      bool chainSuccessful = false;
      for (auto j = req->sizeBegin + 1; j != req->sizeEnd; j++) {
        ++stats.addChained;

        const auto size = *j;
        auto child = cache_->allocateChainedItem(wHdl, size);
        if (!child) {
          ++stats.addChainedFailure;
          continue;
        }
        chainSuccessful = true;
        populateItem(child);
        cache_->addChainedItem(wHdl, std::move(child));
      }
      if (chainSuccessful && cache_->consistencyCheckEnabled()) {
        cache_->trackChainChecksum(wHdl);
      }
    };

    // Always use asyncFind as findToWrite is sync when using HybridCache
    auto sf = cache_->asyncFind(*key);
    if (sf.isReady()) {
      onReadyFn(std::move(sf).value());
      return;
    }

    std::move(sf)
        .deferValue(std::move(onReadyFn))
        .via(folly::Executor::getKeepAliveToken(evb));
  }

  // This function handles the async operations for kUpdate. It calls
  // 'asyncFind' and then 'onReadyFn' if the returned handle is ready. If the
  // returned handle is not ready, it puts a SemiFuture to an event base thread
  // which later calls 'onReadyFn' when the handle is ready.
  void asyncUpdate(ThroughputStats& stats,
                   const Request* req,
                   folly::EventBase* evb,
                   const std::string* key) {
    ++stats.get;
    ++stats.update;
    auto lock = chainedItemAcquireUniqueLock(*key);
    if (ticker_) {
      ticker_->updateTimeStamp(req->timestamp);
    }

    auto onReadyFn = [&, l = std::move(lock)](auto hdl) {
      if (hdl == nullptr) {
        ++stats.getMiss;
        ++stats.updateMiss;
        return;
      }
      auto wHdl = std::move(hdl).toWriteHandle();
      cache_->updateItemRecordVersion(wHdl);
    };

    auto sf = cache_->asyncFind(*key);
    if (sf.isReady()) {
      onReadyFn(std::move(sf).value());
      return;
    }

    std::move(sf)
        .deferValue(std::move(onReadyFn))
        .via(folly::Executor::getKeepAliveToken(evb));
  }

  // TODO maintain state on whether key has chained allocs and use it to only
  // lock for keys with chained items.
  auto chainedItemAcquireSharedLock(Key key) {
    using Lock = std::shared_lock<folly::SharedMutex>;
    return lockEnabled_ ? Lock{getLock(key)} : Lock{};
  }
  auto chainedItemAcquireUniqueLock(Key key) {
    using Lock = std::unique_lock<folly::SharedMutex>;
    return lockEnabled_ ? Lock{getLock(key)} : Lock{};
  }

  // populate the input item handle according to the stress setup.
  void populateItem(WriteHandle& handle) {
    if (!config_.populateItem) {
      return;
    }
    XDCHECK(handle);
    XDCHECK_LE(cache_->getSize(handle), 4ULL * 1024 * 1024);
    if (cache_->consistencyCheckEnabled()) {
      cache_->setUint64ToItem(handle, folly::Random::rand64(rng));
    } else {
      cache_->setStringItem(handle, hardcodedString_);
    }
  }

  // Runs a number of operations on the cache allocator. The actual
  // operations and key/value used are determined by the workload generator
  // initialized.
  //
  // Throughput and Hit/Miss rates are tracked here as well
  //
  // @param stats       Throughput stats
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
        std::this_thread::sleep_for(opDelay);
      }
      // Limit the rate if specified.
      limitRate();
    };

    // thread local variable for event base executor thread
    folly::EventBaseThread ebt;
    folly::EventBase* eb = ebt.getEventBase();

    // lastRequestId is used in piecewise generator, which is not compatible
    // with current asynchronous design, we remove all the lastRequestId used
    std::optional<uint64_t> lastRequestId = std::nullopt;
    for (uint64_t i = 0;
         i < config_.numOps &&
         cache_->getInconsistencyCount() < config_.maxInconsistencyCount &&
         cache_->getInvalidDestructorCount() <
             config_.maxInvalidDestructorCount &&
         !cache_->isNvmCacheDisabled() && !shouldTestStop();
         ++i) {
      try {
        // at the end of every operation, throttle per the config.
        SCOPE_EXIT { throttleFn(); };
          // detect refcount leaks when run in  debug mode.
#ifndef NDEBUG
        auto checkCnt = [](int cnt) {
          if (cnt != 0) {
            throw std::runtime_error(folly::sformat("Refcount leak {}", cnt));
          }
        };
        checkCnt(cache_->getHandleCountForThread());
        SCOPE_EXIT { checkCnt(cache_->getHandleCountForThread()); };
#endif
        ++stats.ops;

        const auto pid = static_cast<PoolId>(opPoolDist(gen));
        const Request& req(getReq(pid, gen, lastRequestId));
        OpType op = req.getOp();
        const std::string* key = &(req.key);
        std::string oneHitKey;
        if (op == OpType::kLoneGet || op == OpType::kLoneSet) {
          oneHitKey = Request::getUniqueKey();
          key = &oneHitKey;
        }

        OpResultType result(OpResultType::kNop);
        switch (op) {
        case OpType::kLoneSet:
        case OpType::kSet: {
          auto lock = chainedItemAcquireUniqueLock(*key);
          result = setKey(pid, stats, key, *(req.sizeBegin), req.ttlSecs,
                          req.admFeatureMap);

          break;
        }
        case OpType::kLoneGet:
        case OpType::kGet: {
          asyncGet(pid, stats, &req, eb, std::move(key));
          break;
        }
        case OpType::kDel: {
          ++stats.del;
          auto lock = chainedItemAcquireUniqueLock(*key);
          auto res = cache_->remove(*key);
          if (res == CacheT::RemoveRes::kNotFoundInRam) {
            ++stats.delNotFound;
          }
          break;
        }
        case OpType::kAddChained: {
          asyncAddChained(pid, stats, &req, eb, std::move(key));
          break;
        }
        case OpType::kUpdate: {
          asyncUpdate(stats, &req, eb, std::move(key));
          break;
        }
        default:
          throw std::runtime_error(
              folly::sformat("invalid operation generated: {}", (int)op));
          break;
        }

        if (op == OpType::kLoneGet || op == OpType::kGet) {
          // The result will be set in the 'onReadyFn' of 'get'
          // For kAddChained and kUpdate, the result has never been changed so
          // we just follow the original path
          continue;
        }

        if (req.requestId) {
          // req might be deleted after calling notifyResult()
          wg_->notifyResult(*req.requestId, result);
        }
      } catch (const cachebench::EndOfTrace& ex) {
        break;
      }
    }
    wg_->markFinish();
  }

  // inserts key into the cache if the admission policy also indicates the
  // key is worthy to be cached.
  //
  // @param pid         pool id to insert the key
  // @param stats       reference to the stats structure.
  // @param key         the key to be inserted
  // @param size        size of the cache value
  // @param ttlSecs     ttl for the value
  // @param featureMap  feature map for admission policy decisions.
  OpResultType setKey(
      PoolId pid,
      ThroughputStats& stats,
      const std::string* key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap) {
    // check the admission policy first, and skip the set operation
    // if the policy returns false
    if (config_.admPolicy && !config_.admPolicy->accept(featureMap)) {
      return OpResultType::kSetSkip;
    }

    ++stats.set;
    auto it = cache_->allocate(pid, *key, size, ttlSecs);
    if (it == nullptr) {
      ++stats.setFailure;
      return OpResultType::kSetFailure;
    } else {
      populateItem(it);
      cache_->insertOrReplace(it);
      return OpResultType::kSetSuccess;
    }
  }

  // fetch a request from the workload generator for a particular pool
  // @param pid             the pool id chosen for the request.
  // @param gen             the thread local random number generator to be
  // fed
  //                        to the workload generator  for constructing the
  //                        request.
  // @param lastRequestId   optional information about the last request id
  // that
  //                        was given to this thread by the workload
  //                        generator. This is used to provide continuity by
  //                        some generator implementations.

  const Request& getReq(const PoolId& pid,
                        std::mt19937_64& gen,
                        std::optional<uint64_t>& lastRequestId) {
    while (true) {
      const Request& req(wg_->getReq(pid, gen, lastRequestId));
      if (config_.checkConsistency && cache_->isInvalidKey(req.key)) {
        continue;
      }
      // TODO: allow callback on nvm eviction instead of checking it repeatedly.
      if (config_.checkNvmCacheWarmUp &&
          folly::Random::oneIn(kNvmAsyncCacheWarmUpCheckRate)) {
        checkNvmCacheWarmedUp(req.timestamp);
      }
      return req;
    }
  }

  void limitRate() {
    if (!rateLimiter_) {
      return;
    }
    rateLimiter_->consumeWithBorrowAndWait(1);
  }

  void checkNvmCacheWarmedUp(uint64_t requestTimestamp) {
    if (hasNvmCacheWarmedUp_) {
      // already notified, nothing to do
      return;
    }
    if (cache_->isNvmCacheDisabled()) {
      return;
    }
    if (cache_->hasNvmCacheWarmedUp()) {
      wg_->setNvmCacheWarmedUp(requestTimestamp);
      XLOG(INFO) << "NVM cache has been warmed up";
      hasNvmCacheWarmedUp_ = true;
    }
  }

  const StressorConfig config_; // config for the stress run

  std::vector<ThroughputStats> throughputStats_; // thread local stats

  std::unique_ptr<GeneratorBase> wg_; // workload generator

  // locks when using chained item and moving.
  std::array<folly::SharedMutex, 1024> locks_;

  // if locking is enabled.
  std::atomic<bool> lockEnabled_{false};

  // memorize rng to improve random performance
  folly::ThreadLocalPRNG rng;

  // string used for generating random payloads
  const std::string hardcodedString_;

  std::unique_ptr<CacheT> cache_;

  // Ticker that syncs the time according to trace timestamp.
  std::shared_ptr<TimeStampTicker> ticker_;

  // main stressor thread
  std::thread stressWorker_;

  // mutex to protect reading the timestamps.
  mutable std::mutex timeMutex_;

  // start time for the stress test
  std::chrono::time_point<std::chrono::system_clock> startTime_;

  // time when benchmark finished. This is set once the benchmark finishes
  std::chrono::time_point<std::chrono::system_clock> endTime_;

  // Token bucket used to limit the operations per second.
  std::unique_ptr<folly::BasicTokenBucket<>> rateLimiter_;

  // Whether flash cache has been warmed up
  bool hasNvmCacheWarmedUp_{false};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
