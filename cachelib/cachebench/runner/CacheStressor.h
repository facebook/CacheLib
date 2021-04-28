#pragma once

#include <folly/Random.h>
#include <folly/TokenBucket.h>

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
template <typename Allocator>
class CacheStressor : public Stressor {
 public:
  using CacheT = Cache<Allocator>;
  using Key = typename CacheT::Key;
  using ItemHandle = typename CacheT::ItemHandle;

  enum class Mode { Exclusive, Shared };

  // @param wrapper   wrapper that encapsulate the CacheAllocator
  // @param config    stress test config
  CacheStressor(CacheConfig cacheConfig,
                StressorConfig config,
                std::unique_ptr<GeneratorBase>&& generator,
                std::unique_ptr<StressorAdmPolicy> admPolicy)
      : config_(std::move(config)),
        throughputStats_(config_.numThreads),
        wg_(std::move(generator)),
        admPolicy_(std::move(admPolicy)),
        hardcodedString_(genHardcodedString()) {
    // if either consistency check is enabled or if we want to move
    // items during slab release, we want readers and writers to chained
    // allocs to be synchronized
    typename CacheT::ChainedItemMovingSync movingSync;
    if (config_.usesChainedItems() &&
        (cacheConfig.moveOnSlabRelease || config_.checkConsistency)) {
      lockEnabled_ = true;

      struct CacheStressSyncObj : public CacheT::SyncObj {
        CacheStressor& stressor;
        std::string key;

        CacheStressSyncObj(CacheStressor& s, std::string itemKey)
            : stressor(s), key(std::move(itemKey)) {
          stressor.chainedItemLock(Mode::Exclusive, key);
        }
        ~CacheStressSyncObj() override {
          stressor.chainedItemUnlock(Mode::Exclusive, key);
        }
      };
      movingSync = [this](typename CacheT::Item::Key key) {
        return std::make_unique<CacheStressSyncObj>(*this, key.str());
      };
    }

    if (cacheConfig.useTraceTimeStamp &&
        cacheConfig.tickerSynchingSeconds > 0) {
      // Create TimeStampTicker to allow time synching based on traces'
      // timestamps.
      ticker_ = std::make_shared<TimeStampTicker>(
          config.numThreads, cacheConfig.tickerSynchingSeconds,
          [wg = wg_.get()](double elapsedSecs) {
            wg->renderWindowStats(elapsedSecs, std::cout);
          });
      cacheConfig.ticker = ticker_;
    }

    cache_ = std::make_unique<CacheT>(cacheConfig, movingSync);
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

  ~CacheStressor() override { finish(); }

  // Start the stress test
  // We first launch a stats worker that will wake up periodically
  // Then we start another worker thread that will be responsible for
  // spawning all the stress test threads
  void start() override {
    startTime_ = std::chrono::system_clock::now();

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

      testDurationNs_ =
          std::chrono::nanoseconds{std::chrono::system_clock::now() -
                                   startTime_}
              .count();
    });
  }

  // Block until all stress workers are finished.
  void finish() override {
    if (stressWorker_.joinable()) {
      stressWorker_.join();
    }
    wg_->markShutdown();
  }

  void abort() override {
    wg_->markShutdown();
    Stressor::abort();
  }

  std::chrono::time_point<std::chrono::system_clock> startTime()
      const override {
    return startTime_;
  }

  Stats getCacheStats() const override { return cache_->getStats(); }

  ThroughputStats aggregateThroughputStats() const override {
    if (throughputStats_.empty()) {
      return {config_.name};
    }

    ThroughputStats res{config_.name};
    for (const auto& stats : throughputStats_) {
      res += stats;
    }

    return res;
  }

  void renderWorkloadGeneratorStats(uint64_t elapsedTimeNs,
                                    std::ostream& out) const override {
    wg_->renderStats(elapsedTimeNs, out);
  }

  uint64_t getTestDurationNs() const override { return testDurationNs_; }

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

  // TODO maintain state on whether key has chained allocs and use it to only
  // lock for keys with chained items.
  void chainedItemLock(Mode mode, Key key) {
    if (lockEnabled_) {
      auto& lock = getLock(key);
      mode == Mode::Shared ? lock.lock_shared() : lock.lock();
    }
  }
  void chainedItemUnlock(Mode mode, Key key) {
    if (lockEnabled_) {
      auto& lock = getLock(key);
      mode == Mode::Shared ? lock.unlock_shared() : lock.unlock();
    }
  }

  void populateItem(ItemHandle& handle) {
    if (!config_.populateItem) {
      return;
    }
    assert(handle);
    assert(cache_->getSize(handle) <= 4 * 1024 * 1024);
    if (cache_->consistencyCheckEnabled()) {
      cache_->setUint64ToItem(handle, folly::Random::rand64(rng));
    } else {
      std::memcpy(cache_->getWritableMemory(handle), hardcodedString_.data(),
                  cache_->getSize(handle));
    }
  }

  // Runs a number of operations on the cache allocator. The actual
  // operations and key/value used are determined by a set of random
  // number generators
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

    std::optional<uint64_t> lastRequestId = std::nullopt;
    for (uint64_t i = 0;
         i < config_.numOps &&
         cache_->getInconsistencyCount() < config_.maxInconsistencyCount &&
         !cache_->isNvmCacheDisabled() && !shouldTestStop();
         ++i) {
      try {
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
          chainedItemLock(Mode::Exclusive, *key);
          SCOPE_EXIT { chainedItemUnlock(Mode::Exclusive, *key); };
          result = setKey(pid, stats, key, *(req.sizeBegin), req.ttlSecs,
                          req.admFeatureMap);

          throttleFn();
          break;
        }
        case OpType::kLoneGet:
        case OpType::kGet: {
          ++stats.get;

          Mode lockMode = Mode::Shared;

          chainedItemLock(lockMode, *key);
          SCOPE_EXIT { chainedItemUnlock(lockMode, *key); };
          if (ticker_) {
            ticker_->updateTimeStamp(req.timestamp);
          }
          // TODO currently pure lookaside, we should
          // add a distribution over sequences of requests/access patterns
          // e.g. get-no-set and set-no-get
          cache_->recordAccess(*key);
          auto it = cache_->find(*key, AccessMode::kRead);
          if (it == nullptr) {
            ++stats.getMiss;
            result = OpResultType::kGetMiss;

            if (config_.enableLookaside) {
              // allocate and insert on miss
              // upgrade access privledges, (lock_upgrade is not
              // appropriate here)
              chainedItemUnlock(lockMode, *key);
              lockMode = Mode::Exclusive;
              chainedItemLock(lockMode, *key);
              setKey(pid, stats, key, *(req.sizeBegin), req.ttlSecs,
                     req.admFeatureMap);
            }
          } else {
            result = OpResultType::kGetHit;
          }

          throttleFn();
          break;
        }
        case OpType::kDel: {
          ++stats.del;
          chainedItemLock(Mode::Exclusive, *key);
          SCOPE_EXIT { chainedItemUnlock(Mode::Exclusive, *key); };
          auto res = cache_->remove(*key);
          if (res == CacheT::RemoveRes::kNotFoundInRam) {
            ++stats.delNotFound;
          }
          throttleFn();
          break;
        }
        case OpType::kAddChained: {
          ++stats.get;
          chainedItemLock(Mode::Exclusive, *key);
          SCOPE_EXIT { chainedItemUnlock(Mode::Exclusive, *key); };
          auto it = cache_->find(*key, AccessMode::kRead);
          if (!it) {
            ++stats.getMiss;

            ++stats.set;
            it = cache_->allocate(pid, *key, *(req.sizeBegin), req.ttlSecs);
            if (!it) {
              ++stats.setFailure;
              break;
            }
            populateItem(it);
            cache_->insertOrReplace(it);
          }
          XDCHECK(req.sizeBegin + 1 != req.sizeEnd);
          bool chainSuccessful = false;
          for (auto j = req.sizeBegin + 1; j != req.sizeEnd; j++) {
            ++stats.addChained;

            const auto size = *j;
            auto child = cache_->allocateChainedItem(it, size);
            if (!child) {
              ++stats.addChainedFailure;
              continue;
            }
            chainSuccessful = true;
            populateItem(child);
            cache_->addChainedItem(it, std::move(child));
          }
          if (chainSuccessful && cache_->consistencyCheckEnabled()) {
            cache_->trackChainChecksum(it);
          }
          throttleFn();
          break;
        }
        default:
          throw std::runtime_error(
              folly::sformat("invalid operation generated: {}", (int)op));
          break;
        }

        lastRequestId = req.requestId;
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

  OpResultType setKey(
      PoolId pid,
      ThroughputStats& stats,
      const std::string* key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap) {
    // check the admission policy first, and skip the set operation
    // if the policy returns false
    if (!admPolicy_->accept(featureMap)) {
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

  const Request& getReq(const PoolId& pid,
                        std::mt19937_64& gen,
                        std::optional<uint64_t>& lastRequestId) {
    while (true) {
      const Request& req(wg_->getReq(pid, gen, lastRequestId));
      if (config_.checkConsistency && cache_->isInvalidKey(req.key)) {
        continue;
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

  const StressorConfig config_;

  std::vector<ThroughputStats> throughputStats_;

  std::unique_ptr<GeneratorBase> wg_;

  std::unique_ptr<StressorAdmPolicy> admPolicy_;

  // locks when using chained item and moving.
  std::array<folly::SharedMutex, 1024> locks_;

  // if locking is enabled.
  std::atomic<bool> lockEnabled_{false};

  // memorize rng to improve random performance
  folly::ThreadLocalPRNG rng;

  const std::string hardcodedString_;

  std::unique_ptr<CacheT> cache_;
  // Ticker that syncs the time according to trace timestamp.
  std::shared_ptr<TimeStampTicker> ticker_;

  std::thread stressWorker_;

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  uint64_t testDurationNs_{0};

  // Token bucket used to limit the operations per second.
  std::unique_ptr<folly::BasicTokenBucket<>> rateLimiter_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
