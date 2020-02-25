#pragma once

#include <atomic>
#include <iostream>
#include <unordered_set>

#include <folly/Random.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/TestStopper.h"
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
                std::unique_ptr<GeneratorBase>&& generator)
      : config_(std::move(config)),
        throughputStats_(config_.numThreads),
        wg_(std::move(generator)),
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
    // Fill up the cache with specified key/value distribution
    if (config_.prepopulateCache) {
      try {
        auto ret = prepopulateCache();
        std::cout << folly::sformat("Inserted {:,} keys in {:.2f} mins",
                                    ret.first,
                                    ret.second.count() / 60.)
                  << std::endl;
        for (auto pid : cache_->poolIds()) {
          auto numItems = cache_->getPoolStats(pid).numItems();
          std::cout << folly::sformat("Pool {}: {:,} keys in RAM",
                                      static_cast<int>(pid), numItems)
                    << std::endl;
        }

        // wait for all the cache operations for prepopulation to complete.
        if (!cache_->isRamOnly()) {
          cache_->flushNvmCache();
        }

        std::cout << "== Cache Stats ==" << std::endl;
        cache_->getStats().render(std::cout);
        std::cout << std::endl;
      } catch (const cachebench::EndOfTrace& ex) {
        std::cout << "End of trace encountered during prepopulaiton, "
                     "attempting to continue..."
                  << std::endl;
      }
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
    assert(handle);
    assert(cache_->getSize(handle) <= 4 * 1024 * 1024);
    if (cache_->consistencyCheckEnabled()) {
      cache_->setUint64ToItem(handle, folly::Random::rand64(rng));
    } else {
      std::memcpy(cache_->getMemory(handle), hardcodedString_.data(),
                  cache_->getSize(handle));
    }
  }

  // Fill the cache and make sure all pools are full.
  // It's the generator's resposibility of providing keys that obey to the
  // distribution
  std::pair<size_t, std::chrono::seconds> prepopulateCache() {
    std::atomic<uint64_t> totalCount = 0;

    auto prePopulateFn = [&]() {
      std::mt19937 gen(folly::Random::rand32());
      std::discrete_distribution<> keyPoolDist(
          config_.keyPoolDistribution.begin(),
          config_.keyPoolDistribution.end());
      size_t count = 0;
      std::unordered_set<PoolId> fullPools;
      std::optional<uint64_t> lastRequestId = std::nullopt;

      // in some cases eviction will never happen. To avoid infinite loop in
      // this case, we use continuousCacheHits as the signal of cache full
      std::unordered_map<PoolId, size_t> continuousCacheHits;

      while (fullPools.size() < cache_->numPools()) {
        // get a pool according to keyPoolDistribution
        auto pid = keyPoolDist(gen);
        if (fullPools.count(pid) > 0) {
          // it's a known full pool, skip it
          continue;
        }

        // getPoolStats is expensive to fetch on every iteration and can
        // serialize the cache creation. So check the eviction count every 10k
        // insertions
        if (count % 10000 == 0 &&
            cache_->getPoolStats(pid).numEvictions() > 0) {
          // it's a new found full pool, record it and reselect one
          fullPools.insert(pid);
          continue;
        }

        // now we have a good pool, get a request
        const Request& req = getReq(pid, gen, lastRequestId);
        if (cache_->find(req.key)) {
          // Treat 1000 continuous cache hits as cache full
          if (++continuousCacheHits[pid] > 1000) {
            fullPools.insert(pid);
          }
          continue;
        }

        // cache miss. Allocate and write to cache
        continuousCacheHits[pid] = 0;

        // req contains a new key, set it to cache
        const auto allocHandle =
            cache_->allocate(pid, req.key, req.key.size() + *(req.sizeBegin));
        if (allocHandle) {
          cache_->insertOrReplace(allocHandle);
          // We throttle in case we are using flash so that we dont drop
          // evictions to flash by inserting at a very high rate.
          if (!cache_->isRamOnly() && count % 8 == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
          }
          count++;
          if (req.requestId) {
            wg_->notifyResult(*req.requestId, OpResultType::kSetSuccess);
          }
        }
      }
      totalCount.fetch_add(count);
    };

    auto numThreads = config_.prepopulateThreads ? config_.prepopulateThreads
                                                 : config_.numThreads;
    auto duration = detail::executeParallel(prePopulateFn, numThreads);
    return std::make_pair(totalCount.load(), duration);
  }

  // Runs a number of operations on the cache allocator. The actual
  // operations and key/value used are determined by a set of random
  // number generators
  //
  // Throughput and Hit/Miss rates are tracked here as well
  //
  // @param genOp       randomly generate operations
  // @param genOpPool   randomly choose a pool to run this op in
  //                    this pretty much only works for allocate
  // @param genValSize  randomly chooses a size
  // @param genChainedItemLen   randomly chooses length for the chain to append
  // @param genChainedItemValSize   randomly choose a size for chained item
  // @param stats       Throughput stats
  void stressByDiscreteDistribution(ThroughputStats& stats) {
    wg_->registerThread();

    std::mt19937 gen(folly::Random::rand32());
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

        const auto pid = opPoolDist(gen);
        const Request& req(getReq(pid, gen, lastRequestId));
        const std::string* key = &(req.key);
        const OpType op = wg_->getOp(pid, gen, req.requestId);
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
          result = setKey(pid, stats, key, *(req.sizeBegin));

          throttleFn();
          break;
        }
        case OpType::kLoneGet:
        case OpType::kGet: {
          ++stats.get;

          Mode lockMode = Mode::Shared;

          chainedItemLock(lockMode, *key);
          SCOPE_EXIT { chainedItemUnlock(lockMode, *key); };
          // TODO currently pure lookaside, we should
          // add a distribution over sequences of requests/access patterns
          // e.g. get-no-set and set-no-get
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
              setKey(pid, stats, key, *(req.sizeBegin));
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
          auto it = cache_->findForWriteForTest(*key, AccessMode::kRead);
          if (!it) {
            ++stats.getMiss;

            ++stats.set;
            it = cache_->allocate(pid, *key, *(req.sizeBegin));
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
  }

  OpResultType setKey(PoolId pid,
                      ThroughputStats& stats,
                      const std::string* key,
                      size_t size) {
    ++stats.set;
    auto it = cache_->allocate(pid, *key, size);
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
                        std::mt19937& gen,
                        std::optional<uint64_t>& lastRequestId) {
    while (true) {
      const Request& req(wg_->getReq(pid, gen, lastRequestId));
      if (config_.checkConsistency && cache_->isInvalidKey(req.key)) {
        continue;
      }
      return req;
    }
  }

  const StressorConfig config_;

  std::vector<ThroughputStats> throughputStats_;

  std::unique_ptr<GeneratorBase> wg_;

  // locks when using chained item and moving.
  std::array<folly::SharedMutex, 1024> locks_;

  // if locking is enabled.
  std::atomic<bool> lockEnabled_{false};

  // memorize rng to improve random performance
  folly::ThreadLocalPRNG rng;

  const std::string hardcodedString_;

  std::unique_ptr<CacheT> cache_;

  std::thread stressWorker_;

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  uint64_t testDurationNs_{0};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
