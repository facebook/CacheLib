#pragma once

#include <atomic>
#include <iostream>
#include <unordered_set>

#include <event.h>
#include <time.h>
#include <folly/Random.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/TestStopper.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"
#include "cachelib/external/memcached/protocol_binary.h"

extern "C" int init_memcached(int argc, char ** argv, size_t size_in_mb, uint32_t nthreads);
extern "C" int stop_memcached();
extern "C" struct conn;
extern "C" struct item;
extern "C" item *item_get(const char *key, const size_t nkey, conn *c, const bool do_update);
extern "C" conn *conn_new(const int sfd, int init_state, const int event_flags, const int read_buffer_size, int transport, struct event_base *base, void *ssl);
extern "C" item *item_alloc(char *key, size_t nkey, int flags, unsigned int exptime, int nbytes);
extern "C" item * process_cachebench_set(char * key, int nkey, char * val, int vlen, conn *c, int cmd);
extern "C" item * process_cachebench_get_or_touch(char * key, int nkey, conn *c, int cmd);
extern "C" int store_item(item *item, int comm, conn* c);
extern "C" struct LIBEVENT_THREAD;
extern "C" struct LIBEVENT_THREAD * threads;
extern "C" void bind_thread_helper(conn * c, uint32_t wid);
extern "C" struct event_base * main_base;
extern "C" int memcached_event_loop(bool * stop_main_loop);
extern "C" char *stats_prefix_dump(int *length);
extern "C" conn * connect_server(const char *hostname, in_port_t port,bool nonblock, const bool ssl);
extern "C" void send_ascii_command(const char *buf);
extern "C" void read_ascii_response(char *buffer, size_t size);
extern "C" int init_tester(int argc, char **argv);
extern "C" struct conn * con;


namespace facebook {
namespace cachelib {
namespace cachebench {
template <typename Allocator>
class MicroStressor : public Stressor {
 public:
  using CacheT = Cache<Allocator>;
  using Key = typename CacheT::Key;
  using ItemHandle = typename CacheT::ItemHandle;

  enum class Mode { Exclusive, Shared };

  // @param wrapper   wrapper that encapsulate the CacheAllocator
  // @param config    stress test config
  MicroStressor(CacheConfig cacheConfig,
                StressorConfig config,
                std::unique_ptr<GeneratorBase>&& generator)
      : config_(std::move(config)),
        throughputStats_(config_.numThreads),
        wg_(std::move(generator)),
        hardcodedString_(genHardcodedString()) {
    // threads and main_base are UNSAFE until this is called!
    if(init_memcached(0, NULL, cacheConfig.cacheSizeMB, config_.numThreads) == EXIT_SUCCESS &&
            init_tester(0, NULL) == EXIT_SUCCESS){
        memcached_running_ = true;
    }

    // Fill up the cache with specified key/value distribution
    memcached_loop_ = std::thread([loop=&stop_memcached_loop_]() {
                memcached_event_loop(loop);
    });
    if (config_.prepopulateCache) {
      try {
        auto ret = prepopulateCache();
        std::cout << folly::sformat("Inserted {:,} keys in {:.2f} mins",
                                    ret.first,
                                    ret.second.count() / 60.)
                  << std::endl;
        // wait for all the cache operations for prepopulation to complete.

        //std::cout << "== Cache Stats ==" << std::endl;
        //cache_->getStats().render(std::cout);
        std::cout << std::endl;
        int l;
      } catch (const cachebench::EndOfTrace& ex) {
        std::cout << "End of trace encountered during prepopulaiton, "
                     "attempting to continue..."
                  << std::endl;
      }
    }
  }

  ~MicroStressor() override { finish(); }

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
        conn * c = conn_new(i,0,0,1024,0,main_base,NULL);
        bind_thread_helper(c, i);
        workers.push_back(
            std::thread([this, throughputStats = &throughputStats_.at(i), c=c]() {
              stressByDiscreteDistribution(*throughputStats, c);
            }));
      }

      for (auto& worker : workers) {
        worker.join();
      }

      testDurationNs_ =
          std::chrono::nanoseconds{std::chrono::system_clock::now() -
                                   startTime_}
              .count();
      stop_memcached_loop_ = true;
      std::cout << "waiting for mc" << std::endl;
      memcached_loop_.join();
    });
  }

  // Block until all stress workers are finished.
  void finish() override {
    if (stressWorker_.joinable()) {
      stressWorker_.join();
    }
    if (memcached_running_  && stop_memcached() == EXIT_SUCCESS){
        memcached_running_ = false;
    }
  }

  std::chrono::time_point<std::chrono::system_clock> startTime()
      const override {
    return startTime_;
  }

  Stats getCacheStats() const override { Stats tmp; return tmp;}

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

  // Fill the cache and make sure all pools are full.
  // It's the generator's resposibility of providing keys that obey to the
  // distribution
  std::pair<size_t, std::chrono::seconds> prepopulateCache() {
    std::atomic<uint64_t> totalCount = 0;

    auto prePopulateFn = [&](uint32_t wid) {
      conn * c = conn_new(wid,0,0,1024,0,main_base,NULL);
      bind_thread_helper(c, wid);

      std::mt19937 gen(folly::Random::rand32());
      std::discrete_distribution<> keyPoolDist(
          config_.keyPoolDistribution.begin(),
          config_.keyPoolDistribution.end());
      size_t count = 0;
      std::unordered_set<PoolId> fullPools;
      std::optional<uint64_t> lastRequestId = std::nullopt;

      // in some cases eviction will never happen. To avoid infinite loop in
      // this case, we use continuousCacheHits as the signal of cache full
      size_t continuousCacheHits;
      while (true) {
        // get a pool according to keyPoolDistribution
        const Request& req = getReq(wid%config_.opPoolDistribution.size(), gen, lastRequestId);
        item * mc_it =process_cachebench_get_or_touch(&req.key.at(0), req.key.size(), c, PROTOCOL_BINARY_CMD_GET);
        if (mc_it) {
          // Treat 1000 continuous cache hits as cache full
          if (++continuousCacheHits > 1000) {
            break;
          }

          if (req.requestId) {
            wg_->notifyResult(*req.requestId, OpResultType::kGetHit);
          }
          continue;
        }

        // cache miss. Allocate and write to cache
        continuousCacheHits = 0;

        // req contains a new key, set it to cache
        item * memcachedItem = process_cachebench_set(&req.key.at(0), req.key.size(), &hardcodedString_.at(0), *(req.sizeBegin) + 2, c, PROTOCOL_BINARY_CMD_SET);
        if (! memcachedItem){
            std::cout << "mc OOM" << std::endl;
            break;
        }
        count++;
        if(count > .1*config_.numKeys){
            //TODO fix prepop to be more clever
            break;
        }
        if (req.requestId) {
          wg_->notifyResult(*req.requestId, OpResultType::kSetSuccess);
        }
        }
      totalCount.fetch_add(count);
    };

    auto numThreads = config_.prepopulateThreads ? config_.prepopulateThreads
                                                 : config_.numThreads;
    auto duration = detail::executeParallelWid(prePopulateFn, numThreads);
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
  void stressByDiscreteDistribution(ThroughputStats& stats, conn * c) {
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
         i < config_.numOps && !shouldTestStop();
         ++i) {
      try {
        ++stats.ops;

        const auto pid = opPoolDist(gen);
        const Request& req(getReq(pid, gen, lastRequestId));
        std::string* key = &(req.key);
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
          result = setKey(pid, stats, key, *(req.sizeBegin), c);
          throttleFn();
          break;
        }
        case OpType::kAddChained:
        case OpType::kLoneGet:
        case OpType::kGet: {
          ++stats.get;

          Mode lockMode = Mode::Shared;

          // TODO currently pure lookaside, we should
          // add a distribution over sequences of requests/access patterns
          // e.g. get-no-set and set-no-get
          item * mc_it = process_cachebench_get_or_touch(&req.key.at(0), req.key.size(), c, PROTOCOL_BINARY_CMD_GET);

          if (mc_it == nullptr) {
            ++stats.getMiss;
            result = OpResultType::kGetMiss;

            if (config_.enableLookaside) {
              // allocate and insert on miss
              // upgrade access privledges, (lock_upgrade is not
              // appropriate here)
              setKey(pid, stats, key, *(req.sizeBegin), c);
            }
          } else {
            result = OpResultType::kGetHit;
          }

          throttleFn();
          break;
        }
        case OpType::kDel: {
          //TODO implement deletes, use the model below
          ++stats.del;
          //auto res = cache_->remove(*key);
          //if (res == CacheT::RemoveRes::kNotFoundInRam) {
          //  ++stats.delNotFound;
          //}
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
                      std::string* key,
                      size_t size,
                      conn * c) {
    ++stats.set;
    item * memcachedItem = process_cachebench_set(&key->at(0), key->size(), &hardcodedString_.at(0), size + 2, c, PROTOCOL_BINARY_CMD_SET);

    if ( memcachedItem == NULL ) {
      ++stats.setFailure;
      return OpResultType::kSetFailure;
    } else {
      return OpResultType::kSetSuccess;
    }
  }

  const Request& getReq(const PoolId& pid,
                        std::mt19937& gen,
                        std::optional<uint64_t>& lastRequestId) {
      const Request& req(wg_->getReq(pid, gen, lastRequestId));
      return req;
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

  std::string hardcodedString_;


  std::thread stressWorker_;

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  uint64_t testDurationNs_{0};
  bool memcached_running_{false};
  bool stop_memcached_loop_{false};
  std::thread memcached_loop_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
