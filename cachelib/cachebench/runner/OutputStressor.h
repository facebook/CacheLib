#pragma once

#include <ctime>
#include <fstream>
#include <iostream>

#include <folly/Random.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/runner/TestStopper.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"

#include "cachelib/cachebench/workload/OnlineGenerator.h"
#include "cachelib/cachebench/workload/ReplayGenerator.h"
#include "cachelib/cachebench/workload/WorkloadGenerator.h"

// Stressor class which sets up the underlying distributions but then dumps
// the resulting traffic to stdout instead of sending requests to the cache.
// This is good for analyzing generated traffic, allowing comparison
// between models and against production traces.
// The Output format is:
//      key (fbid),op,size
// and fbid is left blank in the event of a one hit wonder
namespace facebook {
namespace cachelib {
namespace cachebench {
template <typename Allocator, typename Generator = WorkloadGenerator<>>
class OutputStressor : public Stressor {
 public:
  using CacheT = Cache<Allocator>;
  using Key = typename CacheT::Key;
  using ItemHandle = typename CacheT::ItemHandle;

  enum class Mode { Exclusive, Shared };

  // @param config    stress test config
  OutputStressor(CacheConfig, StressorConfig config)
      : config_(std::move(config)),
        throughputStats_(config_.numThreads),
        wg_(config_) {
    outfile_.open(config_.traceFileName);
    if (outfile_.fail()) {
      throw std::invalid_argument(folly::sformat(
          "Cannot open output location {}", config_.traceFileName));
    }
  }

  ~OutputStressor() override {
    finish();
    outfile_.close();
  }

  // Start the stress test
  // We first launch a stats worker that will wake up periodically
  // Then we start another worker thread that will be responsible for
  // spawning all the stress test threads
  void start() override {
    startTime_ = std::chrono::system_clock::now();
    stressWorker_ = std::thread([this] {
      std::cout << folly::sformat("Total {:.2f}M ops to be run",
                                  config_.numThreads * config_.numOps / 1e6)
                << std::endl;

      std::vector<std::thread> workers;
      outfile_ << "fbid,op,size" << std::endl;
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

  Stats getCacheStats() const override { return Stats{}; }

 private:
  void stressByDiscreteDistribution(ThroughputStats& stats) {
    wg_.registerThread();
    std::mt19937 gen(folly::Random::rand32());
    std::discrete_distribution<> opPoolDist(config_.opPoolDistribution.begin(),
                                            config_.opPoolDistribution.end());
    for (uint64_t i = 0; i < config_.numOps; ++i) {
      ++stats.ops;

      const auto pid = opPoolDist(gen);
      while (true) {
        const auto& req = getReq(pid, gen);
        const OpType op = wg_.getOp(pid, gen);
        outfile_ << req.key << "," << static_cast<uint32_t>(op) << ","
                 << *(req.sizeBegin) << std::endl;
      }
    }
  }

  const Request& getReq(const PoolId& pid, std::mt19937& gen) {
    return wg_.getReq(pid, gen);
  }

  const StressorConfig config_;
  std::vector<ThroughputStats> throughputStats_;
  Generator wg_;
  std::thread stressWorker_;
  std::chrono::time_point<std::chrono::system_clock> startTime_;
  uint64_t testDurationNs_{0};
  std::ofstream outfile_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
