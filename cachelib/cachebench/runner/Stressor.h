#pragma once

#include <folly/Benchmark.h>

#include <atomic>
#include <memory>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
struct ThroughputStats {
  std::string name;
  uint64_t set{0};
  uint64_t setFailure{0};
  uint64_t get{0};
  uint64_t getMiss{0};
  uint64_t del{0};
  uint64_t update{0};
  uint64_t updateMiss{0};
  uint64_t delNotFound{0};
  uint64_t addChained{0};
  uint64_t addChainedFailure{0};
  // current number of ops executed. Read periodically to track progress
  uint64_t ops{0};

  ThroughputStats& operator+=(const ThroughputStats& other);

  void render(uint64_t elapsedTimeNs, std::ostream& out) const;
  void render(uint64_t, folly::UserCounters&) const;
};

class GeneratorBase;

// The class defines the admission policy at stressor level. The stressor
// checks the admission policy first before inserting an item into cache.
//
// This base class always returns true, allowing the insersion.
class StressorAdmPolicy {
 public:
  virtual ~StressorAdmPolicy() = default;

  virtual bool accept(
      const std::unordered_map<std::string, std::string>& /*featureMap*/) {
    return true;
  }
};

class Stressor {
 public:
  static std::unique_ptr<Stressor> makeStressor(
      CacheConfig cacheConfig,
      StressorConfig stressorConfig,
      std::unique_ptr<StressorAdmPolicy> admPolicy);

  virtual ~Stressor() {}

  virtual Stats getCacheStats() const = 0;
  virtual ThroughputStats aggregateThroughputStats() const = 0;
  virtual void renderWorkloadGeneratorStats(uint64_t /*elapsedTimeNs*/,
                                            std::ostream& /*out*/) const {}
  virtual void renderWorkloadGeneratorStats(
      uint64_t /*elapsedTimeNs*/, folly::UserCounters& /*counters*/) const {}
  virtual std::chrono::time_point<std::chrono::system_clock> startTime()
      const = 0;
  virtual uint64_t getTestDurationNs() const = 0;

  virtual void start() = 0;
  virtual void finish() = 0;
  virtual void abort() { stopTest(); }

 protected:
  // check whether the load test should stop. e.g. user interrupt the
  // cachebench.
  bool shouldTestStop() { return stopped_.load(std::memory_order_acquire); }

  // Called when stop request from user is captured. instead of stop the load
  // test immediately, the method sets the state "stopped_" to true. Actual
  // stop logic is in somewhere else.
  void stopTest() { stopped_.store(true, std::memory_order_release); }

 private:
  std::atomic<bool> stopped_{false};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
