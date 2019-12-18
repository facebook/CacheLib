#pragma once

#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class FastShutdownStressor : public Stressor {
 public:
  explicit FastShutdownStressor(CacheConfig cacheConfig, uint64_t numOps);
  Stats getCacheStats() const override {
    return cache_->getStats();
    return Stats{};
  }
  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats stats;
    stats.ops = ops_;
    return stats;
  }
  std::chrono::time_point<std::chrono::system_clock> startTime()
      const override {
    return startTime_;
  }

  uint64_t getTestDurationNs() const override {
    return std::chrono::nanoseconds{endTime_ - startTime_}.count();
  }

  void start() override;
  void finish() override { cache_->cleanupSharedMem(); }

 private:
  const uint64_t numOpsPerThread_{};
  std::unique_ptr<Cache<LruAllocator>> cache_;
  std::atomic<uint64_t> ops_{0};

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  std::thread testThread_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
