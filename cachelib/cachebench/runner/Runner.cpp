#include "cachelib/cachebench/runner/Runner.h"

#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
Runner::Runner(const CacheBenchConfig& config,
               std::unique_ptr<StressorAdmPolicy> admPolicy)
    : stressor_{Stressor::makeStressor(config.getCacheConfig(),
                                       config.getStressorConfig(),
                                       std::move(admPolicy))} {}

bool Runner::run(std::chrono::seconds progressInterval,
                 const std::string& progressStatsFile) {
  ProgressTracker tracker{*stressor_, progressStatsFile};

  stressor_->start();

  if (!tracker.start(progressInterval)) {
    throw std::runtime_error("Cannot start ProgressTracker.");
  }

  stressor_->finish();

  uint64_t durationNs = stressor_->getTestDurationNs();
  auto cacheStats = stressor_->getCacheStats();
  auto opsStats = stressor_->aggregateThroughputStats();
  tracker.stop();

  std::cout << "== Test Results ==\n== Allocator Stats ==" << std::endl;
  cacheStats.render(std::cout);

  std::cout << "\n== Throughput for  ==\n";
  opsStats.render(durationNs, std::cout);

  stressor_->renderWorkloadGeneratorStats(durationNs, std::cout);
  std::cout << std::endl;

  stressor_.reset();
  return cacheStats.renderIsTestPassed(std::cout);
}

void Runner::run(folly::UserCounters& counters) {
  stressor_->start();
  stressor_->finish();

  BENCHMARK_SUSPEND {
    uint64_t durationNs = stressor_->getTestDurationNs();
    auto cacheStats = stressor_->getCacheStats();
    auto opsStats = stressor_->aggregateThroughputStats();

    // Allocator Stats
    cacheStats.render(counters);

    // Throughput
    opsStats.render(durationNs, counters);

    stressor_->renderWorkloadGeneratorStats(durationNs, counters);

    counters["nvm_disable"] = cacheStats.isNvmCacheDisabled ? 100 : 0;
    counters["inconsistency_count"] = cacheStats.inconsistencyCount * 100;

    stressor_.reset();
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
