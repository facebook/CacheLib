#include "cachelib/cachebench/runner/Runner.h"

#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
Runner::Runner(const std::string& configPath,
               const std::string& progressStatsFile,
               uint64_t progressInterval)
    : config_{configPath},
      progressStatsFile_{progressStatsFile},
      progressInterval_{progressInterval} {}

bool Runner::run() {
  stressor_ =
      Stressor::makeStressor(config_.getCacheConfig(), config_.getTestConfig());
  ProgressTracker tracker{*stressor_, progressStatsFile_};

  stressor_->start();

  if (!tracker.start(std::chrono::seconds{progressInterval_})) {
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

  if (cacheStats.isNvmCacheDisabled) {
    std::cout << "NVM Cache was disabled during test!" << std::endl;
  }

  if (cacheStats.inconsistencyCount) {
    std::cout << "Found " << cacheStats.inconsistencyCount
              << " inconsistent cases" << std::endl;
  }

  stressor_.reset();
  return cacheStats.inconsistencyCount == 0 && !cacheStats.isNvmCacheDisabled;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
