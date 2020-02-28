#include "cachelib/cachebench/runner/ProgressTracker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
ProgressTracker::ProgressTracker(const Stressor& s,
                                 const std::string& detailedStatsFile)
    : stressor_(s) {
  if (!detailedStatsFile.empty()) {
    statsFile_.open(detailedStatsFile, std::ios::app);
  }
}

ProgressTracker::~ProgressTracker() {
  try {
    if (statsFile_.is_open()) {
      statsFile_.close();
    }
    stop();
  } catch (const std::exception&) {
  }
}

void ProgressTracker::reportProgress() {
  auto now = std::chrono::system_clock::now();
  auto nowTimeT = std::chrono::system_clock::to_time_t(now);
  char buf[16];
  struct tm time;
  ::localtime_r(&nowTimeT, &time);
  ::strftime(buf, sizeof(buf), "%H:%M:%S", &time);
  auto throughputStats = stressor_.aggregateThroughputStats();
  auto mOpsPerSec = throughputStats.ops / 1e6;

  auto thStr = folly::sformat("{} {:>10.2f}M ops completed", buf, mOpsPerSec);

  // log this always to stdout
  std::cout << thStr << std::endl;

  // additionally log into the stats file
  if (statsFile_.is_open()) {
    statsFile_ << thStr << std::endl;
    statsFile_ << "== Allocator Stats ==" << std::endl;
    const auto currCacheStats = stressor_.getCacheStats();
    currCacheStats.render(statsFile_);

    statsFile_ << "== Throughput Stats ==" << std::endl;
    auto elapsedTimeNs =
        std::chrono::nanoseconds{now - stressor_.startTime()}.count();
    throughputStats.render(elapsedTimeNs, statsFile_);

    stressor_.renderWorkloadGeneratorStats(elapsedTimeNs, statsFile_);
    statsFile_ << std::endl;
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
