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

void ProgressTracker::work() {
  auto now = std::chrono::system_clock::now();
  auto nowTimeT = std::chrono::system_clock::to_time_t(now);
  char buf[16];
  struct tm time;
  ::localtime_r(&nowTimeT, &time);
  ::strftime(buf, sizeof(buf), "%H:%M:%S", &time);
  auto throughputStats = stressor_.aggregateThroughputStats();
  auto mOpsPerSec = throughputStats.ops / 1e6;

  const auto currCacheStats = stressor_.getCacheStats();
  auto [overallHitRatio, ramHitRatio, nvmHitRatio] =
      currCacheStats.getHitRatios(prevStats_);
  auto thStr = folly::sformat(
      "{} {:>10.2f}M ops completed. Hit Ratio {:6.2f}% "
      "(RAM {:6.2f}%, NVM {:6.2f}%)",
      buf,
      mOpsPerSec,
      overallHitRatio,
      ramHitRatio,
      nvmHitRatio);

  // log this always to stdout
  std::cout << thStr << std::endl;

  // additionally log into the stats file
  if (statsFile_.is_open()) {
    statsFile_ << thStr << std::endl;
    statsFile_ << "== Allocator Stats ==" << std::endl;
    currCacheStats.render(statsFile_);

    statsFile_ << "== Hit Ratio Stats Since Last ==" << std::endl;
    currCacheStats.render(prevStats_, statsFile_);

    statsFile_ << "== Throughput Stats ==" << std::endl;
    auto elapsedTimeNs = stressor_.getTestDurationNs();
    throughputStats.render(elapsedTimeNs, statsFile_);

    stressor_.renderWorkloadGeneratorStats(elapsedTimeNs, statsFile_);
    statsFile_ << std::endl;
  }

  prevStats_ = currCacheStats;
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
