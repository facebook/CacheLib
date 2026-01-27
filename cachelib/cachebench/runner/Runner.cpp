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

#include "cachelib/cachebench/runner/Runner.h"

#include "cachelib/cachebench/runner/ProgressTracker.h"
#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
Runner::Runner(size_t instanceId, const CacheBenchConfig& config)
    : instanceId_(instanceId),
      stressor_{Stressor::makeStressor(config.getCacheConfig(instanceId),
                                       config.getStressorConfig(instanceId))} {}

bool Runner::run(std::chrono::seconds progressInterval,
                 const std::string& progressStatsFile,
                 bool alsoPrintResultsToConsole) {
  ProgressTracker tracker{instanceId_, *stressor_, progressStatsFile};

  stressor_->start();

  if (!tracker.start(progressInterval)) {
    throw std::runtime_error("Cannot start ProgressTracker.");
  }

  stressor_->finish();

  durationNs_ = stressor_->getTestDurationNs();
  cacheStats_ = stressor_->getCacheStats();
  opsStats_ = stressor_->aggregateThroughputStats();
  tracker.stop();

  bool passed = true;
  if (progressStatsFile.empty()) {
    passed = render(std::cout);
  } else {
    std::ofstream ofs{progressStatsFile, std::ios::app};
    passed = render(ofs);
    if (alsoPrintResultsToConsole) {
      render(std::cout);
    }
  }

  stressor_.reset();

  if (aborted_) {
    std::cerr << "Test aborted.\n";
    passed = false;
  }
  return passed;
}

bool Runner::run(folly::UserCounters& counters) {
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

  if (aborted_) {
    std::cerr << "Test aborted.\n";
    return false;
  }
  return true;
}

bool Runner::render(std::ostream& os) {
  os << "== Test Results ==\n== Allocator Stats ==" << std::endl;
  cacheStats_.render(os);

  os << "\n== Throughput Stats ==\n";
  opsStats_.render(durationNs_, os);

  stressor_->renderWorkloadGeneratorStats(durationNs_, os);
  os << std::endl;

  return cacheStats_.renderIsTestPassed(os);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
