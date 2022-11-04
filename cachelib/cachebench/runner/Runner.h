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

#pragma once

#include <folly/Benchmark.h>

#include <string>

#include "cachelib/cachebench/runner/ProgressTracker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// A simple wrapper to maintain the stress run through an opaque stressor
// instance.
class Runner {
 public:
  // @param config                the configuration for the cachebench run. This
  //                              contains both the stressor configuration and
  //                              the cache configuration.
  Runner(const CacheBenchConfig& config);

  // @param progressInterval    the interval at which periodic progress of the
  //                            benchmark run is reported/tracked.
  // @param progressStatsFile   the file to log periodic stats and progress
  //                            to in addition to stdtout. Ignored if empty
  // @return true if the run was successful, false if there is a failure.
  bool run(std::chrono::seconds progressInterval,
           const std::string& progressStatsFile);

  // for testings using folly::Benchmark
  // in addition to running time, cachebench has several metrics
  // (hit rate, throughput, ect.) to be compared, use BENCHMARK_COUNTER
  // and put metrics into folly::UserCounters to show metrics in output results.
  void run(folly::UserCounters&);

  void abort() {
    if (stressor_) {
      stressor_->abort();
    }
  }

 private:
  // instance of the stressor.
  std::unique_ptr<Stressor> stressor_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
