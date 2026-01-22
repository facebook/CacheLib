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

#include "cachelib/cachebench/cache/CacheStats.h"
#include "cachelib/cachebench/runner/Runner.h"
#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook::cachelib::cachebench {

struct AggregatedStats {
  // Aggregate stats across a vector of runners
  explicit AggregatedStats(const std::vector<Runner>& runners);

  ThroughputStats opsStats_;
  Stats cacheStats_;
  uint64_t durationNs_{0};
  size_t numInstances_{0};
};

} // namespace facebook::cachelib::cachebench

namespace std {
// Print aggregated stats to the given ostream
std::ostream& operator<<(
    std::ostream& os,
    const facebook::cachelib::cachebench::AggregatedStats& stats);
} // namespace std
