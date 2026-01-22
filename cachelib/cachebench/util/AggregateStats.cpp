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

#include "cachelib/cachebench/util/AggregateStats.h"

namespace facebook::cachelib::cachebench {

AggregatedStats::AggregatedStats(const std::vector<Runner>& runners)
    : numInstances_(runners.size()) {
  for (size_t i = 0; i < runners.size(); i++) {
    opsStats_ += runners[i].getThroughputStats();
    cacheStats_ += runners[i].getCacheStats();
    durationNs_ = std::max(durationNs_, runners[i].getTestDurationNs());
  }
}

} // namespace facebook::cachelib::cachebench

namespace std {
// Print aggregated stats to the given ostream
std::ostream& operator<<(
    std::ostream& os,
    const facebook::cachelib::cachebench::AggregatedStats& stats) {
  os << std::endl
     << "== Aggregated Stats Across " << stats.numInstances_
     << " Instances ==" << std::endl
     << "== Allocator Stats ==" << std::endl;
  stats.cacheStats_.render(os, /* isAggregate */ true);

  os << std::endl << "== Throughput ==" << std::endl;
  const double elapsedSecs = stats.durationNs_ / static_cast<double>(1e9);
  os << folly::sformat("Total Duration: {:.2f} seconds", elapsedSecs)
     << std::endl;
  stats.opsStats_.render(stats.durationNs_, os);
  os << std::endl
     << "NOTE: we can't combine latency counters across multiple instances, "
        "see each instance's output for latency stats"
     << std::endl;
  return os;
}
} // namespace std
