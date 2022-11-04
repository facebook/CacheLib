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

#include "cachelib/common/PercentileStats.h"

namespace facebook {
namespace cachelib {
namespace util {
const std::array<double, 14> PercentileStats::kQuantiles{
    0,    0.05, 0.1,   0.25,   0.5,     0.75,     0.9,
    0.95, 0.99, 0.999, 0.9999, 0.99999, 0.999999, 1.0};
constexpr int PercentileStats::kDefaultWindowSize;

PercentileStats::Estimates PercentileStats::estimate() {
  estimator_.flush();

  auto result = estimator_.estimateQuantiles(
      folly::Range<const double*>{kQuantiles.begin(), kQuantiles.end()});
  XDCHECK_EQ(kQuantiles.size(), result.quantiles.size());
  if (result.count == 0) {
    return {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  }
  return {static_cast<uint64_t>(result.sum / result.count),
          static_cast<uint64_t>(result.quantiles[0].second),
          static_cast<uint64_t>(result.quantiles[1].second),
          static_cast<uint64_t>(result.quantiles[2].second),
          static_cast<uint64_t>(result.quantiles[3].second),
          static_cast<uint64_t>(result.quantiles[4].second),
          static_cast<uint64_t>(result.quantiles[5].second),
          static_cast<uint64_t>(result.quantiles[6].second),
          static_cast<uint64_t>(result.quantiles[7].second),
          static_cast<uint64_t>(result.quantiles[8].second),
          static_cast<uint64_t>(result.quantiles[9].second),
          static_cast<uint64_t>(result.quantiles[10].second),
          static_cast<uint64_t>(result.quantiles[11].second),
          static_cast<uint64_t>(result.quantiles[12].second),
          static_cast<uint64_t>(result.quantiles[13].second)};
}

void PercentileStats::visitQuantileEstimates(const CounterVisitor& visitor,
                                             const Estimates& rst,
                                             folly::StringPiece prefix) {
  constexpr folly::StringPiece fmt = "{}_{}";
  visitor(folly::sformat(fmt, prefix, "avg"), static_cast<double>(rst.avg));
  visitor(folly::sformat(fmt, prefix, "min"), static_cast<double>(rst.p0));
  visitor(folly::sformat(fmt, prefix, "p5"), static_cast<double>(rst.p5));
  visitor(folly::sformat(fmt, prefix, "p10"), static_cast<double>(rst.p10));
  visitor(folly::sformat(fmt, prefix, "p25"), static_cast<double>(rst.p25));
  visitor(folly::sformat(fmt, prefix, "p50"), static_cast<double>(rst.p50));
  visitor(folly::sformat(fmt, prefix, "p75"), static_cast<double>(rst.p75));
  visitor(folly::sformat(fmt, prefix, "p90"), static_cast<double>(rst.p90));
  visitor(folly::sformat(fmt, prefix, "p95"), static_cast<double>(rst.p95));
  visitor(folly::sformat(fmt, prefix, "p99"), static_cast<double>(rst.p99));
  visitor(folly::sformat(fmt, prefix, "p999"), static_cast<double>(rst.p999));
  visitor(folly::sformat(fmt, prefix, "p9999"), static_cast<double>(rst.p9999));
  visitor(folly::sformat(fmt, prefix, "p99999"),
          static_cast<double>(rst.p99999));
  visitor(folly::sformat(fmt, prefix, "p999999"),
          static_cast<double>(rst.p999999));
  visitor(folly::sformat(fmt, prefix, "max"), static_cast<double>(rst.p100));
}
} // namespace util
} // namespace cachelib
} // namespace facebook
