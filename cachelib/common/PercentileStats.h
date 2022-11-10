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

#include <folly/Range.h>

#include <stdexcept>
#include <utility>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/stats/QuantileEstimator.h>
#pragma GCC diagnostic pop
#include <folly/logging/xlog.h>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace util {

class PercentileStats {
 public:
  struct Estimates {
    uint64_t avg;
    uint64_t p0;
    uint64_t p5;
    uint64_t p10;
    uint64_t p25;
    uint64_t p50;
    uint64_t p75;
    uint64_t p90;
    uint64_t p95;
    uint64_t p99;
    uint64_t p999;
    uint64_t p9999;
    uint64_t p99999;
    uint64_t p999999;
    uint64_t p100;
  };

  // visit each latency estimate using the visitor.
  // @param visitor   the stat visitor
  // @param rst       the estimates to be visited
  // @param prefix    prefix for the stat name.
  static void visitQuantileEstimates(const CounterVisitor& visitor,
                                     const Estimates& rst,
                                     folly::StringPiece prefix);

  static void visitQuantileEstimates(
      const std::function<void(folly::StringPiece, double)>& visitor,
      const Estimates& rst,
      folly::StringPiece prefix) {
    visitQuantileEstimates(CounterVisitor(visitor), rst, prefix);
  }

  PercentileStats() : estimator_{std::chrono::seconds{kDefaultWindowSize}} {}
  PercentileStats(std::chrono::seconds windowSize) : estimator_{windowSize} {}

  // track latency by taking the value of duration directly.
  void trackValue(double value,
                  std::chrono::time_point<std::chrono::steady_clock> tp =
                      std::chrono::steady_clock::now()) {
    estimator_.addValue(value, tp);
  }

  // Return the estimates for stat. This is not cheap so do not
  // call frequently. The cost is roughly number of quantiles we
  // pass in multiplied by cost of estimating an individual quantile
  Estimates estimate();

  void visitQuantileEstimator(
      const std::function<void(folly::StringPiece, double)>& visitor,
      folly::StringPiece statPrefix) {
    visitQuantileEstimator(CounterVisitor{visitor}, statPrefix);
  }

  // visit each latency estimate using the visitor.
  // @param visitor   the stat visitor
  // @param rst       the estimates to be visited
  // @param prefix    prefix for the stat name.
  void visitQuantileEstimator(const CounterVisitor& visitor,
                              folly::StringPiece statPrefix) {
    auto rst = estimate();
    visitQuantileEstimates(visitor, rst, statPrefix);
  }

 private:
  static const std::array<double, 14> kQuantiles;
  static constexpr int kDefaultWindowSize = 1;

  folly::SlidingWindowQuantileEstimator<> estimator_;
};

class LatencyTracker {
 public:
  explicit LatencyTracker(PercentileStats& stats)
      : stats_(&stats), begin_(std::chrono::steady_clock::now()) {}
  LatencyTracker() {}
  ~LatencyTracker() {
    if (stats_) {
      auto tp = std::chrono::steady_clock::now();
      auto diffNanos =
          std::chrono::duration_cast<std::chrono::nanoseconds>(tp - begin_)
              .count();
      stats_->trackValue(static_cast<double>(diffNanos), tp);
    }
  }

  LatencyTracker(const LatencyTracker&) = delete;
  LatencyTracker& operator=(const LatencyTracker&) = delete;

  LatencyTracker(LatencyTracker&& rhs) noexcept
      : stats_(rhs.stats_), begin_(rhs.begin_) {
    rhs.stats_ = nullptr;
  }

  LatencyTracker& operator=(LatencyTracker&& rhs) noexcept {
    if (this != &rhs) {
      this->~LatencyTracker();
      new (this) LatencyTracker(std::move(rhs));
    }
    return *this;
  }

 private:
  PercentileStats* stats_{nullptr};
  std::chrono::time_point<std::chrono::steady_clock> begin_;
};
} // namespace util
} // namespace cachelib
} // namespace facebook
