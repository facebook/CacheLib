#pragma once

#include <folly/Range.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/stats/QuantileEstimator.h>
#pragma GCC diagnostic pop
#include <folly/logging/xlog.h>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace util {
using CounterVisitor =
    std::function<void(folly::StringPiece name, double count)>;

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

  PercentileStats() : estimator_{std::chrono::seconds{kDefaultWindowSize}} {}
  PercentileStats(std::chrono::seconds windowSize) : estimator_{windowSize} {}

  // track latency by taking the value of duration directly.
  void trackValue(double value) { estimator_.addValue(value); }

  // Return the estimates for stat. This is not cheap so do not
  // call frequently. The cost is roughly number of quantiles we
  // pass in multiplied by cost of estimating an individual quantile
  Estimates estimate();

  // visit each latency estimate using the visitor.
  // @param visitor   the stat visitor
  // @param rst       the estimates to be visited
  // @param prefix    prefix for the stat name.
  void visitQuantileEstimator(const CounterVisitor& visitor,
                              folly::StringPiece statPrefix) {
    auto rst = estimate();
    visitQuantileEstimates(visitor, rst, statPrefix);
  }

  // visit each latency estimate using the visitor.
  // @param visitor   the stat visitor
  // @param rst       the estimates to be visited
  // @param prefix    prefix for the stat name.
  static void visitQuantileEstimates(const CounterVisitor& visitor,
                                     const Estimates& rst,
                                     folly::StringPiece prefix);

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
      auto diffNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - begin_)
                           .count();
      stats_->trackValue(static_cast<double>(diffNanos));
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
