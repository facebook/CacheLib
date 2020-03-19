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
  Estimates estimate() {
    estimator_.flush();

    auto result = estimator_.estimateQuantiles(
        folly::Range<const double*>{kQuantiles.begin(), kQuantiles.end()});
    XDCHECK_EQ(kQuantiles.size(), result.quantiles.size());
    return {static_cast<uint64_t>(
                result.count == 0 ? 0 : result.sum / result.count),
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

  void visitQuantileEstimator(const CounterVisitor& visitor,
                              folly::StringPiece fmt,
                              folly::StringPiece prefix) {
    auto rst = estimate();
    visitor(folly::sformat(fmt, prefix, "min"), rst.p0);
    visitor(folly::sformat(fmt, prefix, "p5"), rst.p5);
    visitor(folly::sformat(fmt, prefix, "p10"), rst.p10);
    visitor(folly::sformat(fmt, prefix, "p25"), rst.p25);
    visitor(folly::sformat(fmt, prefix, "p50"), rst.p50);
    visitor(folly::sformat(fmt, prefix, "p75"), rst.p75);
    visitor(folly::sformat(fmt, prefix, "p90"), rst.p90);
    visitor(folly::sformat(fmt, prefix, "p95"), rst.p95);
    visitor(folly::sformat(fmt, prefix, "p99"), rst.p99);
    visitor(folly::sformat(fmt, prefix, "p999"), rst.p999);
    visitor(folly::sformat(fmt, prefix, "p9999"), rst.p9999);
    visitor(folly::sformat(fmt, prefix, "p99999"), rst.p99999);
    visitor(folly::sformat(fmt, prefix, "p999999"), rst.p999999);
    visitor(folly::sformat(fmt, prefix, "max"), rst.p100);
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
  ~LatencyTracker() {
    if (stats_) {
      auto diffNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - begin_)
                           .count();
      stats_->trackValue(diffNanos);
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
