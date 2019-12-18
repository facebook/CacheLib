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

class PercentileStats {
 public:
  struct Estimates {
    uint64_t avg;
    uint64_t p10;
    uint64_t p25;
    uint64_t p50;
    uint64_t p75;
    uint64_t p90;
    uint64_t p95;
    uint64_t p99;
    uint64_t p999;
    uint64_t p100;
  };

  PercentileStats() : estimator_{std::chrono::seconds{kDefaultWindowSize}} {}

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
            static_cast<uint64_t>(result.quantiles[8].second)};
  }

 private:
  static const std::array<double, 9> kQuantiles;
  static constexpr int kDefaultWindowSize = 1;

  folly::SlidingWindowQuantileEstimator<> estimator_;
};

// wrapper around percentile stats that assumes that the value tracked is
// cpu cycles
class LatencyStats : public PercentileStats {
 public:
  LatencyStats() = default;

  PercentileStats::Estimates estimate() {
    auto ret = PercentileStats::estimate();
    return Estimates{util::getTimeNsFromCycles(ret.avg),
                     util::getTimeNsFromCycles(ret.p10),
                     util::getTimeNsFromCycles(ret.p25),
                     util::getTimeNsFromCycles(ret.p50),
                     util::getTimeNsFromCycles(ret.p75),
                     util::getTimeNsFromCycles(ret.p90),
                     util::getTimeNsFromCycles(ret.p95),
                     util::getTimeNsFromCycles(ret.p99),
                     util::getTimeNsFromCycles(ret.p999),
                     util::getTimeNsFromCycles(ret.p100)};
  }
};

class LatencyTracker {
 public:
  explicit LatencyTracker(LatencyStats& stats)
      : stats_(&stats), cyclesBegin_(util::cpuCycleCounter()) {}
  ~LatencyTracker() {
    if (stats_) {
      uint64_t cycles = util::cpuCycleCounter() - cyclesBegin_;
      stats_->trackValue(cycles);
    }
  }

  LatencyTracker(const LatencyTracker&) = delete;
  LatencyTracker& operator=(const LatencyTracker&) = delete;

  LatencyTracker(LatencyTracker&& rhs) noexcept
      : stats_(rhs.stats_), cyclesBegin_(rhs.cyclesBegin_) {
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
  LatencyStats* stats_{nullptr};
  const uint64_t cyclesBegin_{};
};
} // namespace util
} // namespace cachelib
} // namespace facebook
