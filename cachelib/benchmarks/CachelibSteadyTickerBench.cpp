// Benchmark for measuring SteadyClockTimeStampTicker vs. inline steady_clock.
// We want to use this benchmark to decide how we may implment a clock whose
// behavior can be changed under differnt setup (simulation vs. proudction). The
// baseline is the util::getSteadyCurrentTimeSec().
// The first choice is to wrap
// this function in a class and provide an alternate implementation in
// simulation.
// The second choice is to use a if-statement and use the inline function in
// proudction.
// We also bench std::time.
// In the benchmark evaluation, the second choice is more acceptable.
// ============================================================================
// cachelib/benchmarks/CachelibSteadyTickerBench.cpprelative  time/iter  iters/s
// ============================================================================
// inline_steady_clock                                          10.66s   93.81m
// steady_clock_ticker                               86.20%     12.37s   80.86m
// inline_steady_clock_in_branch                     99.37%     10.73s   93.22m
// benchInlineClock                                 746.74%      1.43s  700.51m
// ============================================================================

#include <folly/Benchmark.h>

#include "cachelib/common/Ticker.h"
#include "cachelib/common/Time.h"

DEFINE_int32(num_ops, 500 * 1000 * 1000, "number of operations");

namespace facebook {
namespace cachelib {

namespace detail {
class SteadyClockBasedTicker : public Ticker {
 public:
  uint32_t getCurrentTick() override { return util::getSteadyCurrentTimeSec(); }
};
} // namespace detail

void benchInlineSteadyClock() {
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    ts = util::getSteadyCurrentTimeSec();
  }
  folly::doNotOptimizeAway(ts);
}

void benchSteakyClockTicker() {
  auto ticker = std::make_shared<detail::SteadyClockBasedTicker>();
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    ts = ticker->getCurrentTick();
  }
  folly::doNotOptimizeAway(ts);
}

// Simulate the production performance when we use the inline function as the
// clock, while leaving the option to use the ticker as the clock in simulation.
void benchSteadyClockInBranch() {
  // In prod, the ticker object is always null.
  std::shared_ptr<detail::SteadyClockBasedTicker> ticker = nullptr;
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    if (ticker) {
      ts = ticker->getCurrentTick();
    } else {
      ts = util::getSteadyCurrentTimeSec();
    }
  }
  folly::doNotOptimizeAway(ts);
}

// Also benchmark getCurrentTimeSec to see if it is still the fastest.
void benchInlineClock() {
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    ts = util::getCurrentTimeSec();
  }
  folly::doNotOptimizeAway(ts);
}
} // namespace cachelib
} // namespace facebook

BENCHMARK(inline_steady_clock) { facebook::cachelib::benchInlineSteadyClock(); }
BENCHMARK_RELATIVE(steady_clock_ticker) {
  facebook::cachelib::benchSteakyClockTicker();
}
BENCHMARK_RELATIVE(inline_steady_clock_in_branch) {
  facebook::cachelib::benchSteadyClockInBranch();
}

BENCHMARK_RELATIVE(benchInlineClock) { facebook::cachelib::benchInlineClock(); }

int main() { folly::runBenchmarks(); }
