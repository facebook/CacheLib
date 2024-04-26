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

// Benchmark for measuring SteadyClockTimeStampTicker vs. inline steady_clock.
// We want to use this benchmark to decide how we may implment a clock whose
// behavior can be changed under differnt setup (simulation vs. proudction). The
// baseline is the util::getSteadyCurrentTimeSec().
// The first choice is to wrap
// this function in a class and provide an alternate implementation in
// simulation.
// The second choice is to use a if-statement and use the inline function in
// proudction.
//
// Similarly, we also compare between util::getCurrentTimeSec() vs a ticker
// implementation.
//
// The conclusion here is that we should use an if-statement to sub in the
// ticker instead of using the ticker directly.
// ============================================================================
// cachelib/benchmarks/CachelibSteadyTickerBench.cpprelative  time/iter  iters/s
// ============================================================================
// inline_steady_clock                                         10.13s    98.69m
// steady_clock_ticker                             90.222%     11.23s    89.04m
// inline_steady_clock_in_branch                   97.478%     10.39s    96.20m
// inline_clock                                    781.83%      1.30s   771.61m
// clock_ticker                                    558.94%      1.81s   551.63m
// inline_clock_ticker_in_branch                   786.90%      1.29s   776.61m
// system_clock_sec                                100.73%     10.06s    99.42m
// ============================================================================

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

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

// benchmark std::chronos::system_clock in seconds.
void benchSystemClockSec() {
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    ts = std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
             .count();
  }
  folly::doNotOptimizeAway(ts);
}

void benchClockTicker() {
  auto ticker =
      std::make_shared<facebook::cachelib::detail::ClockBasedTicker>();
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    ts = ticker->getCurrentTick();
  }
  folly::doNotOptimizeAway(ts);
}

void benchClockTickerInBranch() {
  // In prod, the ticker object is always null.
  std::shared_ptr<facebook::cachelib::detail::ClockBasedTicker> ticker =
      nullptr;
  uint32_t ts;
  for (auto i = 0; i < FLAGS_num_ops; i++) {
    if (ticker) {
      ts = ticker->getCurrentTick();
    } else {
      ts = util::getCurrentTimeSec();
    }
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

BENCHMARK_RELATIVE(inline_clock) { facebook::cachelib::benchInlineClock(); }
BENCHMARK_RELATIVE(clock_ticker) { facebook::cachelib::benchClockTicker(); }
BENCHMARK_RELATIVE(inline_clock_ticker_in_branch) {
  facebook::cachelib::benchClockTickerInBranch();
}
BENCHMARK_RELATIVE(system_clock_sec) {
  facebook::cachelib::benchSystemClockSec();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
}
