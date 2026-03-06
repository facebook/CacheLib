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

#include "cachelib/cachebench/util/Sleep.h"

#include <folly/logging/xlog.h>
#include <folly/portability/Asm.h>
#include <folly/stats/TDigest.h>

#include <thread>

using namespace std::chrono;
using namespace std::chrono_literals;

namespace facebook::cachelib::cachebench {
namespace {
static bool gCalibrated{false};
static nanoseconds gPauseNs{10};

bool keepRunning(folly::TDigest& digest) {
  auto p25 = digest.estimateQuantile(0.25);
  auto p50 = digest.estimateQuantile(0.5);
  auto p75 = digest.estimateQuantile(0.75);
  return (p75 - p25) > (0.1 * p50);
}
} // namespace

void calibrateSleep() {
  constexpr size_t maxCalibrationLoops = 100;
  constexpr size_t innerLoops = 10000;
  constexpr size_t outerLoops = 100;
  folly::TDigest digest;
  std::array<double, outerLoops> durations{};

  // Keep running until the inner quartile range (P75 - P25) is less than 10% of
  // the median so that we have a precise measurement of the duration of a pause
  size_t curLoop = 0;
  do {
    for (size_t i = 0; i < outerLoops; i++) {
      auto start = steady_clock::now();
      for (size_t j = 0; j < innerLoops; j++) {
        folly::asm_volatile_pause();
      }
      durations[i] = (steady_clock::now() - start).count();
    }
    digest = digest.merge(durations);
  } while (keepRunning(digest) && curLoop++ < maxCalibrationLoops);

  XLOG_IF(WARN, curLoop == maxCalibrationLoops)
      << "Could not calibrate high precision sleep, low-duration sleeps may be "
         "innaccurate";
  gCalibrated = true;
  gPauseNs = nanoseconds(
      static_cast<uint64_t>(digest.estimateQuantile(0.5) / innerLoops));
}

void highPrecisionSleep(nanoseconds duration) {
  if (duration > 1ms) {
    std::this_thread::sleep_for(duration);
  } else {
    auto now = steady_clock::now();
    const auto end = now + duration;

    XLOG_EVERY_MS_IF(ERR, !gCalibrated, 60000) << "Using an uncalibrated sleep";

    // folly::asm_volatile_pause() is inherently variable.  Dynamically adjust
    // the number of pauses by sleeping 10% (estimated) of the remaining
    // duration and rechecking how much time is left.
    while (now < end) {
      auto remaining = end - now;
      auto tenPercentOfDuration = remaining / 10;
      // Sleep a minimum of 1us until the last iteration(s) for efficiency
      auto sleepDuration = tenPercentOfDuration > 1us
                               ? tenPercentOfDuration
                               : std::min(remaining, 1000ns);

      const size_t numPauses = sleepDuration / gPauseNs;
      for (size_t i = 0; i < numPauses; i++) {
        folly::asm_volatile_pause();
      }
      now = steady_clock::now();
    }
  }
}

} // namespace facebook::cachelib::cachebench
