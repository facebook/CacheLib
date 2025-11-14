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

#include <folly/stats/TDigest.h>
#include <gtest/gtest.h>

#include <cmath>

#include "cachelib/cachebench/util/Sleep.h"

using namespace ::testing;
using namespace facebook::cachelib::cachebench;
using namespace std::chrono;
using namespace std::chrono_literals;

class SleepTest : public ::testing::Test {
 public:
  void SetUp() override { calibrateSleep(); }

  static void runSleepTests() {
    nanoseconds sleepTimes[] = {1us, 10us, 100us, 500us, 1ms, 5ms};
    constexpr size_t numItersPerTime = 100;

    for (const auto& time : sleepTimes) {
      std::array<double, numItersPerTime> durations{};
      for (size_t i = 0; i < numItersPerTime; i++) {
        auto start = steady_clock::now();
        highPrecisionSleep(time);
        durations[i] = (steady_clock::now() - start).count();
      }
      folly::TDigest digest;
      digest = digest.merge(durations);

      // Sleep difference should be < 10%
      auto maxError =
          std::max(10000.0, 0.1 * static_cast<double>(time.count()));
      double error =
          digest.estimateQuantile(0.5) - static_cast<double>(time.count());
      EXPECT_LT(std::fabs(error), maxError);
    }
  }
};

TEST_F(SleepTest, basic) { runSleepTests(); }

TEST_F(SleepTest, multiThread) {
  constexpr size_t numThreads = 8;
  std::array<std::thread, numThreads> threads;
  for (size_t i = 0; i < numThreads; i++) {
    threads[i] = std::thread([]() { runSleepTests(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
