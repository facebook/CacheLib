/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include <thread>

#include "cachelib/common/Time.h"

using facebook::cachelib::util::getCurrentTimeMs;
using facebook::cachelib::util::Timer;

namespace facebook {
namespace cachelib {
namespace tests {

namespace {
uint64_t abs(uint64_t a, uint64_t b) { return a > b ? a - b : b - a; }
} // namespace
TEST(Util, TimerTest) {
  {
    auto rnd = folly::Random::rand32(100, 2000);

    Timer timer;
    timer.startOrResume();

    auto begin = getCurrentTimeMs();
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(rnd));
    timer.pause();
    // sleep_for may sleep a little bit more than rnd
    // use this to avoid flakiness
    auto duration = getCurrentTimeMs() - begin;

    // allow 2ms difference
    ASSERT_LE(abs(timer.getDurationMs(), duration), 2);
    ASSERT_EQ(timer.getDurationSec(), duration / 1000);

    {
      auto t = timer.scopedStartOrResume();
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(rnd));
    }
    duration = getCurrentTimeMs() - begin;
    // allow 3ms difference
    ASSERT_LE(abs(timer.getDurationMs(), duration), 3);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
