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

using facebook::cachelib::util::Timer;

namespace facebook {
namespace cachelib {
namespace tests {

TEST(Util, TimerTest) {
  {
    auto rnd = folly::Random::rand32(100, 2000);

    Timer timer;
    timer.startOrResume();
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(rnd));
    timer.pause();

    ASSERT_EQ(timer.getDurationMs(), rnd);
    ASSERT_EQ(timer.getDurationSec(), rnd / 1000);

    {
      auto t = timer.scopedStartOrResume();
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(rnd));
    }
    ASSERT_EQ(timer.getDurationMs(), rnd * 2);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
