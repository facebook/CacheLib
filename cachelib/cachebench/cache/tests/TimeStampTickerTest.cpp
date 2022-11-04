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

#include <gtest/gtest.h>

#include <thread>

#include "cachelib/cachebench/cache/TimeStampTicker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace test {

TEST(TimeStampTickerTest, callbackTest) {
  int called = 0;
  auto callback = [&called](double) { ++called; };

  uint32_t numThreads = 3;
  TimeStampTicker ticker{numThreads, 1, callback};

  auto threadFunc = [&ticker]() {
    ticker.updateTimeStamp(1);
    ticker.updateTimeStamp(2);
  };
  std::vector<std::thread> threads;
  for (size_t i = 0; i < numThreads; i++) {
    threads.push_back(std::thread{threadFunc});
  }
  for (size_t i = 0; i < numThreads; i++) {
    threads[i].join();
  }
  // Expect callback to be called twice since each thread passed two time
  // windows.
  EXPECT_EQ(2, called);
}

} // namespace test
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
