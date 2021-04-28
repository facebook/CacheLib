// Copyright 2004-present Facebook. All Rights Reserved.

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
