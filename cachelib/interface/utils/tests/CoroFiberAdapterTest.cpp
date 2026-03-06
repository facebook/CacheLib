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

#include <folly/Expected.h>
#include <folly/coro/Collect.h>
#include <folly/coro/GtestHelpers.h>
#include <gtest/gtest.h>

#include "cachelib/interface/utils/CoroFiberAdapter.h"

using namespace facebook::cachelib;
using namespace facebook::cachelib::interface::utils;

class CoroFiberAdapterTest : public testing::Test {
 public:
  template <typename T>
  using Result = folly::Expected<T, navy::Status>;

  navy::NavyThread thread_{"test"};
};

CO_TEST_F(CoroFiberAdapterTest, success) {
  auto result = co_await onWorkerThread(
      thread_,
      []() -> Result<int> { return 42; },
      [](auto&&) { FAIL() << "should not have called cleanup function"; });
  CO_ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(result.value(), 42);
}

CO_TEST_F(CoroFiberAdapterTest, defaultCleanup) {
  auto result = co_await onWorkerThread(thread_, []() -> Result<int> {
    return folly::makeUnexpected(navy::Status::DeviceError);
  });
  CO_ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), navy::Status::DeviceError);
}

CO_TEST_F(CoroFiberAdapterTest, retryThenSuccess) {
  size_t callCount = 0;
  auto result = co_await onWorkerThread(
      thread_,
      [&callCount]() -> Result<int> {
        if (callCount++ < 3) {
          return folly::makeUnexpected(navy::Status::Retry);
        }
        return 100;
      },
      [](auto&&) { FAIL() << "should not have called cleanup function"; });
  CO_ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(result.value(), 100);
  EXPECT_EQ(callCount, 4);
}

CO_TEST_F(CoroFiberAdapterTest, returnError) {
  auto result = co_await onWorkerThread(
      thread_,
      []() -> Result<int> {
        return folly::makeUnexpected(navy::Status::DeviceError);
      },
      [](auto&&) { FAIL() << "should not have called cleanup function"; });
  CO_ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), navy::Status::DeviceError);
}

CO_TEST_F(CoroFiberAdapterTest, throws) {
  auto result = co_await folly::coro::co_awaitTry(onWorkerThread(
      thread_,
      []() -> Result<int> { throw std::runtime_error("error"); },
      [](auto&&) { FAIL() << "should not have called cleanup function"; }));
  CO_ASSERT_TRUE(result.hasException<std::runtime_error>());
}

CO_TEST_F(CoroFiberAdapterTest, cancellation) {
  folly::CancellationSource cs;
  folly::fibers::Baton fiberReady, cancelReady;
  bool ranCleanup = false;

  // Ensure the fiber runs the cleanup function in the following situation to
  // avoid leaking resources:
  //
  //   1. Fiber is running an async operation
  //   2. Awaiting coro is cancelled (e.g., times out)
  //   3. Fiber finishes and sets the promise

  auto fiberFunc = [&]() -> Result<int> {
    fiberReady.post();
    cancelReady.wait();
    return 42;
  };
  auto cleanupFunc = [&ranCleanup](auto&& result) {
    ranCleanup = true;
    ASSERT_TRUE(result.hasValue());
    EXPECT_EQ(result.value(), 42);
  };
  auto cancelTask = [&]() -> folly::coro::Task<void> {
    // Ensure the fiber is inside fiberFunc() before cancellation, otherwise
    // onWorkerThread() will skip calling it
    fiberReady.wait();
    cs.requestCancellation();
    cancelReady.post();
    co_return;
  };

  auto result = co_await folly::coro::collectAllTry(
      folly::coro::co_withCancellation(cs.getToken(),
                                       onWorkerThread(thread_,
                                                      std::move(fiberFunc),
                                                      std::move(cleanupFunc))),
      cancelTask());
  EXPECT_TRUE(std::get<0>(result).hasException<folly::OperationCancelled>());
  thread_.drain(); // ensure fiberFunc() finishes running & sets the promise
  EXPECT_TRUE(ranCleanup);
}

CO_TEST_F(CoroFiberAdapterTest, concurrent) {
  auto task = [&](int value) -> folly::coro::Task<void> {
    auto result = co_await onWorkerThread(
        thread_,
        [value]() -> Result<int> { return value * 2; },
        [](auto&&) { FAIL() << "should not have called cleanup function"; });
    CO_ASSERT_TRUE(result.hasValue());
    EXPECT_EQ(result.value(), value * 2);
  };
  co_await folly::coro::collectAll(task(1), task(2), task(3), task(4), task(5));
}
