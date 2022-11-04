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

#include "cachelib/common/CountDownLatch.h"

namespace facebook {
namespace cachelib {

namespace {
static constexpr size_t kThreads = 10;
static constexpr size_t kLatches = 5;
} // namespace

// Assert that arrive_and_wait returns only after the countdown is 0.
TEST(CountDownLatchTest, BasicTest) {
  util::CountDownLatch latch{kThreads};
  auto countDown = [&latch]() {
    latch.arrive_and_wait();
    ASSERT_TRUE(latch.try_wait());
  };

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreads; i++) {
    threads.push_back(std::thread{countDown});
  }
  for (size_t i = 0; i < kThreads; i++) {
    threads[i].join();
  }
}

// Assert that when operating on multiple threads, only waiting on the last
// latch would effectively guarantee all lathes are completed.
TEST(CountDownLatchTest, MultiLatches) {
  std::array<std::unique_ptr<util::CountDownLatch>, kLatches> latches;

  for (size_t i = 0; i < kLatches; i++) {
    latches[i] = std::make_unique<util::CountDownLatch>(kThreads);
  }

  auto countDown = [&latches]() {
    for (size_t i = 0; i < kLatches; i++) {
      latches[i]->count_down();
    }

    latches[kLatches - 1]->wait();

    for (size_t i = 0; i < kLatches; i++) {
      ASSERT_TRUE(latches[i]->try_wait());
    }
  };

  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreads; i++) {
    threads.push_back(std::thread{countDown});
  }
  for (size_t i = 0; i < 10; i++) {
    threads[i].join();
  }
}
} // namespace cachelib
} // namespace facebook
