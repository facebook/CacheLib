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

#pragma once
#include <condition_variable>
#include <mutex>
namespace facebook {
namespace cachelib {
namespace util {

/**
 * Forward import https://en.cppreference.com/w/cpp/thread/latch.
 */
class CountDownLatch {
 public:
  explicit CountDownLatch(uint32_t count = 0);
  // Wait for the count down to complete.
  void wait();

  // decrements the counter in a non-blocking manner
  bool count_down();

  // Count down then wait for other threads to finish the count down.
  void arrive_and_wait();

  // Returns whether the counter is 0. Does not block.
  bool try_wait();

 private:
  std::condition_variable cv_;
  std::mutex lock_;
  uint32_t count_;
};
} // namespace util
} // namespace cachelib
} // namespace facebook
