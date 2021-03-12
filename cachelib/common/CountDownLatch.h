// Copyright 2004-present Facebook. All Rights Reserved.

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
