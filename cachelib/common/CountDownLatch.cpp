// Copyright 2004-present Facebook. All Rights Reserved.

#include "cachelib/common/CountDownLatch.h"

namespace facebook {
namespace cachelib {
namespace util {
CountDownLatch::CountDownLatch(uint32_t count) : count_(count) {}
// Wait for the count down to complete.
void CountDownLatch::wait() {
  std::unique_lock<std::mutex> lock(lock_);
  if (count_ == 0) {
    return;
  }
  cv_.wait(lock, [this]() { return count_ == 0; });
}

// Count down without waiitng.
bool CountDownLatch::count_down() {
  std::unique_lock<std::mutex> lock(lock_);
  if (count_ > 0) {
    count_--;
  }
  if (count_ == 0) {
    cv_.notify_all();
    return true;
  }
  return false;
}

// Count down then wait for other threads to finish the count down.
void CountDownLatch::arrive_and_wait() {
  if (count_down()) {
    return;
  }
  wait();
}

bool CountDownLatch::try_wait() {
  std::unique_lock<std::mutex> lock(lock_);
  return count_ == 0;
}

} // namespace util
} // namespace cachelib
} // namespace facebook
