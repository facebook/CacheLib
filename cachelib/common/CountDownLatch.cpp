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
  {
    std::unique_lock<std::mutex> lock(lock_);
    if (count_ == 0) {
      return true;
    }

    count_--;
    if (count_ > 0) {
      return false;
    }
  }

  cv_.notify_all(); // count transitioned from non-zero to zero
  return true;
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
