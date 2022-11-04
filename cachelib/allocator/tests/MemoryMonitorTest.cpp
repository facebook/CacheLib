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

#include "cachelib/allocator/MemoryMonitor.h"

namespace facebook {
namespace cachelib {
namespace tests {

constexpr size_t kMBytes = 1024 * 1024ULL;
constexpr size_t kGBytes = kMBytes * 1024;

TEST(MemoryMonitorTest, increasingRateLimiter) {
  RateLimiter rl(true /*detectIncrease*/);
  rl.setWindowSize(10 /*windowSize*/);
  size_t rssVal = 10 * kGBytes;                  // Start at 10GB
  size_t reclaimPerIter = 2 * kGBytes * 5 / 100; // 5% of upper-lower bounds
  while (rssVal < 30 * kGBytes) {
    rl.addValue(rssVal); // Growth by 1GB per poll period
    EXPECT_EQ(0, rl.throttle(reclaimPerIter));
    rssVal += kGBytes;
  }
  // Reclaiming is stopped for next 10 iterations, followed by gradual
  // increase in next 10 iterations to full unthrottled reclaim.
  for (int i = 0; i < 21; i++) {
    if (i < 10) {
      rssVal += 10 * kMBytes; // Next 10 steps growth stops
    }
    rl.addValue(rssVal);
    if (i == 20) {
      EXPECT_EQ(reclaimPerIter, rl.throttle(reclaimPerIter));
    } else if (i >= 10) {
      EXPECT_LT(0, rl.throttle(reclaimPerIter));
    } else {
      EXPECT_EQ(0, rl.throttle(reclaimPerIter));
    }
  }
  // While rss is dropping, reclaim is unthrottled
  for (int i = 0; i < 10; i++) {
    rssVal -= kGBytes;
    rl.addValue(rssVal);
    EXPECT_EQ(reclaimPerIter, rl.throttle(reclaimPerIter));
  }
}

TEST(MemoryMonitorTest, decreasingRateLimiter) {
  RateLimiter rl(false /*detectIncrease*/);
  rl.setWindowSize(10 /*windowSize*/);
  size_t freeVal = 30 * kGBytes;                 // Start at 30GB
  size_t reclaimPerIter = 2 * kGBytes * 5 / 100; // 5% of upper-lower bounds
  while (freeVal > 10 * kGBytes) {
    rl.addValue(freeVal);
    EXPECT_EQ(0, rl.throttle(reclaimPerIter));
    freeVal -= kGBytes;
  }
  // Reclaiming is stopped for next 10 iterations, followed by gradual
  // increase in next 10 iterations to full unthrottled reclaim.
  for (int i = 0; i < 21; i++) {
    if (i < 10) {
      freeVal -= 10 * kMBytes;
    }
    rl.addValue(freeVal);
    if (i == 20) {
      EXPECT_EQ(reclaimPerIter, rl.throttle(reclaimPerIter));
    } else if (i >= 10) {
      EXPECT_LT(0, rl.throttle(reclaimPerIter));
    } else {
      EXPECT_EQ(0, rl.throttle(reclaimPerIter));
    }
  }
  // While free memory is dropping, reclaim is unthrottled
  for (int i = 0; i < 10; i++) {
    freeVal += kGBytes;
    rl.addValue(freeVal);
    EXPECT_EQ(reclaimPerIter, rl.throttle(reclaimPerIter));
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
