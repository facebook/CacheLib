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

#include <folly/logging/xlog.h>

#include <atomic>

#include "cachelib/common/CountDownLatch.h"
#include "cachelib/common/Ticker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// An implementation of cachelib::Ticker based on a set timestamp.
// This implementation guarantees that for up to numThreads threads,
// getCurrentTick() will not return ticks that goes backwards in
// bucket where bucket is defined as getCurrentTick() / BUCKET_TICK.
// For a concrete exmample, when getCurrentTick() returns a timestamp in seconds
// and BUCKET_TICK is 3600, this ticker makes sure all threads won't
// expreience time stamp going backward in hour. (e.g. if a thread observes a
// timestamp of 1:01, no other thread will observe a timestamp before 0:59 after
// that.)
class TimeStampTicker : public cachelib::Ticker {
 public:
  // @param numThreads Number of threads that can be updating the timestamps.
  // @param bucketTicks The number of ticks that defines a bucket.
  // @param onCrossTimeWindow The optional callback to run when a time windows
  // has been crossed by all the threads. It is called on the last thread who
  // crosses the window.
  explicit TimeStampTicker(
      uint32_t numThreads,
      uint32_t bucketTicks = 3600,
      std::function<void(double elapsedSecs)> onCrossTimeWindow = nullptr)
      : numThreads_{numThreads}, bucketTicks_{bucketTicks} {
    if (onCrossTimeWindow) {
      onCrossTimeWindows_.push_back(std::move(onCrossTimeWindow));
    }
  }

  // Return the current tick.
  uint32_t getCurrentTick() override {
    return currTimeStamp_.load(std::memory_order_relaxed);
  }

  // This function updates the current itme stamp.
  // It blocks the thread if the timeStampSecond belongs to a future bucket
  // and some other threads haven't finished the current bucket.
  void updateTimeStamp(uint32_t timeStampSecond) {
    advanceTimeStamp(timeStampSecond);
    currTimeStamp_.store(timeStampSecond, std::memory_order_relaxed);
  }

  // Add a callback to be called when the last thread crosses bucket boundary.
  void addCrossTimeWindowCb(
      std::function<void(double elapsedSecs)> onCrossTimeWindow) {
    onCrossTimeWindows_.push_back(std::move(onCrossTimeWindow));
  }

 private:
  using CountDownLatch = util::CountDownLatch;
  // CountDownLathc management
  // We keedp a number of latches in an array. These latches
  // are the synchonization mechanism to makes sure all threads finishes
  // each bucket.

  // Number of latches kept. To be memory efficient, this is typically smaller
  // than the total number of buckets we may observe in the trace. Upon the
  // finish of bucket X, the thread would countdown (and wait) on latch X %
  // LATCH_COUNT.

  // This is safe long as there is not a single thread that tries
  // to update a timestamp that advanced more than LATCH_COUNT buckets.
  static constexpr size_t LATCH_COUNT{100};
  // latches to be synced on.
  std::array<std::shared_ptr<CountDownLatch>, LATCH_COUNT> latches_;
  // Locks protecting the initialization of each latch.
  std::array<std::mutex, LATCH_COUNT> locks_;
  // Return the latch for the current bucket.
  std::shared_ptr<CountDownLatch> getLatch(size_t bucket);

  // Try to advance the timestamp. This blocks until the timestamp can be safely
  // updated.
  // @param ts Advance the timestamp to the ts.
  // @return whether the timestamp can be updated safely without potentially
  // causing bucket going backward. This could happen sometime due to known
  // issue. We typically log this error and move on.
  bool advanceTimeStamp(uint32_t ts);

  // The current time stamp;
  std::atomic<uint32_t> currTimeStamp_{0};

  const uint32_t numThreads_;
  const uint32_t bucketTicks_;

  // The callbacks to run when the last thread cross a window.
  // The param elapsedSecs is how long in seconds a time window is. It have the
  // same value as bucketTicks_.
  std::vector<std::function<void(double elapsedSecs)>> onCrossTimeWindows_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
