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

#include "cachelib/cachebench/cache/TimeStampTicker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// Advance the timestamp.
// To make sure that currTimeStamp_ does not go backwards from one bucket to
// another, each thread need to wait on a CountdownLatch when trying to update
// from one bucket to the next.
//
// This implementation has the following drawbacks:
// 1) If the first timestamp submitted by each thread is different, it may cause
// the first bucket to go backward.
// TODO: Fix this T85645217
// 2) If the last timestamp submitted by each thread is different by more than 1
// bucket, it may hang.
// TODO: Fix this T85645905
bool TimeStampTicker::advanceTimeStamp(uint32_t currTimeStamp) {
  auto oldTimeStamp = currTimeStamp_.load(std::memory_order_relaxed);
  auto oldBucket = oldTimeStamp / bucketTicks_;

  auto currentBucket = currTimeStamp / bucketTicks_;

  if (oldBucket > currentBucket) {
    XLOGF(ERR, "Bucket going BACKWARD from {} to {}", oldBucket, currentBucket);
    // This is an indication that our time-advancing mechanism has a bug.
    // TODO: Once T85645217 is fixed, throw an exception.
    return false;
  }

  // Bucket matches. No update required.
  if (oldBucket == currentBucket) {
    return true;
  }

  if (currentBucket - oldBucket > LATCH_COUNT) {
    // This thread violates the assumption of LATCH_COUNT.
    // This indicates a bug if currentBucket is not 0.
    // TODO: Once T85645217 is fixed, throw an exception.
    XLOGF(ERR,
          "A thread is trying to update too many buckets at one time: {} to {}",
          oldBucket, currentBucket);
    return false;
  }

  // Countdown on all the buckets' latch that this timestamp is attempting
  // to cross.
  std::shared_ptr<CountDownLatch> latch;
  while (oldBucket < currentBucket) {
    latch = getLatch(oldBucket);
    auto lastThread = latch->count_down();
    if (lastThread && !onCrossTimeWindows_.empty()) {
      for (auto& fn : onCrossTimeWindows_) {
        fn(static_cast<double>(bucketTicks_));
      }
    }
    oldBucket++;
  }
  XDCHECK(latch);
  // Wait on the last bucket.
  // Currently we set no timeout as we want to expose any potential bugs.
  latch->wait();
  // All threads finished currentBucket - 1.
  oldTimeStamp = currTimeStamp_.load(std::memory_order_relaxed);
  oldBucket = oldTimeStamp / bucketTicks_;
  if (oldBucket > currentBucket) {
    // This is an indication that our time-advancing mechanism has a bug.
    throw std::runtime_error(folly::to<std::string>(
        "Bucket going BACKWARD from", oldBucket, "to", currentBucket));
  }
  // The bucket is updated by another thread.
  if (oldBucket == currentBucket) {
    return true;
  }

  // This thread would be updating the bucket.
  if (oldBucket + 1 == currentBucket) {
    // Go ahead and update the bucket.
    return true;
  }

  // This means oldBucket < currentBucket - 1. This happens when all the
  // threads skip at least one bucket in between. Can proceed.
  return true;
}

std::shared_ptr<util::CountDownLatch> TimeStampTicker::getLatch(size_t bucket) {
  auto index = bucket % LATCH_COUNT;
  std::unique_lock<std::mutex> lock(locks_[index]);
  // If the latch does not exist, it means this is the first time we use the
  // latch.
  // If the latch's count is 0, it means this latch was from a previous
  // bucket that's LATCH_COUNT ago.

  // In either case, recreate the latch.
  if (latches_[index] == nullptr || latches_[index]->try_wait()) {
    latches_[index] = std::make_shared<CountDownLatch>(numThreads_);
  }
  return latches_[index];
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
