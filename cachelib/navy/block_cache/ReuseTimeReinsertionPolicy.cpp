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

#include "ReuseTimeReinsertionPolicy.h"

#include "cachelib/allocator/nvmcache/NvmItem.h"

namespace facebook::cachelib::navy {

ReuseTimeReinsertionPolicy::ReuseTimeReinsertionPolicy(
    const cachelib::navy::Index& index,
    size_t numBuckets,
    size_t bucketSize,
    uint32_t reinsertionThreshold,
    std::shared_ptr<Ticker> ticker)
    : reuseTimeThreshold_(reinsertionThreshold),
      index_(index),
      numBuckets_(numBuckets),
      bucketSize_(bucketSize) {
  auto config = AccessTracker::Config();
  config.numBuckets = numBuckets_;
  config.useCounts = false;
  config.numTicksPerBucket = bucketSize_;
  config.ticker = std::move(ticker);
  tracker_ = std::make_unique<AccessTracker>(std::move(config));
}

ReuseTimeReinsertionPolicy::ReuseTimeReinsertionPolicy(
    const cachelib::navy::Index& index,
    size_t numBuckets,
    size_t bucketSize,
    uint32_t reinsertionThreshold)
    : ReuseTimeReinsertionPolicy(
          index, numBuckets, bucketSize, reinsertionThreshold, nullptr) {}

bool ReuseTimeReinsertionPolicy::shouldReinsert(folly::StringPiece key,
                                                folly::StringPiece value) {
  updateRateWindow(); // check if window needs to be reset
  reinsertAttempts_.inc();
  attemptedBytes_.add(value.size());
  windowAttemptedCount_.fetch_add(1, std::memory_order_relaxed);

  const auto lr = index_.peek(
      makeHK(cachelib::navy::BufferView{
                 key.size(), reinterpret_cast<const uint8_t*>(key.data())})
          .keyHash());

  if (!(lr.found())) {
    keyNotFound_.inc();
    return false;
  }

  if (isExpired(value)) {
    expired_.inc();
    return false;
  }

  auto reuseTime = getReuseTime(key);
  if ((reuseTime == 0) || (reuseTime > reuseTimeThreshold_)) {
    return false;
  }
  reinserted_.inc();
  windowReinsertedCount_.fetch_add(1, std::memory_order_relaxed);
  windowReinsertedBytes_.fetch_add(value.size(), std::memory_order_relaxed);
  reinsertedBytes_.add(value.size());
  return true;
}

void ReuseTimeReinsertionPolicy::onLookup(folly::StringPiece key) {
  tracker_->recordAccess(key);
}

void ReuseTimeReinsertionPolicy::getCounters(
    const util::CounterVisitor& visitor) const {
  visitor("bc_reinsert_reuse_time_attempted",
          reinsertAttempts_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);
  visitor("bc_reinsert_reuse_time_attempted_bytes",
          attemptedBytes_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);
  visitor("bc_reinsert_reuse_time_key_not_found",
          keyNotFound_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);
  visitor("bc_reinsert_reuse_time_accepted",
          reinserted_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);
  visitor("bc_reinsert_reuse_time_accepted_bytes",
          reinsertedBytes_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);
  visitor("bc_reinsert_expired",
          expired_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);
  visitor("bc_reinsert_no_prev_access",
          noPrevAccess_.get(),
          cachelib::util::CounterVisitor::CounterType::RATE);

  visitor("bc_reinsert_last_byte_accepted",
          lastBytesAccepted_.load(std::memory_order_relaxed),
          cachelib::util::CounterVisitor::CounterType::COUNT);

  visitor("bc_reinsert_last_acceptance_rate",
          lastAcceptanceRate_.load(std::memory_order_relaxed),
          cachelib::util::CounterVisitor::CounterType::COUNT);

  auto attempts = reinsertAttempts_.get();
  double acceptanceRate = attempts > 0
                              ? static_cast<double>(reinserted_.get()) /
                                    static_cast<double>(attempts)
                              : 0.0;
  visitor("bc_reinsert_acceptance_rate",
          acceptanceRate,
          cachelib::util::CounterVisitor::CounterType::COUNT);

  reuseTimeStats_.visitQuantileEstimator(visitor, "bc_reinsert_reuse_time");
}

uint32_t ReuseTimeReinsertionPolicy::getReuseThreshold() const {
  return reuseTimeThreshold_.load(std::memory_order_relaxed);
}

size_t ReuseTimeReinsertionPolicy::getReuseTime(folly::StringPiece key) {
  /*
   * Compute the reuse time for the key. The reuse time is the maximum of
   * time since last access and the most recent reuse time. If we only have
   * a history of access in a single bucket, then we only have time since
   * access. If we have a history of access in multiple buckets, then we
   * have both time since last access and the most recent reuse time.
   *
   * The granularity of reuse time is based on bucket size. Even if the key
   * was accessed in last few seconds, if there is access in the most recent
   * bucket, the reuse time will be equal to bucket size.
   */
  std::tuple<int64_t, int64_t> reuseTuple = getPrevAccessBuckets(key);
  auto reuseTime = 0;
  if (std::get<0>(reuseTuple) >= 0) {
    size_t timeSinceLastAccess = bucketSize_ * (std::get<0>(reuseTuple) + 1);
    size_t latestReuseTime = 0;
    if (std::get<1>(reuseTuple) >= 0) {
      XCHECK(std::get<0>(reuseTuple) < std::get<1>(reuseTuple));
      latestReuseTime =
          (std::get<1>(reuseTuple) - std::get<0>(reuseTuple)) * bucketSize_;
    }
    reuseTime = std::max(timeSinceLastAccess, latestReuseTime);
    reuseTimeStats_.trackValue(static_cast<double>(reuseTime));
  } else {
    noPrevAccess_.inc();
  }
  return reuseTime;
}

std::tuple<int64_t, int64_t> ReuseTimeReinsertionPolicy::getPrevAccessBuckets(
    folly::StringPiece key) {
  /*
   * Get the index of the latest and second latest access buckets for the key.
   * This information allows us to calculate time since last access and the most
   * recent reuse time.
   * If we can't find the latest or second latest access bucket,
   * we assign -1 as the bucket index. The key that was not
   * accessed in the time window being tracked will return a tuple of (-1, -1).
   */
  std::vector<double> accesses = tracker_->getAccesses(key);
  int64_t latestReuseBucket = -1;
  int64_t secondLatestReuseBucket = -1;

  for (size_t bucketIdx = 0; bucketIdx < accesses.size(); bucketIdx++) {
    if (accesses.at(bucketIdx) > 0) {
      if (latestReuseBucket == -1) {
        latestReuseBucket = bucketIdx;
      } else if (secondLatestReuseBucket == -1) {
        secondLatestReuseBucket = bucketIdx;
      } else {
        break;
      }
    }
  }

  return std::make_tuple(latestReuseBucket, secondLatestReuseBucket);
}

uint32_t ReuseTimeReinsertionPolicy::isExpired(folly::StringPiece value) {
  if (value.size() < sizeof(cachelib::NvmItem)) {
    return 0;
  }
  cachelib::NvmItem nvmItem{0, 0, 0, std::vector<cachelib::Blob>{}};
  ::memcpy(&nvmItem, value.data(), sizeof(cachelib::NvmItem));

  return nvmItem.isExpired();
}

void ReuseTimeReinsertionPolicy::updateRateWindow() const {
  auto currentTime = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());

  auto windowStart = windowStartTime_.load(std::memory_order_relaxed);
  // Initialize windowStartTime_ on first call
  if (windowStart == 0) {
    windowStartTime_.store(currentTime, std::memory_order_relaxed);
    return;
  }

  auto elapsed = currentTime - windowStart;
  if (elapsed >= windowSizeMs_) {
    // Try to rotate the window (only one thread should succeed)
    if (windowStartTime_.compare_exchange_strong(
            windowStart, currentTime, std::memory_order_relaxed)) {
      // Atomically read and reset counters to avoid losing increments
      // from concurrent threads
      auto attempts =
          windowAttemptedCount_.exchange(0, std::memory_order_relaxed);
      auto reinserted =
          windowReinsertedCount_.exchange(0, std::memory_order_relaxed);
      auto reinsertedBytes =
          windowReinsertedBytes_.exchange(0, std::memory_order_relaxed);
      double rate =
          attempts > 0 ? static_cast<double>(reinserted) / attempts : 0.0;
      lastAcceptanceRate_.store(rate, std::memory_order_relaxed);
      lastBytesAccepted_.store(reinsertedBytes, std::memory_order_relaxed);
    }
  }
}

} // namespace facebook::cachelib::navy
