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

namespace facebook {
namespace cachelib {

namespace detail {
template <typename CMS>
AccessTrackerBase<CMS>::AccessTrackerBase(Config config)
    : config_(std::move(config)),
      locks_{config_.numBuckets},
      itemCounts_(config_.numBuckets, 0) {
  if (config_.useCounts) {
    counts_.reserve(config_.numBuckets);
    const double errorMargin = config_.cmsMaxErrorValue /
                               static_cast<double>(config_.maxNumOpsPerBucket);
    for (size_t i = 0; i < config_.numBuckets; i++) {
      counts_.push_back(CMS(errorMargin,
                            config_.cmsErrorCertainity,
                            config_.cmsMaxWidth,
                            config_.cmsMaxDepth));
    }
  } else {
    filters_ = facebook::cachelib::BloomFilter::makeBloomFilter(
        config_.numBuckets,
        config_.maxNumOpsPerBucket,
        config_.bfFalsePositiveRate);
  }
  if (!config_.ticker) {
    config_.ticker = std::make_shared<ClockBasedTicker>();
  }
}

// Record access to the current bucket.
template <typename CMS>
void AccessTrackerBase<CMS>::recordAccess(folly::StringPiece key) {
  updateMostRecentAccessedBucket();
  const auto hashVal =
      folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), kRandomSeed);
  const auto idx = mostRecentAccessedBucket_.load(std::memory_order_relaxed);
  LockHolder l(locks_[idx]);
  updateBucketLocked(idx, hashVal);
}

// Return the access histories of the given key.
template <typename CMS>
std::vector<double> AccessTrackerBase<CMS>::getAccesses(
    folly::StringPiece key) {
  updateMostRecentAccessedBucket();
  const auto hashVal =
      folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), kRandomSeed);
  const auto mostRecentBucketIdx =
      mostRecentAccessedBucket_.load(std::memory_order_relaxed);

  std::vector<double> features(config_.numBuckets);
  // Extract values from buckets.
  // features[i]: count for bucket number (mostRecentBucket - i).
  for (size_t i = 0; i < config_.numBuckets; i++) {
    const auto idx = rotatedIdx(mostRecentBucketIdx + config_.numBuckets - i);
    LockHolder l(locks_[idx]);
    features[i] = getBucketAccessCountLocked(idx, hashVal);
  }
  return features;
}

// Update the most recent accessed bucket.
// If updated, this is the first time entering a new bucket, the oldest
// bucket's count would be cleared, keeping the number of buckets
// constant at config_.numBuckets.
template <typename CMS>
void AccessTrackerBase<CMS>::updateMostRecentAccessedBucket() {
  const auto bucketIdx = getCurrentBucketIndex();
  while (true) {
    auto mostRecent = mostRecentAccessedBucket_.load(std::memory_order_relaxed);
    // if we are in the border of currently tracked bucket, we don't need to
    // reset the data. we assume that all threads accessing this don't run for
    // more than 2 buckets.
    if (bucketIdx == mostRecent || rotatedIdx(bucketIdx + 1) == mostRecent) {
      return;
    }

    const bool success = mostRecentAccessedBucket_.compare_exchange_strong(
        mostRecent, bucketIdx);
    if (success) {
      LockHolder l(locks_[bucketIdx]);
      resetBucketLocked(bucketIdx);
      return;
    }
  }
}

template <typename CMS>
double AccessTrackerBase<CMS>::getBucketAccessCountLocked(
    size_t idx, uint64_t hashVal) const {
  return config_.useCounts ? counts_.at(idx).getCount(hashVal)
         : (filters_.couldExist(idx, hashVal)) ? 1
                                               : 0;
}

template <typename CMS>
void AccessTrackerBase<CMS>::updateBucketLocked(size_t idx, uint64_t hashVal) {
  config_.useCounts ? counts_[idx].increment(hashVal)
                    : filters_.set(idx, hashVal);
  itemCounts_[idx]++;
}

template <typename CMS>
void AccessTrackerBase<CMS>::resetBucketLocked(size_t idx) {
  config_.useCounts ? counts_[idx].reset() : filters_.clear(idx);
  itemCounts_[idx] = 0;
}

template <typename CMS>
std::vector<uint64_t> AccessTrackerBase<CMS>::getRotatedAccessCounts() {
  size_t currentBucketIdx = getCurrentBucketIndex();
  std::vector<uint64_t> counts(config_.numBuckets);

  for (size_t i = 0; i < config_.numBuckets; i++) {
    const auto idx = rotatedIdx(currentBucketIdx + config_.numBuckets - i);
    LockHolder l(locks_[idx]);
    counts[i] = itemCounts_[idx];
  }

  return counts;
}
} // namespace detail
} // namespace cachelib
} // namespace facebook
