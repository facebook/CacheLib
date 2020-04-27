// Copyright 2004-present Facebook. All Rights Reserved.

#include "cachelib/common/AccessTracker.h"

namespace facebook {
namespace cachelib {

AccessTracker::AccessTracker(Config config)
    : config_(std::move(config)), locks_{config_.numBuckets} {
  if (config_.useCounts) {
    counts_.reserve(config_.numBuckets);
    const double errorMargin = config_.cmsMaxErrorValue /
                               static_cast<double>(config_.maxNumOpsPerBucket);
    for (size_t i = 0; i < config_.numBuckets; i++) {
      counts_.push_back(
          cachelib::util::CountMinSketch(errorMargin,
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
}

// 1. The access count of the accssed key in the current
// bucket is incremented.
// 2. Collect the most recent config_.numBuckets access counts
// and return in a vector.
std::vector<double> AccessTracker::recordAndPopulateAccessFeatures(
    folly::StringPiece key) {
  const auto bucketIdx = getCurrentBucketIndex();
  const auto hashVal =
      folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), kRandomSeed);
  std::vector<double> features(config_.numBuckets);
  // Current bucket.
  features[0] = updateAndGetCurrentBucket(bucketIdx, hashVal);
  // Extract values from previous buckets.
  // features[i]: count for bucket number (bucketIdx - i).
  for (size_t i = 1; i < config_.numBuckets; i++) {
    const auto idx = rotatedIdx(bucketIdx + config_.numBuckets - i);
    LockHolder l(locks_[idx]);
    features[i] = getBucketAccessCount(idx, hashVal);
  }
  return features;
}

// Update the current bucket access count for the accessed key.
// Return the updated access count.
// If this is the first time entering a new bucket, the oldest
// bucket's count would be cleared, keeping the number of buckets
// constant at config_.numBuckets.
double AccessTracker::updateAndGetCurrentBucket(size_t bucketIdx,
                                                uint64_t hashVal) {
  bool newBucket = false;
  while (true) {
    auto mostRecent = mostRecentAccessedBucket_.load(std::memory_order_relaxed);
    // if we are in the border of currently tracked bucket, we don't need to
    // reset the data. we assume that all threads accessing this don't run for
    // more than 2 buckets.
    if (bucketIdx == mostRecent || rotatedIdx(bucketIdx + 1) == mostRecent) {
      break;
    }

    const bool success = mostRecentAccessedBucket_.compare_exchange_strong(
        mostRecent, bucketIdx);
    if (success) {
      newBucket = true;
      break;
    }
  }
  LockHolder l(locks_[bucketIdx]);
  if (newBucket) {
    resetBucket(bucketIdx);
  }
  updateBucket(bucketIdx, hashVal);
  return getBucketAccessCount(bucketIdx, hashVal);
}

double AccessTracker::getBucketAccessCount(size_t idx, uint64_t hashVal) const {
  return config_.useCounts ? counts_.at(idx).getCount(hashVal)
                           : (filters_.couldExist(idx, hashVal)) ? 1 : 0;
}

void AccessTracker::updateBucket(size_t idx, uint64_t hashVal) {
  config_.useCounts ? counts_[idx].increment(hashVal)
                    : filters_.set(idx, hashVal);
}

void AccessTracker::resetBucket(size_t idx) {
  config_.useCounts ? counts_[idx].reset() : filters_.clear(idx);
}

} // namespace cachelib
} // namespace facebook
