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

#include <folly/SpinLock.h>

#include <atomic>
#include <vector>

#include "cachelib/common/BloomFilter.h"
#include "cachelib/common/CountMinSketch.h"
#include "cachelib/common/Ticker.h"
#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {
namespace detail {
// Template type for CMS
template <typename CMS>
class AccessTrackerBase {
 public:
  using TickerFct = folly::Function<size_t()>;
  struct Config {
    // number of past buckets to track.
    size_t numBuckets{0};

    // Bucket configuration. By default we have one hour per bucket.
    // function to return the current tick.

    // The ticker to be uesd to supply current tick.
    // If this is not set, the default clock based ticker will be used.
    std::shared_ptr<Ticker> ticker{std::make_shared<ClockBasedTicker>()};

    // number of ticks per bucket.
    size_t numTicksPerBucket{3600};

    // if true, the tracker uses CMS. Otherwise, the tracker uses Bloom Filters.
    bool useCounts{true};

    // maximum number of ops we expect per bucket
    size_t maxNumOpsPerBucket{1'000'000};

    // CMS specific configs.

    // error in count can not be more more than this. Must be non zero
    size_t cmsMaxErrorValue{1};

    // certainity that the error is within the above margin.
    double cmsErrorCertainity{0.99};

    // TODO (sathya) this can be cut short by 4x using a uin8_t instead of
    // uint32_t for counts in CMS.
    //
    // maximum width param to control  the max memory usage of 256mb per hour
    size_t cmsMaxWidth{8'000'000};

    // maximum depth param to control  the max memory usage of 256mb per hour
    size_t cmsMaxDepth{8};

    // BloomFilter specific configs.

    // false positive rate for bloom filter
    double bfFalsePositiveRate{0.02};
  };

  explicit AccessTrackerBase(Config config);

  // 1. The access count of the accessed key in the current
  // bucket is incremented.
  // 2. Collect the most recent config_.numBuckets access counts
  // and return in a vector.
  // @param key accessed key.
  // @return vector of length config_.numBuckets. element i contains
  // the access count of current - i bucket span.
  std::vector<double> recordAndPopulateAccessFeatures(folly::StringPiece key) {
    auto features = getAccesses(key);
    recordAccess(key);
    return features;
  }

  // Record access to the current bucket.
  void recordAccess(folly::StringPiece key);

  // Return the access histories of the given key.
  std::vector<double> getAccesses(folly::StringPiece key);

  size_t getNumBuckets() const noexcept { return config_.numBuckets; }

  AccessTrackerBase(const AccessTrackerBase& other) = delete;
  AccessTrackerBase& operator=(const AccessTrackerBase& other) = delete;

  AccessTrackerBase(AccessTrackerBase&& other) noexcept
      : config_(std::move(other.config_)),
        mostRecentAccessedBucket_(other.mostRecentAccessedBucket_.load()),
        filters_(std::move(other.filters_)),
        counts_(std::exchange(other.counts_, {})),
        locks_(std::exchange(other.locks_, {})) {}

  AccessTrackerBase& operator=(AccessTrackerBase&& other) {
    if (this != &other) {
      this->~AccessTrackerBase();
      new (this) AccessTrackerBase(std::move(other));
    }
    return *this;
  }

  size_t getByteSize() const noexcept {
    return filters_.getByteSize() +
           (counts_.empty() ? 0 : counts_.size() * counts_.at(0).getByteSize());
  }

  // Get number of accesses in each bucket.
  // count[i]: number of accesses in the (current - i) bucket.
  std::vector<uint64_t> getRotatedAccessCounts();

 private:
  // this means we can have the estimate be off by 0.001% of max
  static constexpr uint64_t kRandomSeed{314159};

  size_t getCurrentBucketIndex() const {
    return rotatedIdx(config_.ticker->getCurrentTick() /
                      config_.numTicksPerBucket);
  }

  // rotate raw bucket index to fit into the output.
  size_t rotatedIdx(size_t bucket) const { return bucket % config_.numBuckets; }

  // Update the most recent accessed bucket.
  void updateMostRecentAccessedBucket();

  // Get current access count of the value from one bucket.
  //
  // @param idx bucket index.
  // @param hashVal the value to look up
  double getBucketAccessCountLocked(size_t idx, uint64_t hashVal) const;

  // Record the access of the value to a bucket
  //
  // @param idx bucket index.
  // @param hashval the value to look up.
  void updateBucketLocked(size_t idx, uint64_t hashVal);

  // Reset the access count of the bucket. Used when bucket rotation happens.
  //
  // @param idx bucket index.
  void resetBucketLocked(size_t idx);

  Config config_;

  std::atomic<size_t> mostRecentAccessedBucket_{0};

  // Record last config_.numBuckets buckets of potential flash admissions, as
  // input features to admission model.  Each bloom filter is written to for a
  // bucket, then stored for config_.numBuckets-1 bucket spans, then removed.
  // Used only if config_.useCounts sets to false.
  BloomFilter filters_;

  // CMS tracking for counts
  std::vector<CMS> counts_;

  using LockHolder = std::lock_guard<folly::SpinLock>;
  // locks protecting each hour of the filters_ or counts_
  std::vector<folly::SpinLock> locks_;

  std::vector<uint64_t> itemCounts_;
};
} // namespace detail

using AccessTracker = detail::AccessTrackerBase<util::CountMinSketch>;
using AccessTracker8 = detail::AccessTrackerBase<util::CountMinSketch8>;
using AccessTracker16 = detail::AccessTrackerBase<util::CountMinSketch16>;

} // namespace cachelib
} // namespace facebook
#include "cachelib/common/AccessTracker-inl.h"
