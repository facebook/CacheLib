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

#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// generic distribution that uses a random number generator to generate a 64
// bit number fitting the configured distribution.
class Distribution {
 public:
  virtual ~Distribution() {}
  virtual size_t operator()(std::mt19937_64& gen) = 0;
};

// Wrapper around std::normal_distribution
class NormalDistribution final : public Distribution {
 public:
  NormalDistribution(double mean, double stdDev, size_t left, size_t right)
      : left_(left), right_(right), dist_(mean, stdDev) {}

  size_t operator()(std::mt19937_64& gen) override {
    double v;
    do {
      v = std::round(dist_(gen));
    } while (v < left_ || v > right_);
    XDCHECK_GE(v, left_);
    XDCHECK_LE(v, right_);
    return cachelib::util::narrow_cast<size_t>(v);
  }

 private:
  const size_t left_;
  const size_t right_;
  std::normal_distribution<double> dist_;
};

// sampling object id and object size from a Zipf-like distribution
// (aka the independent reference model (IRM))
//
// Multiple objects with similar rates are grouped together for more efficient
// sampling. Two level sampling process: first skewed sample to select the rate,
// then uniform sample within rate to select object.
class FastDiscreteDistribution final : public Distribution {
 public:
  FastDiscreteDistribution() {}
  // This distribution expects a list of buckets (sizes) and weights (probs).
  // All objects in a bucket will be selected with roughly the same
  // probability, and each bucket is drawn approximately proporionally to its
  // weight.
  // @param left denotes the minimum index the distribution can generate
  // @param right denotes the maximum index the distribution can generate
  // @param sizes and @param probs work as described above
  // @param numBuckets controls the number of equal weight buckets used to
  // approximate the distribution.  A higher number results in a distribution
  // closer to the input distribution from sizes and probs.
  FastDiscreteDistribution(size_t left,
                           size_t right,
                           std::vector<size_t> sizes,
                           std::vector<double> probs,
                           size_t numBuckets = 2048)
      : leftOffset_(left), rightOffset_(right) {
    double totalWeight = std::accumulate(probs.begin(), probs.end(), 0.0);
    double totalObjects = std::accumulate(sizes.begin(), sizes.end(), 0.0);
    bucketWeight_ = totalWeight / numBuckets;
    double weightSeen = 0.0;
    size_t objectsSeen = 0;
    scalingFactor_ = (right - left) / totalObjects;
    bucketOffsets_.push_back(0);
    size_t i = 0;
    std::vector<uint64_t> buckets;
    // Divide the input distribution into numBuckets of equal weight.
    // The approximation is that objects in a bucket have equal weight.
    // Since we have equal weight buckets and (roughly) equal weight objects
    // we can sample in constant time by drawing 2 Uniform r.v.'s.
    while (i < probs.size()) {
      if (weightSeen + probs[i] >= bucketWeight_) {
        // interpolate, update bucket, reset
        double bucketPct = (bucketWeight_ - weightSeen) / probs[i];
        objectsSeen +=
            facebook::cachelib::util::narrow_cast<size_t>(bucketPct * sizes[i]);
        objectsSeen = std::max(1UL, objectsSeen);
        sizes[i] -=
            facebook::cachelib::util::narrow_cast<size_t>(bucketPct * sizes[i]);
        probs[i] -= bucketPct * probs[i];

        auto scaledObjects =
            static_cast<uint64_t>(objectsSeen * scalingFactor_);
        buckets.push_back(scaledObjects);
        if (bucketOffsets_.size() > 0) {
          bucketOffsets_.push_back(bucketOffsets_.back() + scaledObjects);
        }
        weightSeen = 0.0;
        objectsSeen = 0;
      } else {
        weightSeen += probs[i];
        objectsSeen += sizes[i];
        i++;
      }
    }
    bucketDistribution_ =
        std::uniform_int_distribution<uint64_t>(0, buckets.size() - 1);
    for (auto it = buckets.begin(); it != buckets.end(); it++) {
      insideBucketDistributions_.emplace_back(0, (*it) - 1);
    }
  }

  FastDiscreteDistribution(FastDiscreteDistribution&&) = default;
  FastDiscreteDistribution& operator=(FastDiscreteDistribution&&) = default;
  FastDiscreteDistribution(const FastDiscreteDistribution&) = default;
  FastDiscreteDistribution& operator=(const FastDiscreteDistribution&) =
      default;

  size_t operator()(std::mt19937_64& gen) override {
    size_t bucket = bucketDistribution_(gen);
    size_t objectInBucket = facebook::cachelib::util::narrow_cast<size_t>(
        insideBucketDistributions_[bucket](gen));
    auto ret = facebook::cachelib::util::narrow_cast<size_t>(
                   (bucketOffsets_[bucket] + objectInBucket)) +
               leftOffset_;
    XDCHECK_LE(ret, rightOffset_);
    XDCHECK_GE(ret, leftOffset_);
    return ret;
  }

  void summarize() const {
    size_t count(0);
    std::cout << "Bucket Weight: " << bucketWeight_ << std::endl;
    std::cout << "Buckets:" << std::endl;
    for (auto offset : bucketOffsets_) {
      std::cout << count << ": " << offset << std::endl;
      count++;
    }
  }

 private:
  std::vector<uint64_t> bucketOffsets_{};
  const size_t leftOffset_{};
  const size_t rightOffset_{};
  double scalingFactor_{};
  double bucketWeight_{};
  std::uniform_int_distribution<uint64_t> bucketDistribution_{};
  std::vector<std::uniform_int_distribution<uint64_t>>
      insideBucketDistributions_{};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
