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
#include <folly/Format.h>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/FastDiscrete.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// Implementation that controls overall workloa distribution. The following
// are modeled
// 1. op distribution to identify the type of operation
// 2. popularity of key distribution  (discrete popularity and normal
//    dist)
// 3. key size, value size and chained item sizes.
class WorkloadDistribution {
 public:
  explicit WorkloadDistribution(const DistributionConfig& c)
      : config_(c),
        opDist_({config_.setRatio, config_.getRatio, config_.delRatio,
                 config_.addChainedRatio, config_.loneGetRatio,
                 config_.loneSetRatio, config_.updateRatio,
                 config_.couldExistRatio}),
        useDiscreteValSizes_(config_.usesDiscreteValueSizes()),
        valSizeDiscreteDist_{initDiscreteValSize(config_)},
        valSizePiecewiseDist_{initPiecewiseValSize(config_)},
        chainedValDist_(config_.chainedItemValSizeRange.begin(),
                        config_.chainedItemValSizeRange.end(),
                        config_.chainedItemValSizeRangeProbability.begin()),
        chainedLenDist_(config_.chainedItemLengthRange.begin(),
                        config_.chainedItemLengthRange.end(),
                        config_.chainedItemLengthRangeProbability.begin()),
        keySizeDist_(config_.keySizeRange.begin(),
                     config_.keySizeRange.end(),
                     config_.keySizeRangeProbability.begin()) {
    if (config_.valSizeRange.size() != config_.valSizeRangeProbability.size() &&
        config_.valSizeRange.size() !=
            config_.valSizeRangeProbability.size() + 1) {
      throw std::invalid_argument(
          "Val size range and their probabilities do not match up for either "
          "discrete or continuous probability. Check your "
          "test config.");
    }

    if (opDist_.probabilities().size() != static_cast<uint8_t>(OpType::kSize)) {
      throw std::invalid_argument(
          "Operation Distribution must cover all possible operations");
    }
  }

  uint8_t sampleOpDist(std::mt19937_64& gen) { return opDist_(gen); }

  double sampleValDist(std::mt19937_64& gen) {
    if (useDiscreteValSizes_) {
      size_t idx = valSizeDiscreteDist_(gen);
      return config_.valSizeRange[idx];
    } else {
      return valSizePiecewiseDist_(gen);
    }
  }

  double sampleChainedValDist(std::mt19937_64& gen) {
    return chainedValDist_(gen);
  }

  double sampleChainedLenDist(std::mt19937_64& gen) {
    return chainedLenDist_(gen);
  }

  double sampleKeySizeDist(std::mt19937_64& gen) { return keySizeDist_(gen); }

  // unlike other sources, we let the workload generator directly make copies
  // of the base popularity distribution and sample from that.
  std::unique_ptr<Distribution> getPopDist(size_t left, size_t right) const {
    if (config_.usesDiscretePopularity()) {
      return std::make_unique<FastDiscreteDistribution>(
          left, right, config_.popularityBuckets, config_.popularityWeights);
    } else {
      // TODO In general, could have different keyFrequency factor besides 2
      double mu = (left + right) * 0.5;
      double sigma = (right - left) * .5 / 2;
      return std::make_unique<NormalDistribution>(mu, sigma, left, right);
    }
  }

 private:
  // init a discrete distribution if parameters match up. If not, return a
  // default empty one.
  static std::discrete_distribution<size_t> initDiscreteValSize(
      const DistributionConfig& c) {
    if (c.valSizeRange.size() == c.valSizeRangeProbability.size()) {
      return std::discrete_distribution<size_t>(
          c.valSizeRangeProbability.begin(), c.valSizeRangeProbability.end());
    }
    return {};
  }

  // init a piecewise distribution if parameters match up. If not, return a
  // default empty one.
  static std::piecewise_constant_distribution<double> initPiecewiseValSize(
      const DistributionConfig& c) {
    if (c.valSizeRange.size() == c.valSizeRangeProbability.size() + 1) {
      return std::piecewise_constant_distribution<double>{
          c.valSizeRange.begin(), c.valSizeRange.end(),
          c.valSizeRangeProbability.begin()};
    }
    return std::piecewise_constant_distribution<double>{};
  }

  const DistributionConfig config_;
  std::discrete_distribution<uint8_t> opDist_;
  // depending on the config's val size range and probability, decides if we
  // should use a continuous piece wise distribution(valSizePiecewiseDist_) or
  // discrete distribution (valSizDiscreteDist_)
  const bool useDiscreteValSizes_{false};
  std::discrete_distribution<size_t> valSizeDiscreteDist_;
  std::piecewise_constant_distribution<double> valSizePiecewiseDist_;
  std::piecewise_constant_distribution<double> chainedValDist_;
  std::piecewise_constant_distribution<double> chainedLenDist_;
  std::piecewise_constant_distribution<double> keySizeDist_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
