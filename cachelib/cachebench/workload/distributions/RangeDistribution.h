#pragma once

#include <folly/Format.h>
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
// Range Distribution represents popularity via a normal distribution,
// and all other distributions via piecewise constant distributions
// which are explicity specified in the configs.
// This essentially allows some bucketing of objects/sizes with similar
// probabilities, but will still require O(buckets) time to sample.
class RangeDistribution {
 public:
  RangeDistribution(const DistributionConfig& c)
      : config_(c),
        opDist_({config_.setRatio, config_.getRatio, config_.delRatio,
                 config_.addChainedRatio, config_.loneGetRatio,
                 config_.loneSetRatio}),
        valSizeDist_(config_.valSizeRange.begin(),
                     config_.valSizeRange.end(),
                     config_.valSizeRangeProbability.begin()),
        chainedValDist_(config_.chainedItemValSizeRange.begin(),
                        config_.chainedItemValSizeRange.end(),
                        config_.chainedItemValSizeRangeProbability.begin()),
        chainedLenDist_(config_.chainedItemLengthRange.begin(),
                        config_.chainedItemLengthRange.end(),
                        config_.chainedItemLengthRangeProbability.begin()),
        keySizeDist_(config_.keySizeRange.begin(),
                     config_.keySizeRange.end(),
                     config_.keySizeRangeProbability.begin()) {
    if (config_.valSizeRange.size() - 1 !=
        config_.valSizeRangeProbability.size()) {
      throw std::invalid_argument(
          "Val size range and their probabilities do not match up. Check your "
          "test config.");
    }
    if (opDist_.probabilities().size() != static_cast<uint8_t>(OpType::kSize)) {
      throw std::invalid_argument(
          "Operation Distribution must cover all possible operations");
    }
  }

  template <typename RNG>
  uint8_t sampleOpDist(RNG& gen) {
    return opDist_(gen);
  }

  template <typename RNG>
  double sampleValDist(RNG& gen) {
    return valSizeDist_(gen);
  }

  template <typename RNG>
  double sampleChainedValDist(RNG& gen) {
    return chainedValDist_(gen);
  }
  template <typename RNG>
  double sampleChainedLenDist(RNG& gen) {
    return chainedLenDist_(gen);
  }

  template <typename RNG>
  double sampleKeySizeDist(RNG& gen) {
    return keySizeDist_(gen);
  }

  std::normal_distribution<double> getPopDist(size_t left, size_t right) {
    double mu = (left + right) * 0.5;
    // TODO In general, could have different keyFrequency factor besides 2
    double sigma = (right - left) * .5 / 2;
    return std::normal_distribution<double>(mu, sigma);
  }

 private:
  const DistributionConfig config_;
  std::discrete_distribution<uint8_t> opDist_;
  std::piecewise_constant_distribution<double> valSizeDist_;

  std::piecewise_constant_distribution<double> chainedValDist_;
  std::piecewise_constant_distribution<double> chainedLenDist_;
  std::piecewise_constant_distribution<double> keySizeDist_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
