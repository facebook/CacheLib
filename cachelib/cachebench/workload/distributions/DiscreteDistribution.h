#pragma once
#include <folly/Format.h>
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"

#include "cachelib/cachebench/workload/distributions/FastDiscrete.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
class DiscreteDistribution {
 public:
  DiscreteDistribution(const DistributionConfig& c)
      : config_(c),
        opDist_({config_.setRatio, config_.getRatio, config_.delRatio,
                 config_.addChainedRatio, config_.loneGetRatio,
                 config_.loneSetRatio}),
        valSizeDist_(config_.valSizeRangeProbability.begin(),
                     config_.valSizeRangeProbability.end()),
        chainedValDist_(config_.chainedItemValSizeRange.begin(),
                        config_.chainedItemValSizeRange.end(),
                        config_.chainedItemValSizeRangeProbability.begin()),
        chainedLenDist_(config_.chainedItemLengthRange.begin(),
                        config_.chainedItemLengthRange.end(),
                        config_.chainedItemLengthRangeProbability.begin()),
        keySizeDist_(config_.keySizeRange.begin(),
                     config_.keySizeRange.end(),
                     config_.keySizeRangeProbability.begin()) {
    if (config_.valSizeRange.size() != config_.valSizeRangeProbability.size()) {
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
    size_t idx = valSizeDist_(gen);
    return config_.valSizeRange[idx];
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

  FastDiscreteDistribution getPopDist(size_t left, size_t right) {
    return FastDiscreteDistribution(
        left, right, config_.popularityBuckets, config_.popularityWeights);
  }

 private:
  const DistributionConfig config_;
  std::discrete_distribution<uint8_t> opDist_;
  std::discrete_distribution<size_t> valSizeDist_;
  std::piecewise_constant_distribution<double> chainedValDist_;
  std::piecewise_constant_distribution<double> chainedLenDist_;
  std::piecewise_constant_distribution<double> keySizeDist_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
