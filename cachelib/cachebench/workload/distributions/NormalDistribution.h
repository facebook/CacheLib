#pragma once
#include <folly/Format.h>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/distributions/FastDiscrete.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
class NormalDistribution {
 public:
  using PopDistT = std::normal_distribution<double>;
  explicit NormalDistribution(const DistributionConfig& c)
      : config_(c),
        opDist_({config_.setRatio, config_.getRatio, config_.delRatio,
                 config_.addChainedRatio, config_.loneGetRatio,
                 config_.loneSetRatio}),
        useDiscreteValSizes_(config_.hasDiscreteValueSizes()),
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

  template <typename RNG>
  uint8_t sampleOpDist(RNG& gen) {
    return opDist_(gen);
  }

  template <typename RNG>
  double sampleValDist(RNG& gen) {
    if (useDiscreteValSizes_) {
      size_t idx = valSizeDiscreteDist_(gen);
      return config_.valSizeRange[idx];
    } else {
      return valSizePiecewiseDist_(gen);
    }
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

  PopDistT getPopDist(size_t left, size_t right) {
    double mu = (left + right) * 0.5;
    // TODO In general, could have different keyFrequency factor besides 2
    double sigma = (right - left) * .5 / 2;
    return std::normal_distribution<double>(mu, sigma);
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
