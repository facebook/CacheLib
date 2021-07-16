#pragma once

#include <random>
#include <stdexcept>
#include <utility>

#include "cachelib/navy/admission_policy/AdmissionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
// RejectRandomAP is a cache admission policy. It accepts items with certain
// probability.
class RejectRandomAP final : public AdmissionPolicy {
 public:
  struct Config {
    // Accept probability, must be in [0, 1] range.
    double probability{};
    // Random number generator seed
    uint32_t seed{1};

    Config& validate();
  };

  // Contructor can throw std::exception if config is invalid.
  //
  // @param config  config that was validated with Config::validate
  //
  // @throw std::invalid_argument on bad config
  explicit RejectRandomAP(Config&& config);
  RejectRandomAP(const RejectRandomAP&) = delete;
  RejectRandomAP& operator=(const RejectRandomAP&) = delete;
  ~RejectRandomAP() override = default;

  // See AdmissionPolicy
  bool accept(HashedKey hk, BufferView value) override;

  // See AdmissionPolicy
  void reset() override {}

  void getCounters(const CounterVisitor& /* visitor */) const override {
    // Noop
  }

 private:
  struct ValidConfigTag {};

  RejectRandomAP(Config&& config, ValidConfigTag);

  const double probability_{};
  std::minstd_rand rg_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
