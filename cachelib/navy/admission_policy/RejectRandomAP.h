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
