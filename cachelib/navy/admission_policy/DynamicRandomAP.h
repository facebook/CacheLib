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

#include <folly/SharedMutex.h>

#include <chrono>
#include <random>
#include <stdexcept>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/admission_policy/AdmissionPolicy.h"
#include "gtest/gtest_prod.h"

namespace facebook {
namespace cachelib {
namespace navy {
/**
 * Rejects randomly and probability of rejection adjusts real-time to achieve
 * a target rate.
 *
 * Probability = baseProbability * probabilityFactor
 *
 * baseProbability is essentially a 1/x curve where x is the size of the item.
 * This means that larger items are penalized. The reasoning is that allowing
 * more small items increases hit ratio. Hits are measured per item, not by
 * size, so more items will result in more hits.
 *
 * probabilityFactor = curTargetRate_ / curRate where curTargetRate_ is the
 * rate needed to achieve the target culmative bytes written 24 h in the future
 * based on the current culmative bytes written. curRate is calculated over
 * updateInterval. This increases acceptance probability when we are under the
 * targetRate and decreases the probability when we are over.
 */
class DynamicRandomAP final : public AdmissionPolicy {
 public:
  using FnBytesWritten = std::function<uint64_t()>;
  using FnBypass = std::function<bool(folly::StringPiece)>;

  struct Config {
    // Target write rate in byte/s
    uint64_t targetRate{0};

    // Interval to update probability factor
    std::chrono::seconds updateInterval{60};

    // Initial value for probability factor. Must be > 0.
    double probabilitySeed{1};

    // Set base probability = 1 for this size. Must be > 0.
    uint32_t baseSize{4096};

    // Exponent to make the probability curve shallower so larger items are not
    // penalized as heavily. Must be between 0 and 1.
    double probabilitySizeDecay{0.3};

    // Probability factor change window, a (0, 1) fraction:
    // [1 - @changeWindow, 1 + @changeWindow]
    double changeWindow{0.25};

    // Function that returns number of bytes written to device
    FnBytesWritten fnBytesWritten;

    // Random number generator seed
    uint32_t seed{1};

    // when enabled(non-zero) uses the hash of the key and deterministically
    // admits/rejects by the hash
    size_t deterministicKeyHashSuffixLength{0};

    // The max write rate target. 0 means disabled.
    // If the target write rate caluclated by write budget exceeds this, use
    // this rate as the target to adjust. By default this is 160MB which is
    // higher than the usual 120MB endurance limit. This gives some leeway for
    // Navy to use up its spare write budget when it wrote less than endurance
    // limit during trough in the traffic.
    uint64_t maxRate{160 * 1024 * 1024};

    // Lower bound and upper bound of the probabitliy factor
    double probFactorLowerBound{kLowerBound_};

    double probFactorUpperBound{kUpperBound_};

    FnBypass fnBypass;

    // Throws if invalid config
    Config& validate();
  };

  // Contructor can throw std::exception if config is invalid.
  //
  // @param config  config that was validated with Config::validate
  //
  // @throw std::invalid_argument on bad config.
  explicit DynamicRandomAP(Config&& config);
  DynamicRandomAP(const DynamicRandomAP&) = delete;
  DynamicRandomAP& operator=(const DynamicRandomAP&) = delete;
  ~DynamicRandomAP() override = default;

  // Whether to accept the given hashed key.
  // The value is used to get size based probability factor.
  bool accept(HashedKey hk, BufferView value) override;

  // Reset the throttling parameters update cycle.
  // Not thread safe.
  void reset() override;
  // Get stats counters to export.
  void getCounters(const CounterVisitor& visitor) const override;

  void setMaxWriteRate(uint64_t maxRate) {
    maxRate_ = maxRate;
    update();
  }

 private:
  struct ValidConfigTag {};
  // Parameters to perform throttling.
  // This struct should be kept compact for a fast copy on read.
  struct ThrottleParams {
    double probabilityFactor{1};

    std::chrono::seconds updateTime{0};
  };

  // Stats about writes in the past update window for updating ThrottleParams
  struct WriteStats {
    // The rate we can write at, given how much we've written since the host
    // started, from a quota standpoint.
    // This number is the sum of bypassed traffic and throttled regular traffic.
    uint64_t curTargetRate{0};
    // The rate that we observe since the last parameter update.
    uint64_t observedCurRate{0};
    uint64_t bytesWrittenLastUpdate{0};

    // The rate of bypassed and accepted accounted at admission time.
    uint64_t bypassedRate{0};
    uint64_t acceptedRate{0};
    uint64_t bytesBypassedLastUpdate{0};
    uint64_t bytesAcceptedLastUpdate{0};
  };

  DynamicRandomAP(Config&& config, ValidConfigTag);

  // Return a copy of the parameters.
  ThrottleParams getThrottleParams() const;
  WriteStats getWriteStats() const;

  double getBaseProbability(uint64_t size) const;
  void updateThrottleParamsLocked(std::chrono::seconds curTime);
  double clampFactorChange(double change) const;
  double clampFactor(double factor) const;
  double genF(const HashedKey& hk) const;

  // Perform update to the throttling parameters.
  // Not thread safe.
  void update();

  // The rate we are configered to write on average over a day.
  const uint64_t targetRate_{};
  // The rate we are confiigured to write at most.
  std::atomic<uint64_t> maxRate_{};
  const std::chrono::seconds updateInterval_{};
  const uint32_t baseProbabilityMultiplier_{};
  const double probabilitySeed_{};
  const double baseProbabilityExponent_{};
  const double maxChange_{};
  const double minChange_{};
  const FnBytesWritten fnBytesWritten_;
  const double lowerBound_{};
  const double upperBound_{};
  const FnBypass fnBypass_;

  mutable TLCounter acceptedBytes_;
  mutable TLCounter bypassedBytes_;

  mutable std::minstd_rand rg_;
  const size_t deterministicKeyHashSuffixLength_{0};

  std::chrono::seconds startupTime_{0};
  mutable folly::SharedMutex mutex_;
  ThrottleParams params_;
  WriteStats writeStats_;

  // probabilityFactor would always be in [lowerBound_, upperBound_]
  static constexpr double kLowerBound_{0.001};
  static constexpr double kUpperBound_{10.0};
  FRIEND_TEST(DynamicRandomAPTest, AboveTarget);
  FRIEND_TEST(DynamicRandomAPTest, BelowTarget);
  FRIEND_TEST(DynamicRandomAPTest, StayInRange);
  FRIEND_TEST(DynamicRandomAPTest, RespectMaxWriteRate);

  FRIEND_TEST(DynamicRandomAPTest, AcceptedBytesCount);
  FRIEND_TEST(DynamicRandomAPTest, ThrottleRegular);
  FRIEND_TEST(DynamicRandomAPTest, ThrottleAll);
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
