/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"

#include <folly/Format.h>
#include <folly/hash/Hash.h>
#include <folly/lang/Bits.h>

#include <algorithm>
#include <shared_mutex>
#include <utility>

#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

namespace {
// Clamp the input into range [lower, upper]
double clamp(double input, double lower, double upper) {
  return std::min(std::max(input, lower), upper);
}
} // namespace
DynamicRandomAP::Config& DynamicRandomAP::Config::validate() {
  if (targetRate == 0) {
    throw std::invalid_argument{folly::sformat(
        "Target rate must be greater than 0. Target rate: {}", targetRate)};
  }
  if (updateInterval == std::chrono::seconds{0}) {
    throw std::invalid_argument{
        folly::sformat("Update interval must be greater than 0. Interval: {}",
                       updateInterval.count())};
  }
  if (probabilitySeed <= 0) {
    throw std::invalid_argument{folly::sformat(
        "Probability seed must be greater than 0. Seed: {}", probabilitySeed)};
  }
  if (baseSize == 0) {
    throw std::invalid_argument{folly::sformat(
        "Base size must be greater than 0. Base size: {}", baseSize)};
  }
  if (!between(probabilitySizeDecay, 0, 1)) {
    throw std::invalid_argument{folly::sformat(
        "Probability size decay must be in range [0, 1]. Decay: {}",
        probabilitySizeDecay)};
  }
  if (!betweenStrict(changeWindow, 0, 1)) {
    throw std::invalid_argument{folly::sformat(
        "Change window must be in range (0, 1). Change: {}", changeWindow)};
  }
  if (!fnBytesWritten) {
    throw std::invalid_argument{"fnBytesWritten function required"};
  }
  return *this;
}

DynamicRandomAP::DynamicRandomAP(Config&& config)
    : DynamicRandomAP{std::move(config.validate()), ValidConfigTag{}} {}

DynamicRandomAP::DynamicRandomAP(Config&& config, ValidConfigTag)
    : targetRate_{config.targetRate},
      maxRate_{config.maxRate},
      updateInterval_{config.updateInterval},
      baseProbabilityMultiplier_{folly::findLastSet(config.baseSize)},
      probabilitySeed_{config.probabilitySeed},
      baseProbabilityExponent_{config.probabilitySizeDecay},
      maxChange_{1.0 + config.changeWindow},
      minChange_{1.0 - config.changeWindow},
      fnBytesWritten_{std::move(config.fnBytesWritten)},
      lowerBound_{config.probFactorLowerBound},
      upperBound_{config.probFactorUpperBound},
      rg_{config.seed},
      deterministicKeyHashSuffixLength_{
          config.deterministicKeyHashSuffixLength} {
  reset();
  XLOGF(INFO,
        "DynamicRandomAP: target rate {} byte/s, update interval {} s. "
        "maxChange {}, minChange {}, kUpperBound_ {}, kLowerBound_ {}.",
        targetRate_, updateInterval_.count(), maxChange_, minChange_,
        kUpperBound_, kLowerBound_);
}

bool DynamicRandomAP::accept(HashedKey hk, BufferView value) {
  const auto curTime = getSteadyClockSeconds();
  auto params = getThrottleParams();
  if (curTime - params.updateTime >= updateInterval_) {
    // Lots of threads can get into this section. First to grab the lock will
    // update. Let proceed the rest.
    std::unique_lock<folly::SharedMutex> lock{mutex_, std::try_to_lock};
    if (lock.owns_lock()) {
      updateThrottleParams(curTime);
    }
  }

  auto baseProb = getBaseProbability(hk.key().size() + value.size());
  baseProbStats_.trackValue(baseProb * 100);
  auto probability = baseProb * params.probabilityFactor;
  probability = std::max(0.0, std::min(1.0, probability));

  return probability == 1 || genF(hk) < probability;
}

// generate a value between [0, 1)
double DynamicRandomAP::genF(const HashedKey& hk) const {
  if (deterministicKeyHashSuffixLength_) {
    auto key = hk.key();
    size_t len = key.size() > deterministicKeyHashSuffixLength_
                     ? key.size() - deterministicKeyHashSuffixLength_
                     : key.size();
    uint64_t h =
        folly::hash::SpookyHashV2::Hash64(key.data(), len, 0 /* seed */);
    return fdiv(static_cast<double>(h),
                static_cast<double>(std::numeric_limits<decltype(h)>::max()));
  }
  return fdiv(static_cast<double>(rg_()), static_cast<double>(rg_.max()));
}

void DynamicRandomAP::reset() {
  startupTime_ = getSteadyClockSeconds();

  params_.updateTime = startupTime_;
  params_.probabilityFactor = probabilitySeed_;
  params_.curTargetRate = 0;
  params_.bytesWrittenLastUpdate = fnBytesWritten_();
}

void DynamicRandomAP::update() {
  std::unique_lock<folly::SharedMutex> lock{mutex_};
  updateThrottleParams(getSteadyClockSeconds());
}

void DynamicRandomAP::updateThrottleParams(std::chrono::seconds curTime) {
  constexpr uint64_t kSecondsInDay{3600 * 24};

  auto updateTimeDelta = (curTime - params_.updateTime).count();
  // in case this is the first update, or we are in unit test where curTime is
  // set arbitrarily.
  if (updateTimeDelta <= 0) {
    updateTimeDelta = updateInterval_.count();
  }

  params_.updateTime = curTime;
  auto bytesWritten = fnBytesWritten_();
  params_.observedCurRate_ =
      (bytesWritten - params_.bytesWrittenLastUpdate) / updateTimeDelta;
  if (params_.observedCurRate_ == 0) {
    return;
  }

  auto secondsElapsed = (curTime - startupTime_).count();
  auto targetWrittenTomorrow = targetRate_ * (secondsElapsed + kSecondsInDay);
  uint64_t curTargetRate{0};
  if (bytesWritten < targetWrittenTomorrow) {
    // Write rate needed to achieve @targetWrittenTomorrow given that we have
    // currently written @bytesWritten.
    curTargetRate = (targetWrittenTomorrow - bytesWritten) / kSecondsInDay;
  }

  if (maxRate_ > 0 && curTargetRate > maxRate_) {
    XLOGF(INFO,
          "max write rate {} will be used because target current write rate {} "
          "exceeds it.",
          maxRate_, curTargetRate);
    curTargetRate = maxRate_;
  }
  params_.curTargetRate = curTargetRate;

  auto rawProbFactor = fdiv(static_cast<double>(curTargetRate),
                            static_cast<double>(params_.observedCurRate_));
  params_.probabilityFactor =
      clampFactor(params_.probabilityFactor * clampFactorChange(rawProbFactor));
  XLOG_EVERY_MS(INFO, 60000)
      << "observed current write rate = " << params_.observedCurRate_
      << ", target current rate = " << curTargetRate
      << " rawProbFactor = " << rawProbFactor
      << ", probFactor = " << params_.probabilityFactor;
  params_.bytesWrittenLastUpdate = bytesWritten;
}

double DynamicRandomAP::clampFactorChange(double change) const {
  return clamp(change, minChange_, maxChange_);
}

double DynamicRandomAP::clampFactor(double factor) const {
  return clamp(factor, lowerBound_, upperBound_);
}

double DynamicRandomAP::getBaseProbability(uint64_t size) const {
  // Get index of most significant bit. This buckets the item sizes.
  uint32_t log2Size = folly::findLastSet(size);
  return std::pow(fdiv(baseProbabilityMultiplier_, log2Size),
                  baseProbabilityExponent_);
}

DynamicRandomAP::ThrottleParams DynamicRandomAP::getThrottleParams() const {
  std::shared_lock<folly::SharedMutex> lock{mutex_};
  return params_;
}

void DynamicRandomAP::getCounters(const CounterVisitor& visitor) const {
  auto params = getThrottleParams();
  visitor("navy_ap_write_rate_target_configured",
          static_cast<double>(targetRate_));
  visitor("navy_ap_write_rate_max_configured", static_cast<double>(maxRate_));
  visitor("navy_ap_write_rate_adjusted_target",
          static_cast<double>(params.curTargetRate));
  visitor("navy_ap_prob_factor_x100", params_.probabilityFactor * 100);
  baseProbStats_.visitQuantileEstimator(visitor, "navy_ap_baseProx_x100");
  visitor("navy_ap_write_rate_observed",
          static_cast<double>(params_.observedCurRate_));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
