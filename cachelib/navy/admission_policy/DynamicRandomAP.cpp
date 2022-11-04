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

// Returns the difference between a and b if non-negative. Otherwise return 0.
uint64_t minus(uint64_t a, uint64_t b) { return a < b ? 0ULL : a - b; }
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
      fnBypass_{std::move(config.fnBypass)},
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
  if (curTime - params_.updateTime >= updateInterval_) {
    // Lots of threads can get into this section. First to grab the lock will
    // update. Let proceed the rest.
    std::unique_lock<folly::SharedMutex> lock{mutex_, std::try_to_lock};
    if (lock.owns_lock()) {
      updateThrottleParamsLocked(curTime);
    }
  }
  uint64_t size = hk.key().size() + value.size();
  if (fnBypass_ && fnBypass_(hk.key())) {
    bypassedBytes_.add(size);
    return true;
  }

  auto baseProb = getBaseProbability(size);
  auto probability = baseProb * params_.probabilityFactor;
  probability = std::max(0.0, std::min(1.0, probability));

  bool accepted = probability == 1 || genF(hk) < probability;
  if (accepted) {
    acceptedBytes_.add(size);
  }
  return accepted;
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
  writeStats_.curTargetRate = 0;
  writeStats_.bytesWrittenLastUpdate = fnBytesWritten_();
  writeStats_.bytesAcceptedLastUpdate = acceptedBytes_.get();
  writeStats_.bytesBypassedLastUpdate = bypassedBytes_.get();
}

void DynamicRandomAP::update() {
  std::unique_lock<folly::SharedMutex> lock{mutex_};
  updateThrottleParamsLocked(getSteadyClockSeconds());
}

void DynamicRandomAP::updateThrottleParamsLocked(std::chrono::seconds curTime) {
  constexpr uint64_t kSecondsInDay{3600 * 24};

  auto updateTimeDelta = (curTime - params_.updateTime).count();
  // in case this is the first update, or we are in unit test where curTime is
  // set arbitrarily.
  if (updateTimeDelta <= 0) {
    updateTimeDelta = updateInterval_.count();
  }

  params_.updateTime = curTime;
  auto bytesWritten = fnBytesWritten_();
  auto bytesWrittenDiff = bytesWritten - writeStats_.bytesWrittenLastUpdate;
  writeStats_.observedCurRate = bytesWrittenDiff / updateTimeDelta;
  // There is no bytes observed from device, skip this update.
  if (writeStats_.observedCurRate == 0) {
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
  writeStats_.curTargetRate = curTargetRate;

  // Find the target rate for regular traffic.
  // First calculate accepted and bypassed rate in the past window.
  auto acceptedBytes = acceptedBytes_.get();
  auto acceptedBytesDiff = acceptedBytes - writeStats_.bytesAcceptedLastUpdate;
  writeStats_.acceptedRate = acceptedBytesDiff / updateTimeDelta;
  writeStats_.bytesAcceptedLastUpdate = acceptedBytes;

  auto bypassedBytes = bypassedBytes_.get();
  auto bypassedBytesDiff = bypassedBytes - writeStats_.bytesBypassedLastUpdate;
  writeStats_.bypassedRate = bypassedBytesDiff / updateTimeDelta;
  writeStats_.bytesBypassedLastUpdate = bypassedBytes;

  auto totalBytes = acceptedBytesDiff + bypassedBytesDiff;

  // Calculate the target rate and observed rate for regular traffic.
  uint64_t trueTargetRate = 0;
  uint64_t trueObservedRate = 0;
  // There's no accepted/bypassed traffic but observed some writes. Treat those
  // as bypassed traffic.
  if (totalBytes == 0) {
    trueTargetRate = minus(curTargetRate, writeStats_.observedCurRate);
    trueObservedRate = 0ULL;
  } else {
    // Ratio of regular traffic over all accepted traffic.
    double ratio = fdiv(acceptedBytesDiff, totalBytes);
    // Write ampflication factor: the ratio between bytes recorded by the device
    // and bytes recorded by admission policy.
    // There is another write amplification from bytes recorded by the device to
    // the actual physical writes, which is not being accounted here.
    double waf = fdiv(bytesWrittenDiff, totalBytes);

    trueTargetRate = minus(writeStats_.curTargetRate,
                           bypassedBytesDiff * waf / updateTimeDelta);
    trueObservedRate = writeStats_.observedCurRate * ratio;
  }
  // There is no observed regular traffic. Skip this update.
  if (trueObservedRate == 0ULL) {
    return;
  }
  auto rawProbFactor = fdiv(static_cast<double>(trueTargetRate),
                            static_cast<double>(trueObservedRate));
  params_.probabilityFactor =
      clampFactor(params_.probabilityFactor * clampFactorChange(rawProbFactor));
  XLOG_EVERY_MS(INFO, 60000)
      << "observed current write rate = " << writeStats_.observedCurRate
      << ", target current rate = " << curTargetRate
      << " rawProbFactor = " << rawProbFactor
      << ", probFactor = " << params_.probabilityFactor;
  writeStats_.bytesWrittenLastUpdate = bytesWritten;
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
  return params_;
}

DynamicRandomAP::WriteStats DynamicRandomAP::getWriteStats() const {
  return writeStats_;
}

void DynamicRandomAP::getCounters(const CounterVisitor& visitor) const {
  auto writeStats = getWriteStats();
  visitor("navy_ap_write_rate_target_configured",
          static_cast<double>(targetRate_));
  visitor("navy_ap_write_rate_max_configured", static_cast<double>(maxRate_));
  visitor("navy_ap_write_rate_adjusted_target",
          static_cast<double>(writeStats.curTargetRate));
  visitor("navy_ap_prob_factor_x100", params_.probabilityFactor * 100);
  visitor("navy_ap_write_rate_observed",
          static_cast<double>(writeStats.observedCurRate));

  visitor("navy_ap_accepted_rate",
          static_cast<double>(writeStats.acceptedRate));
  visitor("navy_ap_bypassed_rate",
          static_cast<double>(writeStats.bypassedRate));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
