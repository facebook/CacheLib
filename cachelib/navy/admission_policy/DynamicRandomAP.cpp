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

DynamicRandomAP::DynamicRandomAP(Config&& config, ValidConfigTag)
    : targetRate_{config.targetRate},
      updateInterval_{config.updateInterval},
      baseProbabilityMultiplier_{folly::findLastSet(config.baseSize)},
      probabilitySeed_{config.probabilitySeed},
      baseProbabilityExponent_{config.probabilitySizeDecay},
      maxChange_{1.0 + config.changeWindow},
      minChange_{1.0 - config.changeWindow},
      fnBytesWritten_{std::move(config.fnBytesWritten)},
      rg_{config.seed},
      deterministicKeyHashSuffixLength_{
          config.deterministicKeyHashSuffixLength} {
  reset();
  XLOGF(INFO,
        "DynamicRandomAP: target rate {} byte/s, update interval {} s",
        targetRate_,
        updateInterval_.count());
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

  auto probability = getBaseProbability(hk.key().size() + value.size()) *
                     params.probabilityFactor;
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

  params_.updateTime = curTime;
  auto bytesWritten = fnBytesWritten_();
  auto curRate =
      (bytesWritten - params_.bytesWrittenLastUpdate) / updateInterval_.count();
  if (curRate == 0) {
    return;
  }

  auto secondsElapsed = (curTime - startupTime_).count();
  auto targetWrittenTomorrow = targetRate_ * (secondsElapsed + kSecondsInDay);
  uint64_t curTargetRate{0};
  if (bytesWritten < targetWrittenTomorrow) {
    // Write rate needed to acheive @targetWrittenTomorrow given that we have
    // currently written @bytesWritten.
    curTargetRate = (targetWrittenTomorrow - bytesWritten) / kSecondsInDay;
  }
  params_.curTargetRate = curTargetRate;
  params_.probabilityFactor *= clampFactorChange(
      fdiv(static_cast<double>(curTargetRate), static_cast<double>(curRate)));
  params_.bytesWrittenLastUpdate = bytesWritten;
}

double DynamicRandomAP::clampFactorChange(double change) const {
  // Avoid changing probability factor too drastically
  if (change < minChange_) {
    return minChange_;
  }
  if (change > maxChange_) {
    return maxChange_;
  }
  return change;
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
  visitor("navy_ap_write_rate_target", static_cast<double>(targetRate_));
  visitor("navy_ap_write_rate_current",
          static_cast<double>(params.curTargetRate));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
