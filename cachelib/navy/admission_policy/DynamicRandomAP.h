#pragma once

#include <folly/SharedMutex.h>

#include <chrono>
#include <random>
#include <stdexcept>

#include "cachelib/navy/admission_policy/AdmissionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
/**
 * Rejects randomly and probability of rejection adjusts real-time to achieve
 * a target rate.
 *
 * Probability = baseProbability * probabilityFactor
 *
 * baseProbability is a essentially a 1/x curve where x is the size of the item.
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

    // Throws if invalid config
    Config& validate();
  };

  // Contructor can throw std::exception
  explicit DynamicRandomAP(Config&& config)
      : DynamicRandomAP{std::move(config.validate()), ValidConfigTag{}} {}

  DynamicRandomAP(const DynamicRandomAP&) = delete;
  DynamicRandomAP& operator=(const DynamicRandomAP&) = delete;
  ~DynamicRandomAP() override = default;

  bool accept(HashedKey hk, BufferView value) override;
  // Must be called non-concurrently
  void reset() override;
  void getCounters(const CounterVisitor& visitor) const override;
  void update();

 private:
  struct ValidConfigTag {};
  struct ThrottleParams {
    double probabilityFactor{1};
    uint64_t curTargetRate{0};
    uint64_t bytesWrittenLastUpdate{0};
    std::chrono::seconds updateTime{0};
  };

  DynamicRandomAP(Config&& config, ValidConfigTag);

  double getBaseProbability(uint64_t size) const;
  void updateThrottleParams(std::chrono::seconds curTime);
  ThrottleParams getThrottleParams() const;
  double clampFactorChange(double change) const;
  double genF(const HashedKey& hk) const;

  const uint64_t targetRate_{};
  const std::chrono::seconds updateInterval_{};
  const uint32_t baseProbabilityMultiplier_{};
  const double probabilitySeed_{};
  const double baseProbabilityExponent_{};
  const double maxChange_{};
  const double minChange_{};
  const FnBytesWritten fnBytesWritten_;

  mutable std::minstd_rand rg_;
  const size_t deterministicKeyHashSuffixLength_{0};

  std::chrono::seconds startupTime_{0};
  mutable folly::SharedMutex mutex_;
  ThrottleParams params_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
