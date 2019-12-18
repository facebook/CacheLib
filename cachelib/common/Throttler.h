#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <thread>

#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {
namespace util {

class Throttler {
 public:
  // this config indicates that we sleep for sleepMs time every
  // sleepIntervalMs time at least.
  struct Config {
    // time we yield when throttling
    uint64_t sleepMs = 10;

    // how often we check if we need to yield
    uint64_t sleepIntervalMs = 5;

    // return true if the config indicates we need to run throttling logic.
    bool needsThrottling() const noexcept { return sleepMs != 0; }

    static Config makeNoThrottleConfig() {
      // setting to 0 on sleepMs means we dont need to throttle.
      return Config{.sleepMs = 0, .sleepIntervalMs = 0};
    }

    std::map<std::string, std::string> serialize() const {
      std::map<std::string, std::string> configMap;
      configMap["sleepMs"] = std::to_string(sleepMs);
      configMap["sleepIntervalMs"] = std::to_string(sleepIntervalMs);
      return configMap;
    }
  };

  // returns true if throttled, false otherwise
  bool throttle() {
    if (!config_.needsThrottling() || ++counter_ % kSpinLimit) {
      return false;
    }

    uint64_t curr = util::getCurrentTimeMs();
    if (curr - lastSleptTimeMs_ > config_.sleepIntervalMs) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.sleepMs));
      lastSleptTimeMs_ = curr;
      ++throttleCounter_;
      return true;
    }
    return false;
  }

  uint64_t numThrottles() const noexcept { return throttleCounter_; }

  explicit Throttler(Config config)
      : config_(std::move(config)),
        lastSleptTimeMs_(util::getCurrentTimeMs()) {}
  explicit Throttler() : Throttler(Config{}) {}

 private:
  // number of spins before we attempt to call time
  static constexpr const uint64_t kSpinLimit = 1024;

  Config config_;
  uint64_t lastSleptTimeMs_;    // last instance where we yielded
  uint64_t counter_{0};         // counter to track the calls.
  uint64_t throttleCounter_{0}; // number of times we've throttled
};

} // namespace util
} // namespace cachelib
} // namespace facebook
