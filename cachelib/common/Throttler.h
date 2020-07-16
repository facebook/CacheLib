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
  // Throttler is used to do certain amount of work before yielding. We call
  // throttle every time we work() and throttler will sleep if we work() too
  // much.  To do this, we have two options. 1) sleepMs 2) workMs.  SleepMs
  // indicates the time we yield when we decide to be throttled.  WorkMs
  // indicates the time we run un-throttled.

  //
  // this config indicates that we sleep for sleepMs time every
  // workMs time at least.
  struct Config {
    // time we yield when throttled
    uint64_t sleepMs = 10;

    // time period of uninterrupted work.
    uint64_t workMs = 5;

    // return true if the config indicates we need to run throttling logic.
    bool needsThrottling() const noexcept { return sleepMs != 0; }

    static Config makeNoThrottleConfig() {
      // setting to 0 on sleepMs means we dont need to throttle.
      return Config{.sleepMs = 0, .workMs = 0};
    }

    std::map<std::string, std::string> serialize() const {
      std::map<std::string, std::string> configMap;
      configMap["sleepMs"] = std::to_string(sleepMs);
      configMap["workMs"] = std::to_string(workMs);
      return configMap;
    }
  };

  // returns true if throttled, false otherwise
  bool throttle() {
    if (!config_.needsThrottling() || ++counter_ % kSpinLimit) {
      return false;
    }

    uint64_t curr = util::getCurrentTimeMs();
    if (curr - currWorkStartMs_ > config_.workMs) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.sleepMs));
      // start the time period when we don't throttle
      currWorkStartMs_ = util::getCurrentTimeMs();
      ++throttleCounter_;
      return true;
    }
    return false;
  }

  uint64_t numThrottles() const noexcept { return throttleCounter_; }

  explicit Throttler(Config config)
      : config_(std::move(config)),
        currWorkStartMs_(util::getCurrentTimeMs()) {}
  explicit Throttler() : Throttler(Config{}) {}

 private:
  // number of spins before we attempt to call time
  static constexpr const uint64_t kSpinLimit = 1024;

  Config config_;
  uint64_t currWorkStartMs_;    // time when we started to not throttle
  uint64_t counter_{0};         // counter to track the calls.
  uint64_t throttleCounter_{0}; // number of times we've throttled
};

} // namespace util
} // namespace cachelib
} // namespace facebook
