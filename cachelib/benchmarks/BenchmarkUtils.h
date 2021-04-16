#pragma once

#include <folly/Format.h>

#include <chrono>
#include <string>

namespace facebook {
namespace cachelib {
class Timer {
 public:
  explicit Timer(std::string name, uint64_t ops)
      : name_{std::move(name)}, ops_{ops} {
    startTime_ = std::chrono::system_clock::now();
    startCycles_ = __rdtsc();
  }
  ~Timer() {
    endTime_ = std::chrono::system_clock::now();
    endCycles_ = __rdtsc();

    std::chrono::nanoseconds durationTime = endTime_ - startTime_;
    uint64_t durationCycles = endCycles_ - startCycles_;
    std::cout << folly::sformat("[{: <60}] Per-Op: {: <5} ns, {: <5} cycles",
                                name_, durationTime.count() / ops_,
                                durationCycles / ops_)
              << std::endl;
  }

 private:
  const std::string name_;
  const uint64_t ops_;
  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  uint64_t startCycles_;
  uint64_t endCycles_;
};

void printMsg(std::string msg) {
  std::cout << folly::sformat("--------{:-<92}", folly::sformat(" {} ", msg))
            << std::endl;
}
} // namespace cachelib
} // namespace facebook
