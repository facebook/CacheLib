#include <chrono>
#include <thread>

#include <folly/Random.h>
#include <folly/logging/xlog.h>
#include <gtest/gtest.h>

#include "cachelib/common/TestUtils.h"

namespace facebook {
namespace cachelib {
namespace test_util {

namespace {

constexpr uint64_t kDefaultTimeoutSecs = 30;

/*
 * This is the core eventually routine.  The others are implemented
 * by calling this one.
 */
::testing::AssertionResult eventuallyTrue(
    std::function<::testing::AssertionResult(bool)> test,
    uint64_t timeoutSecs) {
  constexpr uint64_t kMicrosPerSec = 1000000;
  const uint64_t timeoutUSec = timeoutSecs * kMicrosPerSec;
  constexpr uint64_t kTriesPerSec = 10;
  auto sleepTime = std::chrono::microseconds(kMicrosPerSec / kTriesPerSec);
  auto startTime = std::chrono::steady_clock::now();
  auto now = startTime;

  while (true) {
    bool last_time = (now - startTime) > std::chrono::microseconds(timeoutUSec);
    try {
      ::testing::AssertionResult res = test(last_time);
      if (res || last_time) {
        return res;
      }
    } catch (...) {
      if (last_time) {
        throw;
      }
    }
    std::this_thread::sleep_for(sleepTime);
    now = std::chrono::steady_clock::now();
  }
  /* Should never get here, but the compiler isn't that smart */
  return ::testing::AssertionFailure();
}

} // namespace

/*
 * This variant is for test functions that take no arguments.
 */
bool eventuallyTrue(std::function<bool(void)> test, uint64_t timeoutSecs) {
  return eventuallyTrue(
      [&](bool /* last_time */) {
        return test() ? ::testing::AssertionSuccess()
                      : ::testing::AssertionFailure();
      },
      timeoutSecs);
}

/*
 * This variant is for code that doesn't use ::testing::AssertionResult.
 */
bool eventuallyZero(std::function<int(bool)> test) {
  return eventuallyTrue(
      [&](bool last_time) {
        if (test(last_time) == 0) {
          return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure();
      },
      kDefaultTimeoutSecs);
}

std::string getRandomAsciiStr(unsigned int len) {
  std::string s;
  static const char start[] = {'a', 'A', '0'};
  static const int size[] = {26, 26, 10};

  for (unsigned int i = 0; i < len; i++) {
    unsigned int index =
        folly::Random::rand32() % (sizeof(start) / sizeof(start[0]));
    const char c = (start[index] +
                    static_cast<char>((folly::Random::rand32() % size[index])));
    s += c;
  }
  XDCHECK_EQ(s.length(), len);
  return s;
}

} // namespace test_util
} // namespace cachelib
} // namespace facebook
