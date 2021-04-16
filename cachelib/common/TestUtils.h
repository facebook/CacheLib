#pragma once

#include <functional>
#include <set>
#include <string>

namespace facebook {
namespace cachelib {
namespace test_util {

/**
 * Calls test() repeatedly until it returns true or timeout secs have elapsed
 */
bool eventuallyTrue(std::function<bool(void)> test, uint64_t timeoutSecs = 60);

/**
 * Calls test() repeatedly until it returns 0 or a timeout occurs.
 * The test() function takes a bool, which eventuallyZero
 * sets to false, except for the last call where it is true.  This
 * is useful for avoiding printing error messages inside the function,
 * except on the last call.
 */
bool eventuallyZero(std::function<int(bool)> test);

// generates a random string of random length up 50 chars and at least 10
// chars;
std::string getRandomAsciiStr(unsigned int len);

} // namespace test_util
} // namespace cachelib
} // namespace facebook

#define ASSERT_EVENTUALLY_TRUE(fn, ...) \
  ASSERT_TRUE(facebook::cachelib::test_util::eventuallyTrue(fn, ##__VA_ARGS__))

#ifndef ASSERT_THROW_WITH_MSG
#define ASSERT_THROW_WITH_MSG(statement, exception, msg)     \
  do {                                                       \
    try {                                                    \
      statement;                                             \
      ASSERT_TRUE(false) << "Exception not raised: " << msg; \
    } catch (const exception& e) {                           \
      ASSERT_STREQ(msg, e.what());                           \
    }                                                        \
  } while (0)
#endif
