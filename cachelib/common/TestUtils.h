#pragma once

#include <functional>

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

#define ASSERT_EVENTUALLY_TRUE(fn, ...) \
  ASSERT_TRUE(eventuallyTrue(fn, ##__VA_ARGS__))
