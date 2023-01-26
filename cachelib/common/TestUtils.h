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

#pragma once

#include <cstdint>
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
