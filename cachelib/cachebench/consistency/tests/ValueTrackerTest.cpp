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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/cachebench/consistency/ValueTracker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {
// Test interleaving key events
TEST(ValueTracker, Basic) {
  std::vector<std::string> ss{"cat", "dog"};
  ValueTracker vt{ValueTracker::wrapStrings(ss)};
  auto catSet = vt.beginSet("cat", 0);
  vt.endSet(catSet);
  auto catGet = vt.beginGet("cat");
  vt.beginSet("dog", 0); // Set without end
  auto dogGet = vt.beginGet("dog");
  EXPECT_TRUE(vt.endGet(catGet, 0, true));
  EXPECT_FALSE(vt.endGet(dogGet, 1, true)); // Inconsistent
}
} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
