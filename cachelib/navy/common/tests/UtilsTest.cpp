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

#include <gtest/gtest.h>

#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Utils, PowTwoAlign) {
  EXPECT_EQ(0, powTwoAlign(0, 16));
  EXPECT_EQ(16, powTwoAlign(1, 16));
  EXPECT_EQ(16, powTwoAlign(2, 16));
  EXPECT_EQ(16, powTwoAlign(15, 16));
  EXPECT_EQ(16, powTwoAlign(16, 16));
  EXPECT_EQ(32, powTwoAlign(17, 16));
}

TEST(Utils, Between) {
  EXPECT_TRUE(between(0.4, 0, 1));
  EXPECT_TRUE(between(1.0, 0, 1));
  EXPECT_TRUE(between(0.0, 0, 1));
  EXPECT_TRUE(betweenStrict(0.4, 0, 1));
  EXPECT_FALSE(betweenStrict(1.0, 0, 1));
  EXPECT_FALSE(betweenStrict(0.0, 0, 1));
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
