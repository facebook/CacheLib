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

#include "cachelib/navy/testing/BufferGen.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
bool isRange(uint8_t b, uint8_t rmin, uint8_t rmax) {
  return rmin <= b && b <= rmax;
}

bool isAlphaChar(uint8_t b) {
  return isRange(b, 'a', 'z') || isRange(b, 'A', 'Z') || isRange(b, '0', '9') ||
         b == '+' || b == '=';
}
} // namespace

TEST(BufferGen, Basic) {
  BufferGen bg{1};
  uint32_t count[10]{};
  for (uint32_t i = 0; i < 100; i++) {
    auto buf = bg.gen(4, 8);
    EXPECT_LE(4, buf.size());
    EXPECT_GT(8, buf.size());
    count[buf.size()]++;
    for (size_t j = 0; j < buf.size(); j++) {
      EXPECT_TRUE(isAlphaChar(buf.data()[j]));
    }
  }
  EXPECT_LT(0, count[4]);
  EXPECT_LT(0, count[5]);
  EXPECT_LT(0, count[6]);
  EXPECT_LT(0, count[7]);
}

TEST(BufferGen, Seed) {
  BufferGen bg1{1};
  auto buf1 = bg1.gen(10);
  BufferGen bg2{1};
  auto buf2 = bg2.gen(10);
  EXPECT_EQ(buf1.view(), buf2.view());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
