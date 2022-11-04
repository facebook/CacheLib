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

#include "cachelib/cachebench/consistency/RingBuffer.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {
TEST(RingBuffer, Overflow) {
  RingBuffer<uint32_t, 3> rb;
  EXPECT_EQ(0, rb.size());
  EXPECT_EQ(0, rb.write(10));
  EXPECT_EQ(1, rb.write(11));
  EXPECT_EQ(2, rb.write(12));
  EXPECT_EQ(3, rb.size());
  EXPECT_THROW(rb.write(13), std::runtime_error);
}

// Test read/write and buffer wrap
TEST(RingBuffer, Underflow) {
  RingBuffer<uint32_t, 3> rb;
  EXPECT_EQ(0, rb.size());
  EXPECT_EQ(0, rb.write(10));
  EXPECT_EQ(1, rb.size());
  EXPECT_EQ(10, rb.read());
  EXPECT_EQ(0, rb.size());
  EXPECT_THROW(rb.read(), std::runtime_error);
}

TEST(RingBuffer, ReadRewrite) {
  RingBuffer<uint32_t, 3> rb;
  EXPECT_EQ(0, rb.write(10));
  EXPECT_EQ(1, rb.write(11));
  EXPECT_EQ(2, rb.write(12));

  EXPECT_EQ(10, rb.getAt(0));
  EXPECT_EQ(11, rb.getAt(1));
  EXPECT_EQ(12, rb.getAt(2));

  EXPECT_EQ(10, rb.read());

  EXPECT_EQ(11, rb.getAt(1));
  EXPECT_EQ(12, rb.getAt(2));
  EXPECT_THROW(rb.getAt(0), std::out_of_range);
  EXPECT_THROW(rb.getAt(3), std::out_of_range);

  EXPECT_EQ(3, rb.write(13));
  EXPECT_EQ(11, rb.getAt(1));
  EXPECT_EQ(12, rb.getAt(2));
  EXPECT_EQ(13, rb.getAt(3));
  rb.setAt(3, 14);
  EXPECT_EQ(14, rb.getAt(3));

  EXPECT_EQ(3, rb.size());
  EXPECT_EQ(11, rb.read());
  EXPECT_EQ(12, rb.read());
  EXPECT_EQ(14, rb.read());
  EXPECT_EQ(0, rb.size());
}
} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
