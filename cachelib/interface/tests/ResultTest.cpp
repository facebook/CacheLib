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

#include <sstream>

#include "cachelib/interface/Result.h"

using namespace ::testing;
using namespace facebook::cachelib::interface;

TEST(ResultTest, success) {
  Result<int> r = 42;
  EXPECT_EQ(r.value(), 42);
}

TEST(ResultTest, failure) {
  Result<int> r = makeError(Error::Code::INVALID_CONFIG, "test message");
  EXPECT_EQ(r.error().code_, Error::Code::INVALID_CONFIG);
  EXPECT_EQ(r.error().error_, "test message");
}

TEST(ResultTest, prettyPrintError) {
  Result<int> r = makeError(Error::Code::FIND_FAILED, "test message");

  std::stringstream ss;
  ss << r.error();
  auto expected = fmt::format("Error (FIND_FAILED, code {}): test message",
                              static_cast<int>(Error::Code::FIND_FAILED));
  EXPECT_EQ(ss.str(), expected);
}
