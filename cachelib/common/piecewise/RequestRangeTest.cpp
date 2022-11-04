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

#include <folly/portability/GTest.h>

#include "cachelib/common/piecewise/RequestRange.h"

using facebook::cachelib::RequestRange;

TEST(RequestRangeTests, Parsing) {
  RequestRange hr(0, 499);
  EXPECT_TRUE(hr.getRequestRange().has_value());
  EXPECT_EQ(0, hr.getRequestRange()->first);
  EXPECT_TRUE(hr.getRequestRange()->second.has_value());
  EXPECT_EQ(499, hr.getRequestRange()->second.value());

  hr = RequestRange(500, 999);
  EXPECT_TRUE(hr.getRequestRange().has_value());
  EXPECT_EQ(500, hr.getRequestRange()->first);
  EXPECT_TRUE(hr.getRequestRange()->second.has_value());
  EXPECT_EQ(999, hr.getRequestRange()->second.value());

  hr = RequestRange(folly::none, 500);
  EXPECT_FALSE(hr.getRequestRange().has_value());

  hr = RequestRange(999, 500);
  EXPECT_FALSE(hr.getRequestRange().has_value());

  hr = RequestRange(9500, folly::none);
  EXPECT_TRUE(hr.getRequestRange().has_value());
  EXPECT_EQ(9500, hr.getRequestRange()->first);
  EXPECT_FALSE(hr.getRequestRange()->second.has_value());

  hr = RequestRange(folly::none, folly::none);
  EXPECT_FALSE(hr.getRequestRange().has_value());

  hr = RequestRange(folly::none);
  EXPECT_FALSE(hr.getRequestRange().has_value());
}
