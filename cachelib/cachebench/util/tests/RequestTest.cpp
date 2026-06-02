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

#include "cachelib/cachebench/util/Request.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace {

void expectCommonMovedFields(
    const Request& moved,
    std::vector<size_t>& sizes,
    const std::unordered_map<std::string, std::string>& admFeatureMap) {
  EXPECT_EQ(sizes.begin(), moved.sizeBegin);
  EXPECT_EQ(sizes.end(), moved.sizeEnd);
  EXPECT_EQ(123, moved.ttlSecs);
  EXPECT_TRUE(moved.requestId.has_value());
  EXPECT_EQ(456, moved.requestId.value());
  EXPECT_EQ(admFeatureMap, moved.getAdmFeatureMap());
  EXPECT_EQ(789, moved.timestamp);
  EXPECT_EQ("item-value", moved.itemValue);
  EXPECT_EQ(OpType::kDel, moved.getOp());
}

} // namespace

TEST(RequestTest, MovePreservesFields) {
  std::string key{"key"};
  std::vector<size_t> sizes{10, 20};
  const std::unordered_map<std::string, std::string> admFeatureMap{
      {"numeric-feature", "1.25"}, {"categorical-feature", "photo"}};
  Request request{
      key,           sizes.begin(), sizes.end(),  OpType::kSet,
      /*ttl=*/123,
      /*reqId=*/456, admFeatureMap, "item-value",
  };
  request.onlineKeyString = "online-key";
  request.key = request.onlineKeyString;
  request.ttlSecs = 123;
  request.timestamp = 789;
  request.setOp(OpType::kDel);

  Request moved{std::move(request)};

  EXPECT_EQ("online-key", moved.key);
  EXPECT_EQ("online-key", moved.onlineKeyString);
  expectCommonMovedFields(moved, sizes, admFeatureMap);
}

TEST(RequestTest, MovePreservesExternalKeyView) {
  std::string key{"external-key"};
  std::vector<size_t> sizes{10, 20};
  const std::unordered_map<std::string, std::string> admFeatureMap{
      {"numeric-feature", "1.25"}, {"categorical-feature", "photo"}};
  Request request{
      key,           sizes.begin(), sizes.end(),  OpType::kSet,
      /*ttl=*/123,
      /*reqId=*/456, admFeatureMap, "item-value",
  };
  request.onlineKeyString = "online-key";
  request.ttlSecs = 123;
  request.timestamp = 789;
  request.setOp(OpType::kDel);

  Request moved{std::move(request)};

  EXPECT_EQ("external-key", moved.key);
  EXPECT_EQ(static_cast<const void*>(key.data()),
            static_cast<const void*>(moved.key.data()));
  EXPECT_EQ("online-key", moved.onlineKeyString);
  expectCommonMovedFields(moved, sizes, admFeatureMap);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
