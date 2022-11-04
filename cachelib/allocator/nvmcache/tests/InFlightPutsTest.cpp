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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cachelib/allocator/nvmcache/InFlightPuts.h"

namespace facebook {
namespace cachelib {
namespace tests {

TEST(InFlightPutsTest, FunctionExecution) {
  InFlightPuts p;
  folly::StringPiece key = "foobar";
  auto token = p.tryAcquireToken(key);
  ASSERT_TRUE(token.isValid());

  bool executed = false;
  auto fn = [&]() { executed = true; };
  auto res = token.executeIfValid(fn);
  ASSERT_EQ(res, executed);
  ASSERT_FALSE(token.isValid());

  executed = false;
  // try to re execute with the same token and this should fail.
  ASSERT_FALSE(token.executeIfValid(fn));
  ASSERT_FALSE(executed);
}

TEST(InFlightPutsTest, TokenMove) {
  InFlightPuts p;
  folly::StringPiece key = "foobar";
  {
    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());
    auto movedToken = std::move(token);

    ASSERT_FALSE(token.isValid());
    ASSERT_TRUE(movedToken.isValid());

    InFlightPuts::PutToken moveAssignToken{};
    ASSERT_FALSE(moveAssignToken.isValid());
    moveAssignToken = std::move(movedToken);
    ASSERT_FALSE(movedToken.isValid());
    ASSERT_TRUE(moveAssignToken.isValid());
  }
  auto token = p.tryAcquireToken(key);
  ASSERT_TRUE(token.isValid());
}

TEST(InFlightPutsTest, FunctionException) {
  InFlightPuts p;
  folly::StringPiece key = "foobar";
  auto token = p.tryAcquireToken(key);
  ASSERT_TRUE(token.isValid());

  bool executed = false;
  auto throwFn = []() { throw std::runtime_error(""); };
  ASSERT_THROW(token.executeIfValid(throwFn), std::runtime_error);
  ASSERT_FALSE(executed);
  ASSERT_TRUE(token.isValid());

  auto fn = [&]() { executed = true; };

  auto res = token.executeIfValid(fn);
  ASSERT_EQ(res, executed);
  ASSERT_TRUE(executed);
  ASSERT_FALSE(token.isValid());
}

TEST(InFlightPutsTest, Collision) {
  InFlightPuts p;
  folly::StringPiece key = "foobar";
  {
    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());

    auto token2 = p.tryAcquireToken(key);
    ASSERT_FALSE(token2.isValid());
  }

  // should be able to create one now
  auto token = p.tryAcquireToken(key);
  ASSERT_TRUE(token.isValid());
}

TEST(InFlightPutsTest, InvalidationSimple) {
  InFlightPuts p;
  folly::StringPiece key = "foobar";

  auto token = p.tryAcquireToken(key);
  ASSERT_TRUE(token.isValid());

  p.invalidateToken(key);
  // token still can be valid.
  ASSERT_TRUE(token.isValid());

  // executing a function should fail since the token was invalidated.
  bool executed = false;
  auto fn = [&]() { executed = true; };
  ASSERT_FALSE(token.executeIfValid(fn));
  ASSERT_FALSE(executed);

  // should not be able to create a token even if an invalid one is around.
  ASSERT_FALSE(p.tryAcquireToken(key).isValid());
}

// invalidate the token for key and try to create a new token before the old
// token is desrtroyed
TEST(InFlightPutsTest, InvalidationAndCreate) {
  InFlightPuts p;
  folly::StringPiece key = "foobar";

  bool executed = false;
  auto fn = [&]() { executed = true; };
  {
    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());

    p.invalidateToken(key);
    // token still can be valid.
    ASSERT_TRUE(token.isValid());

    // try to create the new token and this should not produce a valid token
    // since there is already one outstanding token.
    auto newToken = p.tryAcquireToken(key);
    ASSERT_FALSE(newToken.isValid());

    // executing a function should fail since the token was invalidated.
    ASSERT_FALSE(token.executeIfValid(fn));
    ASSERT_FALSE(executed);
  }

  auto token = p.tryAcquireToken(key);
  ASSERT_TRUE(token.isValid());
  executed = false;
  ASSERT_TRUE(token.executeIfValid(fn));
  ASSERT_TRUE(executed);
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
