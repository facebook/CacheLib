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

#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Hash, HashedKeyCollision) {
  HashedKey hk1{"key 1"};
  HashedKey hk2{"key 2"};

  // Simulate a case where hash matches but key doesn't.
  HashedKey hk3 = HashedKey::precomputed(hk2.key(), hk1.keyHash());

  EXPECT_NE(hk1, hk2);
  EXPECT_NE(hk1, hk3);
  EXPECT_NE(hk2, hk3);
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
