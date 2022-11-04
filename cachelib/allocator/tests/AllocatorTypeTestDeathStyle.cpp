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

#include "cachelib/allocator/tests/BaseAllocatorTestDeathStyle.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {
TYPED_TEST_CASE(BaseAllocatorTestDeathStyle, AllocatorTypes);

TYPED_TEST(BaseAllocatorTestDeathStyle, ReadOnlyCacheView) {
  this->testReadOnlyCacheView();
}
} // namespace tests
} // namespace cachelib
} // namespace facebook

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // Use thread-safe mode for all tests that use ASSERT_DEATH
  ::testing::GTEST_FLAG(death_test_style) = "threadsafe";
  return RUN_ALL_TESTS();
}
