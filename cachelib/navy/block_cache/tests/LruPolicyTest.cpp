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

#include <thread>

#include "cachelib/navy/block_cache/LruPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
const RegionId kNone{};
const RegionId kR0{0};
const RegionId kR1{1};
const RegionId kR2{2};
const RegionId kR3{3};
const Region kRegion0{RegionId{0}, 100};
const Region kRegion1{RegionId{1}, 100};
const Region kRegion2{RegionId{2}, 100};
const Region kRegion3{RegionId{3}, 100};
} // namespace

TEST(EvictionPolicy, LruOrder) {
  LruPolicy policy{0};
  policy.track(kRegion0);
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kRegion0);
  policy.touch(kR0);
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kRegion0);
  policy.track(kRegion1);
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kRegion0);
  policy.track(kRegion1);
  policy.touch(kR0);
  // R1 was not touched so not evicted
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kRegion0);
  policy.track(kRegion1);
  policy.touch(kR0);
  policy.touch(kR1);
  // R1 was not touched so not evicted
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kRegion0);
  policy.track(kRegion1);
  policy.touch(kR1);
  policy.touch(kR0);
  policy.memorySize();
  // R1 was not touched so not evicted
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.memorySize();
  policy.track(kRegion0);
  policy.track(kRegion1);
  policy.track(kRegion2);
  policy.track(kRegion3);
  policy.touch(kR1);
  policy.touch(kR2);
  policy.touch(kR0);
  policy.touch(kR3);
  policy.touch(kR1);
  // R1 was not touched so not evicted
  EXPECT_EQ(kR2, policy.evict());
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kR3, policy.evict());
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  // Touching evicted should cause no harm
  policy.touch(kR0);
  EXPECT_EQ(kNone, policy.evict());
}

TEST(EvictionPolicy, LruReset) {
  LruPolicy policy{0};
  policy.track(kRegion1);
  policy.track(kRegion2);
  policy.track(kRegion3);
  policy.touch(kR1);
  policy.touch(kR2);
  policy.touch(kR3);
  policy.touch(kR1);
  // Will evict region 2 if called here
  policy.reset();
  EXPECT_EQ(kNone, policy.evict());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
