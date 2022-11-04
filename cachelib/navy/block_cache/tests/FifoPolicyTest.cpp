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

#include "cachelib/navy/block_cache/FifoPolicy.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
const Region kRegion0{RegionId{0}, 100};
const Region kRegion1{RegionId{1}, 100};
const Region kRegion2{RegionId{2}, 100};
const Region kRegion3{RegionId{3}, 100};
} // namespace

TEST(EvictionPolicy, Fifo) {
  FifoPolicy policy;
  policy.track(kRegion0);
  // Skip region 1
  policy.track(kRegion2);
  policy.track(kRegion3);
  EXPECT_EQ(kRegion0.id(), policy.evict());
  EXPECT_EQ(kRegion2.id(), policy.evict());
  EXPECT_EQ(kRegion3.id(), policy.evict());
}

TEST(EvictionPolicy, FifoReset) {
  FifoPolicy policy;
  policy.track(kRegion1);
  policy.track(kRegion2);
  policy.track(kRegion3);
  policy.touch(RegionId{1});
  policy.touch(RegionId{2});
  policy.touch(RegionId{3});
  policy.touch(RegionId{1});
  // Will evict region 1 if called here
  policy.reset();
  EXPECT_EQ(RegionId{}, policy.evict());
}

TEST(EvictionPolicy, FifoRecover) {
  folly::IOBufQueue ioq;
  {
    FifoPolicy policy;
    policy.track(kRegion1);
    policy.track(kRegion2);
    policy.track(kRegion3);

    {
      auto rw = createMemoryRecordWriter(ioq);
      policy.persist(*rw);
    }
  }

  {
    FifoPolicy policy;
    auto rr = createMemoryRecordReader(ioq);
    policy.recover(*rr);

    EXPECT_EQ(kRegion1.id(), policy.evict());
    EXPECT_EQ(kRegion2.id(), policy.evict());
    EXPECT_EQ(kRegion3.id(), policy.evict());
    EXPECT_EQ(RegionId{}, policy.evict());
  }
}

TEST(EvictionPolicy, SegmentedFifoSimple) {
  Region region0{RegionId{0}, 100};
  Region region1{RegionId{1}, 100};
  Region region2{RegionId{2}, 100};
  Region region3{RegionId{3}, 100};
  region0.setPriority(1);
  region1.setPriority(1);
  region2.setPriority(1);
  region3.setPriority(1);

  SegmentedFifoPolicy policy{{1, 1}};

  policy.track(region0); // {[0], []}
  policy.track(region1); // {[0], [1]}
  policy.track(region2); // {[0, 1], [2]}
  policy.track(region3); // {[0, 1], [2, 3]}

  EXPECT_EQ(region0.id(), policy.evict());
  EXPECT_EQ(region1.id(), policy.evict());
  EXPECT_EQ(region2.id(), policy.evict());
  EXPECT_EQ(region3.id(), policy.evict());
  EXPECT_EQ(RegionId{}, policy.evict());
}

TEST(EvictionPolicy, SegmentedFifoBadConfig) {
  ASSERT_THROW((SegmentedFifoPolicy{{1, 0, 1}}), std::invalid_argument);
  ASSERT_THROW((SegmentedFifoPolicy{{1, 0}}), std::invalid_argument);
  ASSERT_THROW((SegmentedFifoPolicy{{0}}), std::invalid_argument);
  ASSERT_THROW((SegmentedFifoPolicy{{}}), std::invalid_argument);
}

TEST(EvictionPolicy, SegmentedFifoRebalance) {
  Region region0{RegionId{0}, 100};
  Region region1{RegionId{1}, 100};
  Region region2{RegionId{2}, 100};
  Region region3{RegionId{3}, 100};
  region0.setPriority(2);
  region1.setPriority(1);
  region2.setPriority(0);
  region3.setPriority(2);

  SegmentedFifoPolicy policy{{1, 1, 1}};

  // Region 0 has highest pri, but it will be rebalanced into the lowest
  // priority segment
  policy.track(region0); // {[0], [], []}
  EXPECT_EQ(region0.id(), policy.evict());

  policy.track(region0); // {[0], [], []}
  policy.track(region1); // {[0, 1], [], []}
  policy.track(region2); // {[0, 1, 2], [], []}
  policy.track(region3); // {[0, 1, 2], [], [3]}

  EXPECT_EQ(region0.id(), policy.evict());
  EXPECT_EQ(region1.id(), policy.evict());
  EXPECT_EQ(region2.id(), policy.evict());

  region1.setPriority((1));
  region2.setPriority((1));
  policy.track(region1); // {[3, 1], [], []}
  policy.track(region2); // {[3, 1, 2], [], []}

  EXPECT_EQ(region3.id(), policy.evict());
  EXPECT_EQ(region1.id(), policy.evict());
  EXPECT_EQ(region2.id(), policy.evict());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
