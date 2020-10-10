#include "cachelib/navy/block_cache/FifoPolicy.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
