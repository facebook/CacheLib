#include "cachelib/navy/block_cache/FifoPolicy.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(EvictionPolicy, Fifo) {
  FifoPolicy policy;
  policy.track(RegionId{0});
  // Skip region 1
  policy.track(RegionId{2});
  policy.track(RegionId{3});
  EXPECT_EQ(RegionId{0}, policy.evict());
  EXPECT_EQ(RegionId{2}, policy.evict());
  EXPECT_EQ(RegionId{3}, policy.evict());
}

TEST(EvictionPolicy, FifoReset) {
  FifoPolicy policy;
  policy.track(RegionId{1});
  policy.track(RegionId{2});
  policy.track(RegionId{3});
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
