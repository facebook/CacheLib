#include "cachelib/navy/block_cache/LruPolicy.h"

#include "cachelib/navy/common/Utils.h"

#include <gtest/gtest.h>
#include <thread>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
static const RegionId kNone{};
static const RegionId kR0{0};
static const RegionId kR1{1};
static const RegionId kR2{2};
static const RegionId kR3{3};

TEST(EvictionPolicy, LruOrder) {
  LruPolicy policy{0};
  policy.track(kR0);
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kR0);
  policy.touch(kR0);
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kR0);
  policy.track(kR1);
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kR0);
  policy.track(kR1);
  policy.touch(kR0);
  // R1 was not touched so not evicted
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kR0);
  policy.track(kR1);
  policy.touch(kR0);
  policy.touch(kR1);
  // R1 was not touched so not evicted
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.track(kR0);
  policy.track(kR1);
  policy.touch(kR1);
  policy.touch(kR0);
  policy.memorySize();
  // R1 was not touched so not evicted
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kR0, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
  policy.memorySize();
  policy.track(kR0);
  policy.track(kR1);
  policy.track(kR2);
  policy.track(kR3);
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

// No tracking, no eviction
TEST(EvictionPolicy, LruProtect) {
  LruPolicy policy{0};
  policy.track(kR1);
  policy.track(kR2);
  policy.track(kR3);
  policy.touch(kR1);
  policy.touch(kR2);
  policy.touch(kR3);
  EXPECT_EQ(kR1, policy.evict());
  EXPECT_EQ(kR2, policy.evict());
  EXPECT_EQ(kR3, policy.evict());
  EXPECT_EQ(kNone, policy.evict());
}

TEST(EvictionPolicy, LruReset) {
  LruPolicy policy{0};
  policy.track(kR1);
  policy.track(kR2);
  policy.track(kR3);
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
