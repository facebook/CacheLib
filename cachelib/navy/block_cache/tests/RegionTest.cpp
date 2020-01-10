#include "cachelib/navy/block_cache/Region.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Region, BlockAndLock) {
  Region r{RegionId(0), 1024};
  // Can't lock if access isn't blocked
  EXPECT_FALSE(r.tryLock());

  EXPECT_TRUE(r.blockAccess());
  EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
  EXPECT_TRUE(r.tryLock());
  EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
  r.unlock();
  r.unblockAccess();

  {
    auto desc1 = r.open(OpenMode::Write);
    EXPECT_EQ(desc1.status(), OpenStatus::Ready);
    {
      auto desc2 = r.open(OpenMode::Write);
      EXPECT_EQ(desc2.status(), OpenStatus::Ready);
      EXPECT_FALSE(r.blockAccess());
      // Access blocked, but active accessorts prevent from locking
      EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
      EXPECT_FALSE(r.tryLock());
      EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
      r.close(std::move(desc2));
    }
    EXPECT_FALSE(r.blockAccess());
    EXPECT_FALSE(r.tryLock());
    r.close(std::move(desc1));
  }
  EXPECT_TRUE(r.blockAccess());
  // Second block is noop
  EXPECT_TRUE(r.blockAccess());
  EXPECT_TRUE(r.tryLock());
  // Doesn't lock second time: exclusive access
  EXPECT_FALSE(r.tryLock());
  r.unlock();
  r.unblockAccess();

  {
    auto desc = r.open(OpenMode::Write);
    EXPECT_EQ(desc.status(), OpenStatus::Ready);
    r.close(std::move(desc));
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
