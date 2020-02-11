#include "cachelib/navy/block_cache/Region.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Region, ReadAndBlock) {
  Region r{RegionId(0), 1024};

  auto desc = r.openForRead();
  EXPECT_EQ(desc.status(), OpenStatus::Ready);

  EXPECT_FALSE(r.readyForReclaim());
  // Once readyForReclaim has been attempted, all future accesses will be blocked.
  EXPECT_EQ(r.openForRead().status(), OpenStatus::Retry);
  r.close(std::move(desc));
  EXPECT_TRUE(r.readyForReclaim());

  r.reset();
  EXPECT_EQ(r.openForRead().status(), OpenStatus::Ready);
}

TEST(Region, WriteAndBlock) {
  Region r{RegionId(0), 1024};

  auto [desc1, addr1] = r.openAndAllocate(1025);
  EXPECT_EQ(desc1.status(), OpenStatus::Error);

  auto [desc2, addr2] = r.openAndAllocate(100);
  EXPECT_EQ(desc2.status(), OpenStatus::Ready);
  EXPECT_FALSE(r.readyForReclaim());
  r.close(std::move(desc2));
  EXPECT_TRUE(r.readyForReclaim());

  r.reset();
  auto [desc3, addr3] = r.openAndAllocate(1024);
  EXPECT_EQ(desc3.status(), OpenStatus::Ready);
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
