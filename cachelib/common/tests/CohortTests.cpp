#include "cachelib/common/Cohort.h"

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace tests {

// ensure that incrementing and decrementing works as expected
TEST(Cohort, IncrDecr) {
  Cohort cohort;
  auto isTop = cohort.incrActiveReqs();
  EXPECT_EQ(isTop, cohort.isTopCohort());
  EXPECT_EQ(cohort.getPending(isTop), 1ULL);
  cohort.decrActiveReqs(isTop);
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);
}

// try to switch cohorts and ensure that switching waits for current pending
// refs to be drained
TEST(Cohort, SwithWithCohort) {
  Cohort cohort;

  auto isTop = cohort.incrActiveReqs();
  cohort.incrActiveReqs();
  cohort.incrActiveReqs();
  EXPECT_EQ(cohort.getPending(isTop), 3ULL);

  folly::Baton b;
  auto switchingThread = std::thread([&]() {
    cohort.switchCohorts();
    b.post();
  });

  std::this_thread::sleep_for(std::chrono::seconds(5));

  cohort.decrActiveReqs(isTop);
  EXPECT_EQ(cohort.getPending(isTop), 2ULL);
  cohort.decrActiveReqs(isTop);
  EXPECT_EQ(cohort.getPending(isTop), 1ULL);
  cohort.decrActiveReqs(isTop);
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);

  b.wait();

  // cohort must have changed now.
  EXPECT_NE(isTop, cohort.isTopCohort());

  switchingThread.join();
}

// ensure that incrementing cohort is consistent with switching.
TEST(Cohort, MaintainTopOrBottom) {
  Cohort cohort;
  auto isTop = cohort.isTopCohort();
  EXPECT_EQ(isTop, cohort.incrActiveReqs());
  EXPECT_EQ(isTop, cohort.incrActiveReqs());
  EXPECT_EQ(isTop, cohort.incrActiveReqs());
  EXPECT_EQ(isTop, cohort.isTopCohort());

  cohort.decrActiveReqs(isTop);
  cohort.decrActiveReqs(isTop);
  cohort.decrActiveReqs(isTop);

  cohort.switchCohorts();

  EXPECT_NE(isTop, cohort.isTopCohort());
  EXPECT_NE(isTop, cohort.incrActiveReqs());
  EXPECT_NE(isTop, cohort.incrActiveReqs());
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
