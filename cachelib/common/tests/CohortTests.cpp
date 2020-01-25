#include "cachelib/common/Cohort.h"

#include <folly/Random.h>
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

// ensure that incrementing cohort is consistent with switching in a
// multi-threaded env.
TEST(Cohort, MultiThreaded) {
  Cohort cohort;
  int nBumps = folly::Random::rand32(1000, 2000);
  int nSwitches = nBumps;

  // we start with bottom cohort
  EXPECT_FALSE(cohort.isTopCohort());

  auto incrDecrInThread = [&]() {
    for (int i = 0; i < nBumps; i++) {
      auto isTop = cohort.incrActiveReqs();
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      cohort.decrActiveReqs(isTop);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 32; i++) {
    threads.emplace_back(incrDecrInThread);
  }

  threads.emplace_back([&]() {
    for (int i = 0; i < nSwitches; i++) {
      cohort.switchCohorts();
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  });

  for (auto& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  EXPECT_EQ(cohort.getPending(cohort.isTopCohort()), 0ULL);
  EXPECT_EQ(cohort.getPending(!cohort.isTopCohort()), 0ULL);

  // ensure the cohort state matches the number of times we could have
  // switched.
  if (nSwitches % 2 == 0) {
    EXPECT_FALSE(cohort.isTopCohort());
  } else {
    EXPECT_TRUE(cohort.isTopCohort());
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
