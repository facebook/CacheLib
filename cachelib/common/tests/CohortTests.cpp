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

#include <folly/Random.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "cachelib/common/Cohort.h"

namespace facebook {
namespace cachelib {
namespace tests {

// ensure that incrementing and decrementing works as expected
TEST(Cohort, IncrDecr) {
  Cohort cohort;
  auto tok = cohort.incrActiveReqs();
  bool isTop = tok.isTop();
  EXPECT_EQ(isTop, cohort.isTopCohort());
  EXPECT_EQ(cohort.getPending(isTop), 1ULL);
  tok.decrement();
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);

  {
    // test auto-decrement when exiting scope
    auto tok0 = cohort.incrActiveReqs();
    EXPECT_EQ(tok0.isTop(), isTop);
    EXPECT_EQ(cohort.getPending(isTop), 1ULL);
  }
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);

  {
    // test move constructor against double-decrement
    auto tok1 = cohort.incrActiveReqs();
    auto tok2 = std::move(tok1);
  }
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);

  {
    // test move assignment against leak or double-decrement
    auto tok3 = cohort.incrActiveReqs();
    auto tok4 = cohort.incrActiveReqs();
    auto tok5 = cohort.incrActiveReqs();
    tok3 = std::move(tok4);
    tok3.decrement();
    tok3 = std::move(tok5);
  }
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);

  {
    // test self-move
    auto tok6 = cohort.incrActiveReqs();
    auto ptr6 = &tok6; // compiler prevents direct self-move
    tok6 = std::move(*ptr6);
    tok6.decrement();
  }
  EXPECT_EQ(cohort.getPending(isTop), 0ULL);
}

// try to switch cohorts and ensure that switching waits for current pending
// refs to be drained
TEST(Cohort, SwitchWithCohort) {
  Cohort cohort;

  bool isTop = cohort.isTopCohort();
  auto tok0 = cohort.incrActiveReqs();
  auto tok1 = cohort.incrActiveReqs();
  auto tok2 = cohort.incrActiveReqs();
  EXPECT_EQ(cohort.getPending(isTop), 3ULL);

  folly::Baton b;
  auto switchingThread = std::thread([&]() {
    cohort.switchCohorts();
    b.post();
  });

  std::this_thread::sleep_for(std::chrono::seconds(5));

  tok2.decrement();
  EXPECT_EQ(cohort.getPending(isTop), 2ULL);
  tok1.decrement();
  EXPECT_EQ(cohort.getPending(isTop), 1ULL);
  tok0.decrement();
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
  auto tok0 = cohort.incrActiveReqs();
  EXPECT_EQ(isTop, tok0.isTop());
  auto tok1 = cohort.incrActiveReqs();
  EXPECT_EQ(isTop, tok1.isTop());
  auto tok2 = cohort.incrActiveReqs();
  EXPECT_EQ(isTop, tok2.isTop());
  EXPECT_EQ(isTop, cohort.isTopCohort());

  tok0.decrement();
  tok1.decrement();
  tok2.decrement();

  cohort.switchCohorts();

  EXPECT_NE(isTop, cohort.isTopCohort());
  auto tok3 = cohort.incrActiveReqs();
  EXPECT_NE(isTop, tok3.isTop());
  auto tok4 = cohort.incrActiveReqs();
  EXPECT_NE(isTop, tok4.isTop());
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
      auto tok = cohort.incrActiveReqs();
      std::this_thread::sleep_for(std::chrono::microseconds(10));
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
