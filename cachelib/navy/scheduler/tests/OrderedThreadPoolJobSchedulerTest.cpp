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
#include <gtest/gtest.h>

#include <set>
#include <thread>

#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/testing/SeqPoints.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {

// order jobs with same type and ensure that they are executed in the
// enqueued order.
TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueSameType) {
  uint64_t key = 5;
  SeqPoints sp;
  std::vector<int> order;
  int seq = 0;
  OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
  scheduler.enqueueWithKey(
      [&sp, &order, n = ++seq]() {
        sp.wait(0);
        order.push_back(n);
        sp.reached(1);
        return JobExitCode::Done;
      },
      "", JobType::Write, key);

  scheduler.enqueueWithKey(
      [&sp, &order, n = ++seq]() {
        sp.wait(0);
        order.push_back(n);
        sp.reached(2);
        return JobExitCode::Done;
      },
      "", JobType::Write, key);

  scheduler.enqueueWithKey(
      [&sp, &order, n = ++seq]() {
        sp.wait(0);
        order.push_back(n);
        sp.reached(3);
        return JobExitCode::Done;
      },
      "", JobType::Write, key);

  EXPECT_EQ(2, scheduler.getTotalSpooled());
  sp.reached(0);
  sp.wait(1);
  sp.wait(2);
  sp.wait(3);

  for (int i = 1; i <= seq; i++) {
    EXPECT_EQ(i, order[i - 1]);
  }
}

// enqueue jobs with different job types for the same key. Ensure that the
// ordering is maintained.
TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueDiffType) {
  std::array<JobType, 2> jobTypes = {JobType::Read, JobType::Write};
  uint64_t key = 5;
  SeqPoints sp;
  std::vector<int> order;
  int seq = 0;
  OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
  scheduler.enqueueWithKey(
      [&sp, &order, n = ++seq]() {
        sp.wait(0);
        order.push_back(n);
        sp.reached(1);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  scheduler.enqueueWithKey(
      [&sp, &order, n = ++seq]() {
        sp.wait(0);
        order.push_back(n);
        sp.reached(2);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  scheduler.enqueueWithKey(
      [&sp, &order, n = ++seq]() {
        sp.wait(0);
        order.push_back(n);
        sp.reached(3);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  EXPECT_EQ(2, scheduler.getTotalSpooled());
  sp.reached(0);
  sp.wait(1);
  sp.wait(2);
  sp.wait(3);

  for (int i = 1; i <= seq; i++) {
    EXPECT_EQ(i, order[i - 1]);
  }
}

// enqueue three jobs, check that two of them are spooled and calling finish
// should handle the draining of all the jobs, even with rescheduling.
TEST(OrderedThreadPoolJobScheduler, SpoolAndFinish) {
  std::array<JobType, 2> jobTypes = {JobType::Read, JobType::Write};
  uint64_t key = 5;
  SeqPoints sp;

  OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
  scheduler.enqueueWithKey(
      [&sp]() {
        sp.wait(0);
        sp.reached(1);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  scheduler.enqueueWithKey(
      [&sp]() {
        sp.wait(0);
        sp.reached(2);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  scheduler.enqueueWithKey(
      [&sp, i = 0]() mutable {
        sp.wait(0);
        if (i < 2) {
          i++;
          return JobExitCode::Reschedule;
        }
        sp.reached(3);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  EXPECT_EQ(2, scheduler.getTotalSpooled());

  sp.reached(0);
  scheduler.finish();
  sp.wait(1);
  sp.wait(2);
  sp.wait(3);
}

// ensure that the ordering is maintained with the rescheduling of the jobs.
// We enqueue three jobs for same key that can reschedule and ensure that
// after reschedule, the order is maintained as well.
TEST(OrderedThreadPoolJobScheduler, JobWithRetry) {
  std::array<JobType, 3> jobTypes = {JobType::Read, JobType::Write,
                                     JobType::Reclaim};
  uint64_t key = 5;
  SeqPoints sp;

  std::atomic<uint64_t> numReschedules{0};

  OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
  scheduler.enqueueWithKey(
      [&, i = 0]() mutable {
        sp.wait(0);
        if (i < 2) {
          i++;
          numReschedules++;
          return JobExitCode::Reschedule;
        }
        sp.reached(1);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  scheduler.enqueueWithKey(
      [&, i = 0]() mutable {
        sp.wait(0);
        if (i < 2) {
          i++;
          numReschedules++;
          return JobExitCode::Reschedule;
        }
        sp.reached(2);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  scheduler.enqueueWithKey(
      [&, i = 0]() mutable {
        sp.wait(0);
        if (i < 2) {
          i++;
          numReschedules++;
          return JobExitCode::Reschedule;
        }
        sp.reached(3);
        return JobExitCode::Done;
      },
      "", jobTypes[folly::Random::rand32() % jobTypes.size()], key);

  EXPECT_EQ(2, scheduler.getTotalSpooled());
  EXPECT_EQ(0, numReschedules);

  sp.reached(0);
  sp.wait(1);
  EXPECT_GE(numReschedules, 2);
  sp.wait(2);
  EXPECT_GE(numReschedules, 4);
  sp.wait(3);
  EXPECT_EQ(6, numReschedules);
}

TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueAndFinish) {
  unsigned int numKeys = 10000;
  std::atomic<int> numCompleted{0};

  {
    OrderedThreadPoolJobScheduler scheduler{3, 32, 10};
    for (unsigned int i = 0; i < numKeys; i++) {
      scheduler.enqueueWithKey(
          [&]() {
            ++numCompleted;
            return JobExitCode::Done;
          },
          "", JobType::Write, folly::Random::rand32());
    }

    scheduler.finish();
  }
  EXPECT_EQ(numCompleted, numKeys);
}

// enqueue a certain number of jobs and validate the stats for spooling are
// reflective of the behavior expected.
TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueMaxLen) {
  unsigned int numKeys = 10000;
  std::atomic<int> numCompleted{0};
  SeqPoints sp;
  sp.setName(0, "all enqueued");

  unsigned int numQueues = 4;
  OrderedThreadPoolJobScheduler scheduler{numQueues, 1, 10};
  for (unsigned int i = 0; i < numKeys; i++) {
    scheduler.enqueueWithKey(
        [&]() {
          sp.wait(0);
          ++numCompleted;
          return JobExitCode::Done;
        },
        "", JobType::Read, folly::Random::rand32());
  }

  uint64_t numSpooled = 0;
  uint64_t maxQueueLen = 0;
  uint64_t pendingJobs = 0;
  scheduler.getCounters({[&](folly::StringPiece name, double stat) {
    if (name == "navy_reader_max_queue_len") {
      maxQueueLen = static_cast<uint64_t>(stat);
    } else if (name == "navy_req_order_curr_spool_size") {
      numSpooled = static_cast<uint64_t>(stat);
    } else if (name == "navy_max_reader_pool_pending_jobs") {
      pendingJobs = static_cast<uint64_t>(stat);
    }
  }});

  EXPECT_GE(numSpooled, 0);
  uint64_t numQueued = numKeys - numSpooled;
  EXPECT_LE(maxQueueLen, numQueued);
  // we could have at most one job executing per Queue. So the total of
  // pending jobs must not be off by more than numQueue when compared with
  // total enqueued.
  EXPECT_LE(numQueued - pendingJobs, numQueues);

  sp.reached(0);
  scheduler.finish();
  EXPECT_EQ(numCompleted, numKeys);
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
