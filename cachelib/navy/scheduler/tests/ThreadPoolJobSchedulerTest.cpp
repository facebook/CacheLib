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

#include <gtest/gtest.h>

#include <set>
#include <thread>

#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/testing/SeqPoints.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
void spinWait(std::atomic<int>& ai, int target) {
  while (ai.load(std::memory_order_acquire) != target) {
    std::this_thread::yield();
  }
}

TEST(ThreadPoolJobScheduler, BlockOneTaskTwoWorkers) {
  std::atomic<int> ai{0};
  ThreadPoolJobScheduler scheduler{1, 2};
  scheduler.enqueue(
      [&ai]() {
        spinWait(ai, 2);
        ai.store(3, std::memory_order_release);
        return JobExitCode::Done;
      },
      "wait",
      JobType::Write);
  scheduler.enqueue(
      [&ai]() {
        ai.store(1, std::memory_order_release);
        return JobExitCode::Done;
      },
      "post",
      JobType::Write);
  spinWait(ai, 1);
  ai.store(2, std::memory_order_release);
  spinWait(ai, 3);
}

TEST(ThreadPoolJobScheduler, StopEmpty) {
  ThreadPoolJobScheduler scheduler{1, 1};
}

TEST(ThreadPoolJobScheduler, EnqueueFirst) {
  ThreadPoolJobScheduler scheduler{1, 1};
  // There are no races with one thread, but just to be sure
  for (int i = 0; i < 50; i++) {
    // Assumes one thread
    std::vector<int> v;
    scheduler.enqueue(
        [&scheduler, &v]() {
          v.push_back(0);
          for (int j = 1; j < 3; j++) {
            scheduler.enqueue(
                [&v, j]() {
                  v.push_back(j);
                  return JobExitCode::Done;
                },
                "write1",
                JobType::Write);
          }
          scheduler.enqueue(
              [&v]() {
                v.push_back(3);
                return JobExitCode::Done;
              },
              "reclaim",
              JobType::Reclaim);
          return JobExitCode::Done;
        },
        "write2",
        JobType::Write);
    scheduler.finish();
    std::vector<int> expected{0, 3, 1, 2};
    EXPECT_EQ(expected, v);
  }
}

TEST(ThreadPoolJobScheduler, EnqueueWithKey) {
  ThreadPoolJobScheduler scheduler{1, 2};
  // Ensure the result is consistent
  for (int i = 0; i < 50; i++) {
    std::set<std::thread::id> threadIds;
    for (int j = 0; j < 3; j++) {
      scheduler.enqueueWithKey(
          [&threadIds]() {
            threadIds.insert(std::this_thread::get_id());
            return JobExitCode::Done;
          },
          "test",
          JobType::Write,
          0);
    }
    scheduler.finish();
    EXPECT_EQ(1, threadIds.size());
  }
}

TEST(ThreadPoolJobScheduler, Finish) {
  SeqPoints sp;
  ThreadPoolJobScheduler scheduler{2, 1};
  bool done = false;
  bool spReached = false;
  scheduler.enqueue(
      [&sp, &done, &spReached]() {
        if (spReached) {
          done = true;
          return JobExitCode::Done;
        }
        sp.reached(0);
        return JobExitCode::Reschedule;
      },
      "test",
      JobType::Read);

  // Wait we got first normal reschedule
  sp.wait(0);
  spReached = true;

  scheduler.finish();
  EXPECT_TRUE(done);
}

TEST(ThreadPoolJobScheduler, FinishSchedulesNew) {
  SeqPoints sp;

  // This test cover the bug in ThreadPoolJobScheduler we had. Let we have 2
  // thread scheduler, with threads #1 and #2. We finish threads one-by-one,
  // starting with thread #1. If a job on thread #2 schedules a new job on
  // thread #1 it will be never executed!
  ThreadPoolJobScheduler scheduler{2, 1};

  scheduler.enqueueWithKey(
      [&sp, &scheduler] {
        sp.wait(0);
        scheduler.enqueueWithKey(
            [&sp] {
              EXPECT_FALSE(sp.waitFor(1, std::chrono::seconds{1}));
              return JobExitCode::Done;
            },
            "job2",
            JobType::Read,
            0 /* thread #1 */
        );
        return JobExitCode::Done;
      },
      "job1",
      JobType::Read,
      1 /* thread #2 */
  );

  std::thread t{[&sp] {
    std::this_thread::sleep_for(std::chrono::seconds{1});
    sp.reached(0);
  }};
  scheduler.finish();
  // With bug, control gets here before "job2" done.
  sp.reached(1);
  t.join();
}

TEST(ThreadPoolJobScheduler, ReadWriteReclaim) {
  std::vector<int> v;
  ThreadPoolJobScheduler scheduler{1, 1};

  SeqPoints sp;
  sp.setName(0, "Write issued");
  sp.setName(1, "Reclaim issued");
  sp.setName(2, "Read issued");
  scheduler.enqueue(
      [&sp, &scheduler, &v] {
        sp.wait(0);
        v.push_back(0);
        scheduler.enqueue(
            [&sp, &v] {
              sp.wait(2);
              v.push_back(1);
              return JobExitCode::Done;
            },
            "write2",
            JobType::Write);
        sp.reached(2);
        return JobExitCode::Done;
      },
      "read1",
      JobType::Read);
  scheduler.enqueue(
      [&sp, &v] {
        if (!sp.waitFor(1, std::chrono::milliseconds{10})) {
          return JobExitCode::Reschedule;
        }
        v.push_back(2);
        sp.reached(0);
        return JobExitCode::Done;
      },
      "write1",
      JobType::Write);
  scheduler.enqueue(
      [&sp, &v] {
        v.push_back(3);
        sp.reached(1);
        return JobExitCode::Done;
      },
      "reclaim",
      JobType::Write);
  scheduler.finish();
  EXPECT_EQ((std::vector<int>{3, 2, 0, 1}), v);
}

// Enqueue a certain number of jobs that cause the queue to be piled up and
// check that the stats are reflective.
TEST(ThreadPoolJobScheduler, MaxQueueLen) {
  unsigned int numQueues = 4;
  ThreadPoolJobScheduler scheduler{numQueues, 1};
  SeqPoints sp;
  sp.setName(0, "all enqueued");

  std::atomic<unsigned int> jobsDone{0};
  auto job = [&] {
    sp.wait(0);
    ++jobsDone;
    return JobExitCode::Done;
  };

  int numToQueue = 1000;
  for (int i = 0; i < numToQueue; i++) {
    scheduler.enqueue(job, "read", JobType::Read);
  }

  // we have enqueued 1000 jobs across 4 queues. One job could be executed per
  // thread. So the max queue len would be 249 or 250 per queue.
  bool checked = false;
  scheduler.getCounters({[&](folly::StringPiece name, double stat) {
    if (name == "navy_reader_pool_max_queue_len") {
      EXPECT_LE(numToQueue / numQueues - stat, 1);
      checked = true;
    }
  }});
  EXPECT_TRUE(checked);
  sp.reached(0);
  scheduler.finish();
  EXPECT_EQ(jobsDone, numToQueue);
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
