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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/testing/MockJobScheduler.h"
#include "cachelib/navy/testing/SeqPoints.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
class MockJob {
 public:
  MOCK_METHOD0(run, JobExitCode());
};
} // namespace

TEST(MockJobScheduler, Basic) {
  MockJobScheduler ex;

  MockJob job;
  EXPECT_CALL(job, run())
      .WillOnce(testing::Return(JobExitCode::Reschedule))
      .WillOnce(testing::Return(JobExitCode::Reschedule))
      .WillOnce(testing::Return(JobExitCode::Done));
  ex.enqueue([&job] { return job.run(); }, "test", JobType::Read);

  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_EQ(0, ex.getDoneCount());
  EXPECT_FALSE(ex.runFirst());
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_EQ(0, ex.getDoneCount());
  EXPECT_FALSE(ex.runFirst());
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_EQ(0, ex.getDoneCount());
  EXPECT_TRUE(ex.runFirst());
  EXPECT_EQ(0, ex.getQueueSize());
  EXPECT_EQ(1, ex.getDoneCount());
}

TEST(MockJobScheduler, BasicTwoJobs) {
  MockJobScheduler ex;

  MockJob job1;
  EXPECT_CALL(job1, run())
      .WillOnce(testing::Return(JobExitCode::Reschedule))
      .WillOnce(testing::Return(JobExitCode::Done));
  ex.enqueue([&job1] { return job1.run(); }, "job1", JobType::Read);
  MockJob job2;
  EXPECT_CALL(job2, run())
      .WillOnce(testing::Return(JobExitCode::Reschedule))
      .WillOnce(testing::Return(JobExitCode::Reschedule))
      .WillOnce(testing::Return(JobExitCode::Done));
  ex.enqueue([&job2] { return job2.run(); }, "job2", JobType::Read);

  EXPECT_FALSE(ex.runFirstIf("job1"));
  EXPECT_FALSE(ex.runFirstIf("job2"));
  EXPECT_TRUE(ex.runFirstIf("job1"));
  EXPECT_FALSE(ex.runFirstIf("job2"));
  EXPECT_TRUE(ex.runFirstIf("job2"));
}

TEST(MockJobScheduler, RunIf) {
  MockJobScheduler ex;

  MockJob job;
  EXPECT_CALL(job, run()).WillOnce(testing::Return(JobExitCode::Done));
  ex.enqueue([&job] { return job.run(); }, "test", JobType::Read);

  EXPECT_THROW(ex.runFirstIf("invalid"), std::logic_error);
  EXPECT_TRUE(ex.runFirstIf("test"));
}

TEST(MockJobScheduler, JobSchedulesFirst) {
  MockJobScheduler ex;

  ex.enqueue(
      [&ex] {
        ex.enqueue([] { return JobExitCode::Done; }, "child", JobType::Reclaim);
        return JobExitCode::Done;
      },
      "first",
      JobType::Read);

  EXPECT_TRUE(ex.runFirstIf("first"));
  EXPECT_TRUE(ex.runFirstIf("child"));
  EXPECT_EQ(0, ex.getQueueSize());
  EXPECT_EQ(2, ex.getDoneCount());
}

TEST(MockSingleThreadJobScheduler, Run) {
  MockSingleThreadJobScheduler ex;

  SeqPoints sp;
  std::atomic<bool> retry{true};
  ex.enqueue(
      [&retry, &sp] {
        if (retry) {
          return JobExitCode::Reschedule;
        }
        sp.reached(0);
        return JobExitCode::Done;
      },
      "job1",
      JobType::Read);
  ex.enqueue(
      [&retry, &sp] {
        if (retry) {
          return JobExitCode::Reschedule;
        }
        sp.reached(1);
        return JobExitCode::Done;
      },
      "job2",
      JobType::Read);
  ex.enqueue(
      [&retry, &sp] {
        if (retry) {
          return JobExitCode::Reschedule;
        }
        sp.reached(2);
        return JobExitCode::Done;
      },
      "job3",
      JobType::Read);
  EXPECT_EQ(0, ex.getDoneCount());
  retry = false;
  sp.wait(0);
  sp.wait(1);
  sp.wait(2);
  EXPECT_EQ(3, ex.getDoneCount());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
