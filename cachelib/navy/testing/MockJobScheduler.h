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

#pragma once

#include <atomic>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
// This job scheduler doesn't have any associated thread. User manually runs
// queued jobs. Used in tests.
class MockJobScheduler : public JobScheduler {
 public:
  MockJobScheduler() = default;
  ~MockJobScheduler() override;

  // @param job   The job enqueued on the scheduler
  // @param name  Name associated with the job
  // @param type  Job type. This indicates if this is a read, write, or
  //              Navy internal jobs such as reclaim or flush.
  void enqueue(Job job, folly::StringPiece name, JobType type) override;

  // @param job   The job enqueued on the scheduler
  // @param name  Name associated with the job
  // @param type  Job type. This indicates if this is a read, write, or
  //              Navy internal jobs such as reclaim or flush.
  // @param key   Key is ignored in the mock scheduler since we do NOT
  //              shard the jobs internally.
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobType type,
                      uint64_t /* key */) override {
    // Ignore @key because mock scheduler models one thread job scheduler
    enqueue(std::move(job), name, type);
  }

  // This will block until the scheduler has finished all its jobs
  void finish() override;

  // Mock scheduler exports no stats
  void getCounters(const CounterVisitor&) const override {}

  // Runs the first job
  bool runFirst() { return runFirstIf(""); }

  // Runs first job if its name matches @expected, throws std::logic_error
  // otherwise. Returns true if job is done, false if rescheduled.
  // @param expected    Job name. "" means match any job.
  bool runFirstIf(folly::StringPiece expected);

  // Returns how many jobs are currently enqueued in the scheduler
  size_t getQueueSize() const {
    std::lock_guard<std::mutex> lock{m_};
    return q_.size();
  }

  // Returns how many jobs have completed so far
  size_t getDoneCount() const {
    std::lock_guard<std::mutex> lock{m_};
    return doneCount_;
  }

  size_t getRescheduleCount() const {
    std::lock_guard<std::mutex> lock{m_};
    return rescheduleCount_;
  }

 protected:
  struct JobName {
    Job job;
    folly::StringPiece name{};

    JobName(Job j, folly::StringPiece n) : job{std::move(j)}, name{n} {}

    bool nameIs(folly::StringPiece expected) const {
      return expected.size() == 0 || expected == name;
    }
  };

  [[noreturn]] static void throwLogicError(const std::string& what);

  bool runFirstIfLocked(folly::StringPiece expected,
                        std::unique_lock<std::mutex>& lock);

  std::deque<JobName> q_;
  size_t doneCount_{};
  size_t rescheduleCount_{};
  std::atomic<bool> processing_{false};
  mutable std::mutex m_;
};

// This job scheduler runs everything on one thread. Used in tests.
class MockSingleThreadJobScheduler : public MockJobScheduler {
 public:
  MockSingleThreadJobScheduler()
      : t_{[this] {
          while (!stopped_) {
            process();
          }
        }} {}
  ~MockSingleThreadJobScheduler() override {
    stopped_ = true;
    t_.join();
  }

 private:
  void process();

  std::atomic<bool> stopped_{false};
  std::thread t_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
