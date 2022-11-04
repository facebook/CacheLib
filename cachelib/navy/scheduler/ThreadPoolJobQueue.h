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

#include <folly/Range.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>

#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

// A double ended job queue, job can be pushed in to the beginning or end of
// queue. The process() function will keep executing the head job (pop it before
// process) until stop signal received.
class JobQueue {
 public:
  struct Stats {
    uint64_t jobsDone{};
    uint64_t jobsHighReschedule{};
    uint64_t reschedules{};
    uint64_t maxQueueLen{};
  };

  enum class QueuePos {
    Front, // High priority jobs
    Back
  };

  JobQueue() = default;
  JobQueue(const JobQueue&) = delete;
  JobQueue& operator=(const JobQueue&) = delete;
  ~JobQueue() = default;

  // put a job into the next queue in pool
  // @param job   the job to be executed
  // @param name  name of the job, for logging/debugging purposes
  // @param pos   front/back of the queue to push in
  void enqueue(Job job, folly::StringPiece name, QueuePos pos);

  // Returns total count of enqueued jobs to this queue. After @finish, all of
  // them are done, but new may be enqueued after.
  uint64_t finish();

  // Processes queue until stop signal is received
  void process();

  // Raises stop signal. You can join worker threads after. No more new jobs
  // are admitted to the queue.
  void requestStop();

  Stats getStats() const;

 private:
  struct QueueEntry {
    Job job;
    uint32_t rescheduleCount{};
    folly::StringPiece name;

    QueueEntry(Job j, folly::StringPiece n) : job{std::move(j)}, name{n} {}
    QueueEntry(QueueEntry&&) = default;
    QueueEntry& operator=(QueueEntry&&) = default;
  };

  JobExitCode runJob(QueueEntry& entry);

  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  // Can have several queues and round robin between them to reduce contention
  std::deque<QueueEntry> queue_;
  uint64_t enqueueCount_{};
  mutable uint64_t maxQueueLen_{};
  mutable uint64_t jobsDone_{};
  mutable uint64_t jobsHighReschedule_{};
  mutable uint64_t reschedules_{};
  int processing_{};
  bool stop_{false};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
