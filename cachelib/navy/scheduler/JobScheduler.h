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

#include <folly/Function.h>

#include <memory>

#include "cachelib/navy/common/CompilerUtils.h"
#include "cachelib/navy/common/Types.h"

// Defines Job and JobScheduler (asynchronous executor).
//
// Job has a movable function returning JobExitCode:
//   - Done           Job finished
//   - Reschedule     Re-queue and retry again
//
// JobScheduler has the following members:
//   - enqueue(Job)               Enqueues a job
//   - enqueueWithKey(Job, key)   Enqueues a job with a key. Can be used to hash
//                                jobs.
//   - finish()                   Waits for all the scheduled jobs to finish

namespace facebook {
namespace cachelib {
namespace navy {
enum class JobExitCode {
  Done,
  Reschedule,
};

// Allow job to have movable captures
using Job = folly::Function<JobExitCode()>;

enum class JobType { Read, Write, Reclaim, Flush };

class JobScheduler {
 public:
  virtual ~JobScheduler() = default;

  // Uses @key to schedule job on one of available workers. Jobs can be
  // ordered by their key based on their enqueue order,  if the scheduler
  // supports it.
  virtual void enqueueWithKey(Job job,
                              folly::StringPiece name,
                              JobType type,
                              uint64_t key) = 0;

  // enqueue a job for execution. No ordering guarantees are made for these
  // jobs.
  virtual void enqueue(Job job, folly::StringPiece name, JobType type) = 0;

  // guarantees that all enqueued jobs are finished and blocks until then.
  virtual void finish() = 0;

  // visits each available counter for the visitor to take appropriate action.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;
};

// create a thread pool job scheduler that ensures ordering of requests by
// key. This is the default job scheduler for use in Navy.
std::unique_ptr<JobScheduler> createOrderedThreadPoolJobScheduler(
    uint32_t readerThreads,
    uint32_t writerThreads,
    uint32_t reqOrderShardPower);

} // namespace navy
} // namespace cachelib
} // namespace facebook
