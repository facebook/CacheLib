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
#include <mutex>
#include <thread>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/scheduler/ThreadPoolJobQueue.h"

namespace facebook {
namespace cachelib {
namespace navy {

// A pool of job queues and their corresponding threads.
class ThreadPoolExecutor {
 public:
  struct Stats {
    uint64_t jobsDone{};
    uint64_t jobsHighReschedule{};
    uint64_t reschedules{};
    uint64_t maxQueueLen{};
    uint64_t maxPendingJobs{};
  };

  // @param numThreads  number of threads for the executor
  // @param name        name for debugging
  ThreadPoolExecutor(uint32_t numThreads, folly::StringPiece name);

  // put a job into the next queue in pool
  // @param job   the job to be executed
  // @param name  name of the job, for logging/debugging purposes
  // @param pos   front/back of the queue to push in
  void enqueue(Job job, folly::StringPiece name, JobQueue::QueuePos pos);

  // put a job into the a specific queue in pool based on the key hash
  // @param job   the job to be executed
  // @param name  name of the job, for logging/debugging purposes
  // @param pos   front/back of the queue to push in
  // @param key   the key hash
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobQueue::QueuePos pos,
                      uint64_t key);

  // Waits till all queued and currently running jobs are finished
  // @return  the total number of jobs processed in the pool.
  uint64_t finish();

  // stops the queues and wait the current processing job to complete
  void join();

  // returns the stats of the pool
  Stats getStats() const;

  folly::StringPiece getName() const { return name_; }

 private:
  const folly::StringPiece name_{};
  std::atomic<uint32_t> nextQueue_{0};
  std::vector<std::unique_ptr<JobQueue>> queues_;
  std::vector<std::thread> workers_;
};

// Pool of worker threads, each with their own job queue
class ThreadPoolJobScheduler final : public JobScheduler {
 public:
  // @param readerThreads   number of threads for the read scheduler
  // @param writerThreads   number of threads for the write scheduler
  explicit ThreadPoolJobScheduler(uint32_t readerThreads,
                                  uint32_t writerThreads);
  ThreadPoolJobScheduler(const ThreadPoolJobScheduler&) = delete;
  ThreadPoolJobScheduler& operator=(const ThreadPoolJobScheduler&) = delete;
  ~ThreadPoolJobScheduler() override { join(); }

  // Enqueue a job for processing. @name for logging/debugging purposes.
  // Only support Reclaim job type.
  void enqueue(Job job, folly::StringPiece name, JobType type) override;

  // Enqueue a job for processing. @name for logging/debugging purposes.
  // Supports Read and Write job types.
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobType type,
                      uint64_t key) override;

  // Waits till all queued and currently running jobs are finished
  void finish() override;

  // Exports scheduler stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const override;

 private:
  void join();

  ThreadPoolExecutor reader_;
  ThreadPoolExecutor writer_;
};

// Implements an ordered job scheduler. Ordering is guaranteed for the
// executiong of jobs enqueued by a key.
class OrderedThreadPoolJobScheduler final : public JobScheduler {
 public:
  // @param readerThreads   number of threads for the read scheduler
  // @param writerThreads   number of threads for the write scheduler
  // @param numShardsPower  power of two specification for sharding internally
  //                        to avoid contention and queueing
  explicit OrderedThreadPoolJobScheduler(size_t readerThreads,
                                         size_t writerThreads,
                                         size_t numShardsPower);
  OrderedThreadPoolJobScheduler(const OrderedThreadPoolJobScheduler&) = delete;
  OrderedThreadPoolJobScheduler& operator=(
      const OrderedThreadPoolJobScheduler&) = delete;
  ~OrderedThreadPoolJobScheduler() override {}

  // put a job into the queue
  // @param job   the job to be executed
  // @param name  name of the job, for logging/debugging purposes
  // @param type  the type of job: Read/Write/Reclaim/Flush
  void enqueue(Job job, folly::StringPiece name, JobType type) override;

  // put a job into the queue based on the key hash
  // execution ordering of the key is guaranteed
  // @param job   the job to be executed
  // @param name  name of the job, for logging/debugging purposes
  // @param type  the type of job: Read/Write/Reclaim/Flush
  // @param key   the key hash
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobType type,
                      uint64_t key) override;

  // waits till all queued and currently running jobs are finished
  void finish() override;

  // returns the total number of jobs that were spooled (pending) due to
  // ordering.
  uint64_t getTotalSpooled() const { return numSpooled_.get(); }

  // Exports the ordered scheduler stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const override;

 private:
  // represents the parameters describing a job to be scheduled.
  struct JobParams {
    Job job;
    JobType type;
    folly::StringPiece name;
    uint64_t key;

    JobParams(Job j, JobType t, folly::StringPiece n, uint64_t k)
        : job{std::move(j)}, type{t}, name{n}, key{k} {}

    JobParams(const JobParams&) = delete;
    JobParams& operator=(const JobParams&) = delete;
    JobParams(JobParams&& o) noexcept
        : job{std::move(o.job)}, type{o.type}, name{o.name}, key{o.key} {}
    JobParams& operator=(JobParams&& o) {
      if (this != &o) {
        this->~JobParams();
        new (this) JobParams(std::move(o));
      }
      return *this;
    }
  };

  void scheduleNextJob(uint64_t shard);
  void scheduleJobLocked(JobParams params, uint64_t shard);

  // mutex per shard. mutex protects the pending jobs and spooling state
  mutable std::vector<std::mutex> mutexes_;

  // list of jobs per shard if there is already a job pending for the shard
  std::vector<std::list<JobParams>> pendingJobs_;

  // indicates if there is a pending job for the shard. This is kept as an
  // uint64_t instead of bool to ensure atomicity with sharded locks.
  std::vector<uint64_t> shouldSpool_;

  // total number of jobs that were spooled due to ordering.
  AtomicCounter numSpooled_{0};

  // current number of jobs in the spool
  AtomicCounter currSpooled_{0};

  // sharding power
  const size_t numShardsPower_;

  // the underlying async scheduler
  ThreadPoolJobScheduler scheduler_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
