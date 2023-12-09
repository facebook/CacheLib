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

#include <folly/fibers/TimedMutex.h>
#include <folly/io/async/EventBase.h>

#include <list>
#include <vector>

#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/scheduler/NavyRequestDispatcher.h"

namespace facebook {
namespace cachelib {
namespace navy {

using folly::EventBase;
using folly::EventBaseManager;
using folly::fibers::TimedMutex;

// NavyRequestScheduler is a NavyRequest dispatcher with resolving any
// data dependencies. For this, NavyRequestScheduler performs spooling.
// For actual worker, two types of NavyRequestDispatcher are instantiated,
// one for read and the other for the rest of request types
class NavyRequestScheduler : public JobScheduler {
 public:
  // Interval to check the health of the scheduler and dispatchers
  static constexpr size_t kHealthCheckIntervalMs = 1000;
  // Threshold to declare as unhealthy
  static constexpr size_t kHealthMaxStallTimeMs = 1000;

  // @param readerThreads   number of threads for the read scheduler
  // @param writerThreads   number of threads for the write scheduler
  // @param numShardsPower  power of two specification for sharding internally
  //                        to avoid contention and queueing
  explicit NavyRequestScheduler(size_t numReaderThreads,
                                size_t numWriterThreads,
                                size_t maxNumReads,
                                size_t maxNumWrites,
                                size_t stackSize,
                                size_t reqOrderShardPower);
  NavyRequestScheduler(const NavyRequestScheduler&) = delete;
  NavyRequestScheduler& operator=(const NavyRequestScheduler&) = delete;
  ~NavyRequestScheduler() override;

  // Put a job into the queue based on the key hash
  // execution ordering of the key is guaranteed
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobType type,
                      uint64_t key) override;

  // Notify the completion of the request
  void notifyCompletion(uint64_t key) override;

  // waits till all queued and currently running jobs are finished
  void finish() override;

  // Exports the ordered scheduler stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const override;

 private:
  void submitSpooledReq(size_t shard);

  // check if the contexts are healthy, i.e., the response time is reasonable
  void checkHealth(
      std::vector<std::shared_ptr<NavyRequestDispatcher>>& dispatchers,
      std::vector<NavyRequestDispatcher::Stats>& lastStats);

  // Return the context for the key and type
  NavyRequestDispatcher& getDispatcher(uint64_t keyHash, JobType type) {
    if (type == JobType::Read) {
      return *readerDispatchers_[keyHash % numReaderThreads_];
    }
    return *writerDispatchers_[keyHash % numWriterThreads_];
  }

  const size_t numReaderThreads_;
  const size_t numWriterThreads_;
  // sharding power
  const size_t numShards_;

  bool stopped_{false};

  std::vector<std::shared_ptr<NavyRequestDispatcher>> readerDispatchers_;
  std::vector<std::shared_ptr<NavyRequestDispatcher>> writerDispatchers_;

  // mutex per shard. mutex protects the pending jobs and spooling state
  mutable std::vector<TimedMutex> mutexes_;

  // sharding power
  // const size_t numShardsPower_;
  // list of jobs per shard if there is already a job pending for the shard
  std::vector<std::list<std::unique_ptr<NavyRequest>>> pendingReqs_;

  // indicates if there is a pending job for the shard. This is kept as an
  // uint64_t instead of bool to ensure atomicity with sharded locks.
  std::vector<uint64_t> shouldSpool_;

  // total number of jobs that were spooled due to ordering.
  AtomicCounter numSpooled_{0};

  // current number of jobs in the spool
  AtomicCounter currSpooled_{0};

  // Thread to check the health of the scheduler and dispatchers
  std::unique_ptr<NavyThread> healthThread_;
  TimedMutex healthMutex_;

  // The number of times health check failed
  uint64_t numUnhealthy_{0};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
