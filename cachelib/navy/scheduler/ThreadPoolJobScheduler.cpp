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

#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>
#include <folly/system/ThreadName.h>

#include <cassert>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

std::unique_ptr<JobScheduler> createOrderedThreadPoolJobScheduler(
    unsigned int readerThreads,
    unsigned int writerThreads,
    unsigned int reqOrderShardPower) {
  return std::make_unique<OrderedThreadPoolJobScheduler>(
      readerThreads, writerThreads, reqOrderShardPower);
}

ThreadPoolExecutor::ThreadPoolExecutor(uint32_t numThreads,
                                       folly::StringPiece name)
    : name_{name}, queues_(numThreads) {
  XDCHECK_GT(numThreads, 0u);
  workers_.reserve(numThreads);
  for (uint32_t i = 0; i < numThreads; i++) {
    queues_[i] = std::make_unique<JobQueue>();
    workers_.emplace_back(
        [&q = queues_[i],
         threadName = folly::sformat("navy_{}_{}", name.subpiece(0, 6), i)] {
          folly::setThreadName(threadName);
          q->process();
        });
  }
}

void ThreadPoolExecutor::enqueue(Job job,
                                 folly::StringPiece name,
                                 JobQueue::QueuePos pos) {
  auto index = nextQueue_.fetch_add(1, std::memory_order_relaxed);
  queues_[index % queues_.size()]->enqueue(std::move(job), name, pos);
}

void ThreadPoolExecutor::enqueueWithKey(Job job,
                                        folly::StringPiece name,
                                        JobQueue::QueuePos pos,
                                        uint64_t key) {
  queues_[key % queues_.size()]->enqueue(std::move(job), name, pos);
}

uint64_t ThreadPoolExecutor::finish() {
  uint64_t ec = 0;
  for (auto& q : queues_) {
    ec += q->finish();
  }
  return ec;
}

void ThreadPoolExecutor::join() {
  for (auto& q : queues_) {
    q->requestStop();
  }
  XLOGF(INFO, "JobScheduler: join threads for {}", name_);
  for (auto& t : workers_) {
    t.join();
  }
  XLOGF(INFO, "JobScheduler: join threads for {} successful", name_);
  workers_.clear();
}

ThreadPoolExecutor::Stats ThreadPoolExecutor::getStats() const {
  Stats stats;
  for (const auto& q : queues_) {
    auto js = q->getStats();
    stats.jobsDone += js.jobsDone;
    stats.jobsHighReschedule += js.jobsHighReschedule;
    stats.reschedules += js.reschedules;
    stats.maxQueueLen = std::max(stats.maxQueueLen, js.maxQueueLen);
    stats.maxPendingJobs += js.maxQueueLen;
  }
  return stats;
}

ThreadPoolJobScheduler::ThreadPoolJobScheduler(uint32_t readerThreads,
                                               uint32_t writerThreads)
    : reader_(readerThreads, "reader_pool"),
      writer_(writerThreads, "writer_pool") {}

void ThreadPoolJobScheduler::enqueue(Job job,
                                     folly::StringPiece name,
                                     JobType type) {
  switch (type) {
  case JobType::Read:
    reader_.enqueue(std::move(job), name, JobQueue::QueuePos::Back);
    break;
  case JobType::Write:
    writer_.enqueue(std::move(job), name, JobQueue::QueuePos::Back);
    break;
  case JobType::Reclaim:
    writer_.enqueue(std::move(job), name, JobQueue::QueuePos::Front);
    break;
  case JobType::Flush:
    writer_.enqueue(std::move(job), name, JobQueue::QueuePos::Front);
    break;
  default:
    XLOGF(ERR,
          "JobScheduler: unrecognized job type: {}",
          static_cast<uint32_t>(type));
    XDCHECK(false);
  }
}

void ThreadPoolJobScheduler::enqueueWithKey(Job job,
                                            folly::StringPiece name,
                                            JobType type,
                                            uint64_t key) {
  switch (type) {
  case JobType::Read:
    reader_.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Back, key);
    break;
  case JobType::Write:
    writer_.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Back, key);
    break;
  case JobType::Reclaim:
    writer_.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Front,
                           key);
    break;
  case JobType::Flush:
    writer_.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Front,
                           key);
    break;
  default:
    XLOGF(ERR,
          "JobScheduler: unrecognized job type: {}",
          static_cast<uint32_t>(type));
    XDCHECK(false);
  }
}

void ThreadPoolJobScheduler::join() {
  reader_.join();
  writer_.join();
}

void ThreadPoolJobScheduler::finish() {
  uint64_t enqueueTotalCount = 0;
  while (true) {
    const uint64_t ec = reader_.finish() + writer_.finish();

    // After iterating over queues, we can't guarantee that any of finished jobs
    // didn't enqueue another job in a queue that we think "finished". We finish
    // queues again and again until no more jobs scheduled.
    if (ec == enqueueTotalCount) {
      break;
    }
    enqueueTotalCount = ec;
  }
}

void ThreadPoolJobScheduler::getCounters(const CounterVisitor& visitor) const {
  auto getStats = [&visitor](ThreadPoolExecutor::Stats stats,
                             folly::StringPiece name) {
    const std::string maxQueueLen =
        folly::sformat("navy_{}_max_queue_len", name);
    const std::string reschedules = folly::sformat("navy_{}_reschedules", name);
    const std::string highReschedules =
        folly::sformat("navy_{}_jobs_high_reschedule", name);
    const std::string jobsDone = folly::sformat("navy_{}_jobs_done", name);
    const std::string maxPendingJobs =
        folly::sformat("navy_max_{}_pending_jobs", name);
    visitor(maxQueueLen, stats.maxQueueLen);
    visitor(reschedules, stats.reschedules, CounterVisitor::CounterType::RATE);
    visitor(highReschedules, stats.jobsHighReschedule);
    visitor(jobsDone, stats.jobsDone, CounterVisitor::CounterType::RATE);
    visitor(maxPendingJobs, stats.maxPendingJobs);
  };
  getStats(reader_.getStats(), reader_.getName());
  getStats(writer_.getStats(), writer_.getName());
}

namespace {
constexpr size_t numShards(size_t power) { return 1ULL << power; }
} // namespace

OrderedThreadPoolJobScheduler::OrderedThreadPoolJobScheduler(
    size_t readerThreads, size_t writerThreads, size_t numShardsPower)
    : mutexes_(numShards(numShardsPower)),
      pendingJobs_(numShards(numShardsPower)),
      shouldSpool_(numShards(numShardsPower), false),
      numShardsPower_(numShardsPower),
      scheduler_(readerThreads, writerThreads) {}

void OrderedThreadPoolJobScheduler::enqueueWithKey(Job job,
                                                   folly::StringPiece name,
                                                   JobType type,
                                                   uint64_t key) {
  const auto shard = key % numShards(numShardsPower_);
  JobParams params{std::move(job), type, name, key};
  std::lock_guard<std::mutex> l(mutexes_[shard]);
  if (shouldSpool_[shard]) {
    // add to the pending jobs since there is already a job for this key
    pendingJobs_[shard].emplace_back(std::move(params));
    numSpooled_.inc();
    currSpooled_.inc();
  } else {
    shouldSpool_[shard] = true;
    scheduleJobLocked(std::move(params), shard);
  }
}

void OrderedThreadPoolJobScheduler::scheduleJobLocked(JobParams params,
                                                      uint64_t shard) {
  scheduler_.enqueueWithKey(
      [this, j = std::move(params.job), shard]() mutable {
        auto ret = j();
        if (ret == JobExitCode::Done) {
          scheduleNextJob(shard);
        } else {
          XDCHECK_EQ(ret, JobExitCode::Reschedule);
        }
        return ret;
      },
      params.name,
      params.type,
      params.key);
}

void OrderedThreadPoolJobScheduler::scheduleNextJob(uint64_t shard) {
  std::lock_guard<std::mutex> l(mutexes_[shard]);
  if (pendingJobs_[shard].empty()) {
    shouldSpool_[shard] = false;
    return;
  }

  currSpooled_.dec();
  scheduleJobLocked(std::move(pendingJobs_[shard].front()), shard);
  pendingJobs_[shard].pop_front();
}

void OrderedThreadPoolJobScheduler::enqueue(Job job,
                                            folly::StringPiece name,
                                            JobType type) {
  scheduler_.enqueue(std::move(job), name, type);
}

void OrderedThreadPoolJobScheduler::finish() {
  scheduler_.finish();
  XDCHECK_EQ(currSpooled_.get(), 0ULL);
}

void OrderedThreadPoolJobScheduler::getCounters(const CounterVisitor& v) const {
  scheduler_.getCounters(v);
  v("navy_req_order_spooled", numSpooled_.get(),
    CounterVisitor::CounterType::RATE);
  v("navy_req_order_curr_spool_size", currSpooled_.get());
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
