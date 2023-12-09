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

#include "cachelib/navy/scheduler/NavyRequestScheduler.h"

#include <folly/fibers/ForEach.h>

namespace facebook {
namespace cachelib {
namespace navy {

std::unique_ptr<JobScheduler> createNavyRequestScheduler(
    size_t numReaderThreads,
    size_t numWriterThreads,
    size_t maxNumReads,
    size_t maxNumWrites,
    size_t stackSize,
    size_t reqOrderShardPower) {
  return std::make_unique<NavyRequestScheduler>(numReaderThreads,
                                                numWriterThreads,
                                                maxNumReads,
                                                maxNumWrites,
                                                stackSize,
                                                reqOrderShardPower);
}

NavyRequestScheduler::NavyRequestScheduler(size_t numReaderThreads,
                                           size_t numWriterThreads,
                                           size_t maxNumReads,
                                           size_t maxNumWrites,
                                           size_t stackSize,
                                           size_t numShardsPower)
    : numReaderThreads_(numReaderThreads),
      numWriterThreads_(numWriterThreads),
      numShards_(1ULL << numShardsPower),
      mutexes_(numShards_),
      pendingReqs_(numShards_),
      shouldSpool_(numShards_, false) {
  // Adjust the max number of reads and writes if needed
  maxNumReads = std::max(maxNumReads, numReaderThreads);
  maxNumWrites = std::max(maxNumWrites, numWriterThreads);

  for (size_t i = 0; i < numReaderThreads_; i++) {
    auto dispatcher = std::make_shared<NavyRequestDispatcher>(
        *this, fmt::format("navy_reader_{}", i),
        maxNumReads / numReaderThreads_, stackSize);
    readerDispatchers_.emplace_back(std::move(dispatcher));
  }

  for (size_t i = 0; i < numWriterThreads_; i++) {
    auto dispatcher = std::make_shared<NavyRequestDispatcher>(
        *this, fmt::format("navy_writer_{}", i),
        maxNumWrites / numWriterThreads_, stackSize);
    writerDispatchers_.emplace_back(std::move(dispatcher));
  }

  healthThread_ = std::make_unique<NavyThread>("navy_health",
                                               NavyThread::Options(16 * 1024));
  healthMutex_.lock();
  healthThread_->addTaskRemote([this]() {
    size_t numDispatchers = numReaderThreads_ + numWriterThreads_;
    std::vector<NavyRequestDispatcher::Stats> lastStats(numDispatchers);
    std::vector<std::shared_ptr<NavyRequestDispatcher>> dispatchers;
    for (auto dispatcher : readerDispatchers_) {
      dispatchers.emplace_back(dispatcher);
    }
    for (auto dispatcher : writerDispatchers_) {
      dispatchers.emplace_back(dispatcher);
    }

    std::chrono::milliseconds interval(kHealthCheckIntervalMs);
    while (!stopped_) {
      bool status = healthMutex_.try_lock_for(interval);
      XDCHECK(!status);
      checkHealth(dispatchers, lastStats);
    }
  });
}

NavyRequestScheduler::~NavyRequestScheduler() {
  stopped_ = true;
  healthThread_.reset();
  readerDispatchers_.clear();
  writerDispatchers_.clear();
}

void NavyRequestScheduler::enqueueWithKey(Job job,
                                          folly::StringPiece name,
                                          JobType type,
                                          uint64_t key) {
  if (stopped_) {
    return;
  }

  auto req = std::make_unique<NavyRequest>(std::move(job), name, type, key);
  // Allow one request can be outstanding per shard by spooling requests
  // if there is another request already running
  const auto shard = req->getKey() % numShards_;
  std::lock_guard<TimedMutex> l(mutexes_[shard]);
  if (shouldSpool_[shard]) {
    pendingReqs_[shard].emplace_back(std::move(req));
    numSpooled_.inc();
    currSpooled_.inc();
  } else {
    shouldSpool_[shard] = true;
    auto& dispatcher = getDispatcher(req->getKey(), req->getType());
    dispatcher.submitReq(std::move(req));
  }
}

// Notify completion of the request
void NavyRequestScheduler::notifyCompletion(uint64_t key) {
  const auto shard = key % numShards_;
  std::lock_guard<TimedMutex> l(mutexes_[shard]);
  if (pendingReqs_[shard].empty()) {
    shouldSpool_[shard] = false;
    return;
  }

  currSpooled_.dec();
  auto nextReq = std::move(pendingReqs_[shard].front());
  if (!stopped_) {
    auto& dispatcher = getDispatcher(nextReq->getKey(), nextReq->getType());
    dispatcher.submitReq(std::move(nextReq));
  }
  pendingReqs_[shard].pop_front();
}

void NavyRequestScheduler::finish() {}

void NavyRequestScheduler::getCounters(const CounterVisitor& visitor) const {
  auto visitdispatcherStats =
      [&visitor](const std::vector<std::shared_ptr<NavyRequestDispatcher>>&
                     dispatchers,
                 folly::StringPiece name) {
        uint64_t numPolled = 0;
        uint64_t numSubmitted = 0;
        uint64_t numDispatched = 0;
        uint64_t numCompleted = 0;
        uint64_t curOutstanding = 0;

        for (const auto& dispatcher : dispatchers) {
          auto stat = dispatcher->getStats();
          numPolled += stat.numPolled;
          numSubmitted += stat.numSubmitted;
          numDispatched += stat.numDispatched;
          numCompleted += stat.numCompleted;
          curOutstanding += stat.curOutstanding;
        }

        auto prefix = fmt::format("navy_jobs.{}_", name);
        visitor(prefix + "polled", numPolled,
                CounterVisitor::CounterType::RATE);
        visitor(prefix + "submitted", numSubmitted,
                CounterVisitor::CounterType::RATE);
        visitor(prefix + "dispatched", numDispatched,
                CounterVisitor::CounterType::RATE);
        visitor(prefix + "completed", numCompleted,
                CounterVisitor::CounterType::RATE);
        visitor(prefix + "outstanding", curOutstanding);
      };

  visitdispatcherStats(readerDispatchers_, "reader");
  visitdispatcherStats(writerDispatchers_, "writer");

  visitor("navy_jobs.spooled.curr", currSpooled_.get());
  visitor("navy_jobs.spooled.total", numSpooled_.get());
}

void NavyRequestScheduler::checkHealth(
    std::vector<std::shared_ptr<NavyRequestDispatcher>>& dispatchers,
    std::vector<NavyRequestDispatcher::Stats>& lastStats) {
  bool healthy = true;
  std::vector<std::function<bool()>> funcs;
  for (size_t i = 0; i < dispatchers.size(); i++) {
    auto stat = dispatchers[i]->getStats();
    auto& lastStat = lastStats[i];
    auto numQueued = stat.numSubmitted - lastStat.numDispatched;
    auto numPolled = stat.numPolled - lastStat.numPolled;
    if (numQueued > 0 && lastStat.numDispatched == stat.numDispatched) {
      XLOGF(ERR,
            "[{}] dispatch stalled. polled {} submitted {} -> {} dispatched "
            "{} -> {}",
            dispatchers[i]->getName(), numPolled, lastStat.numSubmitted,
            stat.numSubmitted, lastStat.numDispatched, stat.numDispatched);
      healthy = false;
    }
    if (lastStat.curOutstanding > 0 &&
        lastStat.numCompleted == stat.numCompleted) {
      XLOGF(ERR, "[{}] IO stalled. submitted {} -> {} completed {} -> {}",
            dispatchers[i]->getName(), lastStat.numSubmitted, stat.numSubmitted,
            lastStat.numCompleted, stat.numCompleted);
      healthy = false;
    }
    lastStats[i] = stat;

    funcs.push_back([i, &healthy, &dispatchers]() {
      auto& dispatcher = dispatchers[i];
      auto baton = std::make_shared<folly::fibers::Baton>();
      // Check the loop time of the eventbase by pushing a dummy task
      dispatcher->addTaskRemote([bat = baton]() { bat->post(); });

      auto startTime = util::getCurrentTimeMs();
      if (!baton->try_wait_for(
              std::chrono::milliseconds(kHealthMaxStallTimeMs))) {
        auto curTime = util::getCurrentTimeMs();
        XLOGF(ERR, "[{}] eventbase loop stalled for {} ms",
              dispatchers[i]->getName(), curTime - startTime);
        healthy = false;
        return false;
      }
      return true;
    });
  }

  folly::fibers::forEach(funcs.begin(), funcs.end(), [](size_t, bool) {});
  if (!healthy) {
    numUnhealthy_++;
    XLOG_N_PER_MS(ERR, 10, 600000) << folly::sformat(
        "Navy job scheduler health check failed ({})", numUnhealthy_);
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
