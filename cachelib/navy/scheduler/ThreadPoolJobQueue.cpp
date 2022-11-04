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

#include "cachelib/navy/scheduler/ThreadPoolJobQueue.h"

#include "cachelib/common/Utils.h"
namespace facebook {
namespace cachelib {
namespace navy {

namespace {
constexpr uint64_t kHighRescheduleCount = 250;
constexpr uint64_t kHighRescheduleReportRate = 100;

// A scoped unlock, it unlocks the given lock and lock it back when out of scope
// (destructor).
template <typename T>
class ScopedUnlock {
 public:
  explicit ScopedUnlock(std::unique_lock<T>& lock) : lock_{lock} {
    lock_.unlock();
  }
  ScopedUnlock(const ScopedUnlock&) = delete;
  ScopedUnlock& operator=(const ScopedUnlock&) = delete;
  ~ScopedUnlock() { lock_.lock(); }

 private:
  std::unique_lock<T>& lock_;
};
} // namespace

void JobQueue::enqueue(Job job, folly::StringPiece name, QueuePos pos) {
  bool wasEmpty = false;
  {
    std::lock_guard<std::mutex> lock{mutex_};
    wasEmpty = queue_.empty() && processing_ == 0;
    if (!stop_) {
      if (pos == QueuePos::Front) {
        queue_.emplace_front(std::move(job), name);
      } else {
        XDCHECK_EQ(pos, QueuePos::Back);
        queue_.emplace_back(std::move(job), name);
      }
      enqueueCount_++;
    }
    maxQueueLen_ = std::max<uint64_t>(maxQueueLen_, queue_.size());
  }

  if (wasEmpty) {
    cv_.notify_one();
  }
}

void JobQueue::requestStop() {
  std::lock_guard<std::mutex> lock{mutex_};
  if (!stop_) {
    stop_ = true;
    cv_.notify_all();
  }
}

void JobQueue::process() {
  std::unique_lock<std::mutex> lock{mutex_};
  while (!stop_) {
    if (queue_.empty()) {
      cv_.wait(lock);
    } else {
      auto entry = std::move(queue_.front());
      queue_.pop_front();
      processing_++;
      lock.unlock();

      auto exitCode = runJob(entry);

      lock.lock();
      processing_--;

      switch (exitCode) {
      case JobExitCode::Reschedule: {
        queue_.emplace_back(std::move(entry));
        if (queue_.size() == 1) {
          // In really rare case let's be a little better than busy wait
          ScopedUnlock<std::mutex> unlock{lock};
          std::this_thread::yield();
        }
        break;
      }
      case JobExitCode::Done: {
        jobsDone_++;
        if (entry.rescheduleCount > 100) {
          jobsHighReschedule_++;
        }
        reschedules_ += entry.rescheduleCount;
        break;
      }
      }
    }
  }
}

JobExitCode JobQueue::runJob(QueueEntry& entry) {
  auto safeJob = [&entry] {
    try {
      return entry.job();
    } catch (const std::exception& ex) {
      XLOGF(ERR, "Exception thrown from the job: {}", ex.what());
      util::printExceptionStackTraces();
      throw;
    } catch (...) {
      XLOG(ERR, "Unknown exception thrown from the job");
      util::printExceptionStackTraces();
      throw;
    }
  };

  auto exitCode = safeJob();
  switch (exitCode) {
  case JobExitCode::Reschedule: {
    entry.rescheduleCount++;
    if (entry.rescheduleCount >= kHighRescheduleCount &&
        entry.rescheduleCount % kHighRescheduleReportRate == 0) {
      XLOGF(DBG,
            "Job '{}' rescheduled {} times",
            entry.name,
            entry.rescheduleCount);
    }
    break;
  }
  case JobExitCode::Done: {
    // Deallocate outside the lock:
    entry.job = Job{};
    break;
  }
  }
  return exitCode;
}

uint64_t JobQueue::finish() {
  // Busy wait, but used only in tests
  std::unique_lock<std::mutex> lock{mutex_};
  while (processing_ != 0 || !queue_.empty()) {
    ScopedUnlock<std::mutex> unlock{lock};
    std::this_thread::yield();
  }
  return enqueueCount_;
}

JobQueue::Stats JobQueue::getStats() const {
  Stats stats;
  std::unique_lock<std::mutex> lock{mutex_};
  stats.jobsDone = jobsDone_;
  stats.jobsHighReschedule = jobsHighReschedule_;
  stats.reschedules = reschedules_;
  stats.maxQueueLen = maxQueueLen_;
  maxQueueLen_ = queue_.size();
  return stats;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
