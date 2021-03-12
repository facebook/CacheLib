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
