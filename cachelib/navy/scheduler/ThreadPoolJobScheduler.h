#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Job queue. Users it creates threads and calls @process in these threads.
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

  // @name for debugging
  ThreadPoolExecutor(uint32_t numThreads, folly::StringPiece name);

  void enqueue(Job job, folly::StringPiece name, JobQueue::QueuePos pos);

  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobQueue::QueuePos pos,
                      uint64_t key);

  uint64_t finish();

  void join();

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

  void enqueue(Job job, folly::StringPiece name, JobType type) override;
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobType type,
                      uint64_t key) override;

  void finish() override;

  uint64_t getTotalSpooled() const { return numSpooled_.get(); }

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
