#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>
#include <folly/system/ThreadName.h>

#include <cassert>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
constexpr uint64_t kHighRescheduleCount = 250;
constexpr uint64_t kHighRescheduleReportRate = 100;

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
    wasEmpty = queue_.empty();
    if (!stop_) {
      if (pos == QueuePos::Front) {
        queue_.emplace_front(std::move(job), name);
      } else {
        XDCHECK_EQ(pos, QueuePos::Back);
        queue_.emplace_back(std::move(job), name);
      }
      enqueueCount_++;
    }
    maxQueueLen_ = std::max(maxQueueLen_, queue_.size());
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
      // TODO: Histogram of reschedule count
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
    if (entry.rescheduleCount > 0) {
      XLOGF(DBG,
            "Job '{}' rescheduled {} times total",
            entry.name,
            entry.rescheduleCount);
    }
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
    visitor(reschedules, stats.reschedules);
    visitor(highReschedules, stats.jobsHighReschedule);
    visitor(jobsDone, stats.jobsDone);
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
  v("navy_req_order_spooled", numSpooled_.get());
  v("navy_req_order_curr_spool_size", currSpooled_.get());
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
