#pragma once

#include <atomic>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
// This job scheduler doesn't have any associated thread. User manually runs
// queued jobs. Used in tests.
class MockJobScheduler : public JobScheduler {
 public:
  MockJobScheduler() = default;
  ~MockJobScheduler() override;

  void enqueue(Job job, folly::StringPiece name, JobType type) override;
  void enqueueWithKey(Job job,
                      folly::StringPiece name,
                      JobType type,
                      uint64_t /* key */) override {
    // Ignore @key because mock scheduler models one thread job scheduler
    enqueue(std::move(job), name, type);
  }

  void finish() override;

  void getCounters(const CounterVisitor&) const override {}

  bool runFirst() { return runFirstIf(""); }

  // Runs first job if its name matches @expected, throws std::logic_error
  // otherwise. Returns true if job is done, false if rescheduled.
  bool runFirstIf(folly::StringPiece expected);

  size_t getQueueSize() const {
    std::lock_guard<std::mutex> lock{m_};
    return q_.size();
  }

  size_t getDoneCount() const {
    std::lock_guard<std::mutex> lock{m_};
    return doneCount_;
  }

 protected:
  struct JobName {
    Job job;
    folly::StringPiece name{};

    JobName(Job j, folly::StringPiece n) : job{std::move(j)}, name{n} {}

    bool nameIs(folly::StringPiece expected) const {
      return expected.size() == 0 || expected == name;
    }
  };

  [[noreturn]] static void throwLogicError(const std::string& what);

  bool runFirstIfLocked(folly::StringPiece expected,
                        std::unique_lock<std::mutex>& lock);

  std::deque<JobName> q_;
  size_t doneCount_{};
  std::atomic<bool> processing_{false};
  mutable std::mutex m_;
};

// This job scheduler runs everything on one thread. Used in tests.
class MockSingleThreadJobScheduler : public MockJobScheduler {
 public:
  MockSingleThreadJobScheduler()
      : t_{[this] {
          while (!stopped_) {
            process();
          }
        }} {}
  ~MockSingleThreadJobScheduler() override {
    stopped_ = true;
    t_.join();
  }

 private:
  void process();

  std::atomic<bool> stopped_{false};
  std::thread t_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
