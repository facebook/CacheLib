#pragma once

#include <memory>

#include <folly/Function.h>

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

enum class JobType { Read, Write, Reclaim };

class JobScheduler {
 public:
  virtual ~JobScheduler() = default;
  virtual void enqueue(Job job, folly::StringPiece name, JobType type) = 0;
  // Uses @key to deterministically schedule job on one of available workers.
  virtual void enqueueWithKey(Job job,
                              folly::StringPiece name,
                              JobType type,
                              uint64_t key) = 0;
  virtual void finish() = 0;
  virtual void getCounters(const CounterVisitor& visitor) const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
