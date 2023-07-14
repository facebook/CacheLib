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

#include <folly/Range.h>

#include <atomic>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>

namespace facebook {
namespace cachelib {

/* This provides a worker object that periodically performs some work. The
 * worker can be stopped or started any number of times.
 *
 * Ensures that the current iteration of work is completed before returning
 * success. The worker is stopped when the object goes out of scope.  It also
 * executes a pre-work and post-work functions.
 */
class PeriodicWorker {
 public:
  PeriodicWorker() = default;
  PeriodicWorker(const PeriodicWorker&) = delete;
  PeriodicWorker& operator=(const PeriodicWorker&) = delete;
  /* Start the worker thread with given worker period if not already
   * started.
   *
   * @param sleepInterval  The sleep interval for the worker thread in
   *                       milliseconds
   * @return   true if a worker thread was initialized. false if either
   *           the worker thread could not be created or if the sleep
   *           interval is 0 or worker was already running
   */
  bool start(const std::chrono::milliseconds sleepInterval,
             const folly::StringPiece = "");

  /* Stop the worker thread and executes the post work fn. On success,
   * this will ensure that the thread is terminated properly.
   *
   * @param timeout  The timeout for the stop operation, 0 means forever
   * @return If successfully stopped, returns true. False otherwise
   */
  bool stop(const std::chrono::milliseconds timeout = std::chrono::seconds(0));

  /* Set the sleep interval for the worker. The value may only take
   * effect after the current iteration of the loop depending on where
   * the worker thread is in its execution.
   *
   * @param interval  The value in milliseconds for the sleep interval
   * @return If successfully set, returns true. False otherwise
   */

  void setInterval(const std::chrono::milliseconds interval) noexcept {
    interval_ = interval.count();
  }

  /* Derived classes need to ensure worker thread is stopped before object
   * is destroyed by calling stop() in their destructors since the
   * worker may access their resources. */
  virtual ~PeriodicWorker() = 0;

  /* Get sleep interval for the worker.
   *
   * @return value of the sleep interval for the worker
   */
  std::chrono::milliseconds getInterval() const noexcept {
    return std::chrono::milliseconds(interval_);
  }

  uint64_t getRunCount() const noexcept {
    return runCount_.load(std::memory_order_relaxed);
  }

  /* Wake up the worker */
  void wakeUp() noexcept {
    {
      std::unique_lock<std::timed_mutex> l(lock_);
      wakeUp_ = true;
    }
    cond_.notify_one();
  }

 protected:
  /* Has the work been asked to be stopped?
   *
   * @return true when the worker has been marked for stopping, false
   *         otherwise
   */
  bool shouldStopWork() const;

 private:
  /* Function that is executed before work is run for the first time */
  virtual void preWork() {}

  /* Function that represents the work to be executed periodically */
  virtual void work() = 0;

  /* Worker thread which will periodically do the work */
  std::unique_ptr<std::thread> workerThread_;

  /* Sleep interval for the worker thread in milliseconds */
  std::atomic<uint64_t> interval_{0};

  /* State to track whether the worker thread should stop. This starts
   * off as false by default, and is flipped to true by the stop(ping)
   * thread */
  bool shouldStopWork_ = false;

  /* State to track whether the worker thread should wake up from sleep. This
   * starts off as false by default, and is flipped to true by the wakeUp call
   */
  bool wakeUp_ = false;

  /* Condition variable to signal stopper or worker thread */
  std::condition_variable_any cond_{};

  /* Mutex controlling the thread, interval and work state */
  mutable std::timed_mutex lock_{};

  /* Number of times in loop() */
  std::atomic<uint64_t> runCount_{0};

  /* The main worker loop that handles the work periodically */
  void loop(void);
};
} // namespace cachelib
} // namespace facebook
