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

#include "cachelib/common/PeriodicWorker.h"

#include <folly/Memory.h>
#include <folly/logging/xlog.h>
#include <folly/system/ThreadName.h>

#include <chrono>

namespace {

std::chrono::system_clock::time_point getSysClock(
    std::chrono::milliseconds timeout) {
  return std::chrono::system_clock::now() + timeout;
}
} // namespace

using namespace facebook::cachelib;
using LockHolder = std::unique_lock<std::timed_mutex>;

/* Implementation is needed since the PeriodicWorker destructor will be
 * eventually called */
PeriodicWorker::~PeriodicWorker() {
  try {
    stop(std::chrono::milliseconds(0));
  } catch (const std::exception&) {
  }
  XDCHECK(workerThread_ == nullptr);
}

void PeriodicWorker::loop(void) {
  /* Wait for interval_ time before executing work again */
  auto breakOut = [this]() { return wakeUp_ || shouldStopWork_; };

  LockHolder l(lock_);
  preWork();

  while (!shouldStopWork_) {
    l.unlock();
    work();
    const std::chrono::milliseconds timeoutMs(interval_);
    runCount_.fetch_add(1, std::memory_order_relaxed);
    l.lock();
    cond_.wait_until(l, getSysClock(timeoutMs), breakOut);
    wakeUp_ = false;
  }

  /* Indicate to the context stopping the work that the work was stopped
   * successfully. The stopping context sets this to "true" when work is
   * happening, and waits for this field to flip. */
  shouldStopWork_ = false;
  l.unlock();

  cond_.notify_one();
  return;
}

bool PeriodicWorker::start(const std::chrono::milliseconds sleepInterval,
                           const folly::StringPiece thread_name) {
  if (sleepInterval == std::chrono::milliseconds::zero()) {
    return false;
  }

  setInterval(sleepInterval);

  LockHolder l(lock_);
  if (workerThread_) {
    return true;
  }

  auto runLoop = [this]() { loop(); };
  workerThread_ = std::make_unique<std::thread>(std::move(runLoop));

  /*
   * Set the name of the thread
   */
  folly::setThreadName(workerThread_->native_handle(), thread_name);

  return true;
}

bool PeriodicWorker::shouldStopWork() const {
  LockHolder l(lock_);
  return shouldStopWork_;
}
bool PeriodicWorker::stop(const std::chrono::milliseconds timeout) {
  LockHolder l(lock_);
  if (shouldStopWork_) {
    return false;
  }

  if (!workerThread_) {
    return true;
  } else {
    shouldStopWork_ = true;

    /* Notify worker to stop processing. We do this holding the lock
     * instead of an unlock and subsequent lock, we let go off the lock
     * soon enough (on cond_wait) if we wait/block. */
    cond_.notify_one();

    /* Wait for at least timeout seconds when work is in progress for
     * worker to get done and flip the variable to !shouldStopWork,
     * sleep for a long time (forever) when timeout == 0 */
    const auto waitUntil = (timeout == std::chrono::milliseconds::zero())
                               ? std::chrono::system_clock::time_point::max()
                               : getSysClock(timeout);

    auto breakOut = [this]() { return !shouldStopWork_; };
    cond_.wait_until(l, waitUntil, breakOut);

    if (shouldStopWork_) {
      /* Wait was terminated due to a timeout and the predicate still
       * does not hold true, let the work continue and fail stop() */
      shouldStopWork_ = false;
      return false;
    }

    /* When we are here worker should
     * be in the process of terminating. Lets wait for the thread to
     * terminate before completing stop().  A new worker instance can be
     * spun once we unlock below. */
    auto tmp = std::move(workerThread_);
    XDCHECK(workerThread_ == nullptr);
    l.unlock();

    if (tmp->joinable()) {
      tmp->join();
    }
  }
  return true;
}
