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

#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#include <atomic>
#include <memory>

#include "cachelib/common/ConditionVariable.h"

namespace facebook {
namespace cachelib {
namespace navy {

class NavyThread;

// @return the current NavyThread context if running on NavyThread. nullptr
// otherwise.
NavyThread* getCurrentNavyThread();

/**
 * NavyThread is a wrapper class that wraps folly::ScopedEventBaseThread and
 * FiberManager. The purpose of NavyThread is to start a new thread running
 * an EventBase loop and FiberManager loop along with providing an deligate
 * interface to add tasks to the FiberManager.
 *
 * NavyThread is not CopyConstructible nor CopyAssignable nor
 * MoveConstructible nor MoveAssignable.
 */
class NavyThread {
 public:
  struct Options {
    static constexpr size_t kDefaultStackSize{64 * 1024};
    constexpr Options() {}

    explicit Options(size_t size) : stackSize(size) {}

    /**
     * Maximum stack size for fibers which will be used for executing all the
     * tasks.
     */
    size_t stackSize{kDefaultStackSize};
  };

  /**
   * Initializes with current EventBaseManager and passed-in thread name.
   */
  explicit NavyThread(folly::StringPiece name, Options options = Options());

  ~NavyThread();

  /**
   * Add the passed-in task to the FiberManager.
   *
   * @param func Task functor; must have a signature of `void func()`.
   *             The object will be destroyed once task execution is complete.
   */
  void addTaskRemote(folly::Func func) {
    {
      std::unique_lock<folly::fibers::TimedMutex> lk(drainMutex_);
      numRunning_++;
    }

    fm_->addTaskRemote([this, func = std::move(func)]() mutable {
      func();
      std::unique_lock<folly::fibers::TimedMutex> lk(drainMutex_);
      XDCHECK_GT(numRunning_, 0u);
      if (--numRunning_ == 0u) {
        drainCond_.notifyAll();
      }
    });
  }

  /**
   * Add the passed-in task to the FiberManager.
   * Must be called from FiberManager's thread.
   *
   * @param func Task functor; must have a signature of `void func()`.
   *             The object will be destroyed once task execution is complete.
   */
  void addTask(folly::Func func) {
    {
      std::unique_lock<folly::fibers::TimedMutex> lk(drainMutex_);
      numRunning_++;
    }

    fm_->addTask([this, func = std::move(func)]() mutable {
      func();
      std::unique_lock<folly::fibers::TimedMutex> lk(drainMutex_);
      XDCHECK_GT(numRunning_, 0u);
      if (--numRunning_ == 0u) {
        drainCond_.notifyAll();
      }
    });
  }

  /**
   * Wait until tasks are drained
   */
  void drain() {
    std::unique_lock<folly::fibers::TimedMutex> lk(drainMutex_);
    if (numRunning_ == 0) {
      return;
    }
    drainCond_.wait(lk);
  }

 private:
  NavyThread(NavyThread&& other) = delete;
  NavyThread& operator=(NavyThread&& other) = delete;

  NavyThread(const NavyThread& other) = delete;
  NavyThread& operator=(const NavyThread& other) = delete;

  // Actual worker thread running EventBase and FiberManager loop
  std::unique_ptr<folly::ScopedEventBaseThread> th_;

  // FiberManager which are driven by the thread
  folly::fibers::FiberManager* fm_;

  size_t numRunning_{0};
  folly::fibers::TimedMutex drainMutex_;
  util::ConditionVariable drainCond_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
