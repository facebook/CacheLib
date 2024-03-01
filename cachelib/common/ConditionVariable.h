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

#include <folly/IntrusiveList.h>
#include <folly/Synchronized.h>
#include <folly/fibers/Baton.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {
namespace util {

using folly::fibers::Baton;
using folly::fibers::TimedMutex;
using SharedMutex = folly::fibers::TimedRWMutex<Baton>;

/**
 * ConditionVariable is a synchronization primitive that can be used to block
 * until a condition is satisfied. This is the implementation of
 * ConditionVariable supporting both thread and fiber waiters.
 */
class ConditionVariable {
 public:
  struct Waiter {
    Waiter() = default;

    // The baton will be signalled when this condition variable is notified
    Baton baton_;

   private:
    friend ConditionVariable;
    folly::SafeIntrusiveListHook hook_;
  };

  // addWaiter adds a waiter to the wait list with the waitList lock held,
  // such that the actual wait can be made asynchronously with the baton
  void addWaiter(Waiter* waiter) {
    XDCHECK(waiter);
    auto waitListLock = waitList_.wlock();
    waitListLock->push_back(*waiter);
    numWaiters_++;
  }

  size_t numWaiters() { return numWaiters_; }

  void wait(std::unique_lock<TimedMutex>& lock) {
    Waiter waiter;
    addWaiter(&waiter);
    lock.unlock();
    waiter.baton_.wait();
    lock.lock();
  }

  // Expect to be protected by the same lock
  void notifyOne() {
    auto waitListLock = waitList_.wlock();
    auto& waitList = *waitListLock;

    if (waitList.empty()) {
      return;
    }

    auto waiter = &waitList.front();
    waitList.pop_front();
    XDCHECK_GT(numWaiters_, 0u);
    numWaiters_--;
    waiter->baton_.post();
  }

  // Expect to be protected by the same lock
  void notifyAll() {
    auto waitListLock = waitList_.wlock();
    auto& waitList = *waitListLock;

    if (waitList.empty()) {
      return;
    }

    while (!waitList.empty()) {
      auto waiter = &waitList.front();
      waitList.pop_front();
      XDCHECK_GT(numWaiters_, 0u);
      numWaiters_--;
      waiter->baton_.post();
    }
  }

 private:
  using WaiterList = folly::SafeIntrusiveList<Waiter, &Waiter::hook_>;
  folly::Synchronized<WaiterList, SharedMutex> waitList_;
  size_t numWaiters_ = 0;
};

} // namespace util
} // namespace cachelib
} // namespace facebook
