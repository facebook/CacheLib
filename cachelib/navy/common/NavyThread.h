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
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#include <memory>

namespace facebook {
namespace cachelib {
namespace navy {

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
  /**
   * Initializes with current EventBaseManager and passed-in thread name.
   */
  explicit NavyThread(folly::StringPiece name)
      : th_(name), fm_(&folly::fibers::getFiberManager(*th_.getEventBase())) {}

  ~NavyThread() = default;

  /**
   * Add the passed-in task to the FiberManager.
   *
   * @param func Task functor; must have a signature of `void func()`.
   *             The object will be destroyed once task execution is complete.
   */
  void addTaskRemote(folly::Func func) { fm_->addTaskRemote(std::move(func)); }

  /**
   * Add the passed-in task to the FiberManager.
   * Must be called from FiberManager's thread.
   *
   * @param func Task functor; must have a signature of `void func()`.
   *             The object will be destroyed once task execution is complete.
   */
  void addTask(folly::Func func) { fm_->addTask(std::move(func)); }

 private:
  NavyThread(NavyThread&& other) = delete;
  NavyThread& operator=(NavyThread&& other) = delete;

  NavyThread(const NavyThread& other) = delete;
  NavyThread& operator=(const NavyThread& other) = delete;

  // Actual worker thread running EventBase and FiberManager loop
  folly::ScopedEventBaseThread th_;

  // FiberManager which are driven by the thread
  folly::fibers::FiberManager* fm_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
