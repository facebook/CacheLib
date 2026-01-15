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

#include <folly/coro/Promise.h>
#include <folly/coro/Task.h>

#include <concepts>

#include "cachelib/navy/common/NavyThread.h"
#include "cachelib/navy/common/Types.h"

namespace facebook::cachelib::interface::utils {
namespace detail {
using DefaultCleanupT = decltype([](auto&&) {});
} // namespace detail

/**
 * Run a (potentially async) function on a fiber on a Navy worker thread and
 * block until the operation completes. Returns the output from the function.
 *
 * Your function should return a folly::Expected<ValueT, navy::Status> --
 * onWorkerThread() will continue retrying if it returns a navy::Status::Retry
 * error. Note that the return value from the fiber *must* be movable.  If your
 * type isn't movable, wrap it in a unique_ptr.
 *
 * onWorkerThread() runs cleanup() on the fiber if the awaiting coroutine got
 * cancelled while the fiber was still executing.  This allows cleaning up in
 * order to prevent stranded resources (e.g., open RegionDescriptors).
 *
 * @param func function to run on a worker fiber
 * @param cleanup function to run if the coroutine got cancelled while the
 * fiber was executing func()
 * @return output from func()
 */
template <typename FuncT,
          typename CleanupFuncT = detail::DefaultCleanupT,
          std::movable ReturnT = std::invoke_result_t<FuncT>>
folly::coro::Task<ReturnT> onWorkerThread(
    navy::NavyThread& thread,
    FuncT&& func,
    CleanupFuncT&& cleanup = detail::DefaultCleanupT{}) {
  // Wrap the fiber output in an RAII struct; this allows the fiber to run
  // cleanup() if the coroutine doesn't consume the output.
  struct CleanupHelper {
    explicit CleanupHelper(CleanupFuncT&& cleanup)
        : cleanup_(std::forward<CleanupFuncT>(cleanup)), consumed_(false) {}
    CleanupHelper(CleanupHelper&& other) noexcept
        : cleanup_(std::move(other.cleanup_)),
          result_(std::move(other.result_)),
          consumed_(other.consumed_) {
      other.consumed_ = true;
    }

    CleanupHelper(const CleanupHelper&) = delete;
    CleanupHelper& operator=(const CleanupHelper&) = delete;
    CleanupHelper& operator=(CleanupHelper&& other) noexcept = delete;

    ~CleanupHelper() {
      if (!consumed_) {
        cleanup_(std::move(result_));
      }
    }

    CleanupFuncT cleanup_;
    ReturnT result_;
    bool consumed_;
  };

  auto promiseFuturePair = folly::coro::makePromiseContract<CleanupHelper>();
  auto cancellationToken = co_await folly::coro::co_current_cancellation_token;
  thread.addTaskRemote(
      [func = std::forward<FuncT>(func),
       promise = std::move(promiseFuturePair.first),
       cleanupHelper = CleanupHelper(std::forward<CleanupFuncT>(cleanup)),
       token = cancellationToken]() mutable {
        // setting error is required to kick off the first loop
        cleanupHelper.result_ = folly::makeUnexpected(navy::Status::Retry);
        while (cleanupHelper.result_.hasError() &&
               cleanupHelper.result_.error() == navy::Status::Retry &&
               !token.isCancellationRequested()) {
          try {
            cleanupHelper.result_ = func();
          } catch (...) {
            // Not going to produce a value so don't run the cleanup
            cleanupHelper.consumed_ = true;
            promise.setException(std::current_exception());
            return;
          }
        }
        promise.setValue(std::move(cleanupHelper));
      });

  auto retVal = co_await std::move(promiseFuturePair.second);
  retVal.consumed_ = true;
  co_return std::move(retVal.result_);
}

} // namespace facebook::cachelib::interface::utils
