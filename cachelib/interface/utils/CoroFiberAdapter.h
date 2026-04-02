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

#include <folly/Random.h>
#include <folly/coro/Promise.h>
#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>

#include <chrono>
#include <concepts>

#include "cachelib/navy/common/NavyThread.h"
#include "cachelib/navy/common/Types.h"

namespace facebook::cachelib::interface::utils {
namespace detail {
using DefaultCleanupT = decltype([](auto&&) {});
} // namespace detail

/**
 * Executor for running tasks on fibers on NavyThreads.
 *
 * NavyThread::addTaskRemote() is expensive because it acquires locks and calls
 * into the kernel to write to file descriptors to activate fibers.  Rather than
 * incurring this cost on every coro -> fiber hop, pre-spawn fibers and use a
 * user-space submission queue to trigger work on fibers.
 */
class CoroFiberAdapter {
 public:
  struct Config {
    std::string threadName{"coro_fiber_adapter"};
    size_t numThreads{32};
    size_t fibersPerThread{4};
    uint32_t stackSize{32 * 1024};

    const Config& validate() const;
  };

  /**
   * Constructor - sets up the queues and launches fibers on NavyThreads
   * @param config configuration for the adapter
   */
  explicit CoroFiberAdapter(const Config& config);
  ~CoroFiberAdapter();

  CoroFiberAdapter(const CoroFiberAdapter&) = delete;
  CoroFiberAdapter& operator=(const CoroFiberAdapter&) = delete;
  CoroFiberAdapter(CoroFiberAdapter&&) = delete;
  CoroFiberAdapter& operator=(CoroFiberAdapter&&) = delete;

  /**
   * Run a (potentially async) function on a fiber on a Navy worker thread and
   * block until the operation completes. Returns the output from the function.
   *
   * Your function should return a folly::Expected<ValueT, navy::Status> --
   * onWorkerThread() will retry up to kMaxAttempts times if it returns a
   * navy::Status::Retry error, using exponential backoff with jitter between
   * attempts. If all attempts return Retry, the final Retry error is returned
   * to the caller. Note that the return value from the fiber *must* be movable.
   * If your type isn't movable, wrap it in a unique_ptr.
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
      FuncT&& func, CleanupFuncT&& cleanup = detail::DefaultCleanupT{}) {
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
    auto cancellationToken =
        co_await folly::coro::co_current_cancellation_token;
    folly::Func task = [func = std::forward<FuncT>(func),
                        promise = std::move(promiseFuturePair.first),
                        cleanupHelper =
                            CleanupHelper(std::forward<CleanupFuncT>(cleanup)),
                        token = cancellationToken]() mutable {
      static constexpr size_t kMaxAttempts = 3;
      static constexpr std::chrono::milliseconds kBaseDelay{10};

      // setting error is required to kick off the first loop
      cleanupHelper.result_ = folly::makeUnexpected(navy::Status::Retry);
      for (size_t attempt = 0;
           attempt < kMaxAttempts && cleanupHelper.result_.hasError() &&
           cleanupHelper.result_.error() == navy::Status::Retry &&
           !token.isCancellationRequested();
           ++attempt) {
        if (attempt > 0) {
          auto baseDelay = kBaseDelay * (1u << (attempt - 1));
          auto jitter = std::chrono::milliseconds(
              folly::Random::rand32(kBaseDelay.count()));
          folly::futures::sleep(baseDelay + jitter).wait();
        }

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
    };
    getTaskQueue().enqueue(std::move(task));

    auto retVal = co_await std::move(promiseFuturePair.second);
    retVal.consumed_ = true;
    co_return std::move(retVal.result_);
  }

  /**
   * Drain all work from the task queue and shut down fibers.  Note: no more
   * tasks will be executed by the executor!
   */
  void drain();

 private:
  struct ValidConstructorTag {};
  CoroFiberAdapter(ValidConstructorTag, const Config& config);

  struct WorkerContext {
    WorkerContext(folly::StringPiece threadName,
                  navy::NavyThread::Options options);

    folly::coro::UnboundedQueue<folly::Func> tasks_;
    navy::NavyThread thread_;
  };

  size_t fibersPerThread_;
  std::vector<std::unique_ptr<WorkerContext>> workers_;

  // Returns task queues in a round-robin fashion
  folly::coro::UnboundedQueue<folly::Func>& getTaskQueue();
};

} // namespace facebook::cachelib::interface::utils
