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

#include "cachelib/interface/utils/CoroFiberAdapter.h"

namespace facebook::cachelib::interface::utils {
namespace {
void fiberRun(folly::coro::UnboundedQueue<folly::Func>& queue) {
  while (auto task = queue.dequeue().semi().get()) {
    task();
  }
}
} // namespace

const CoroFiberAdapter::Config& CoroFiberAdapter::Config::validate() const {
  if (threadName.empty()) {
    throw std::invalid_argument("threadName must be non-empty");
  }
  auto check = [](auto val, const char* field) {
    if (val <= 0) {
      throw std::invalid_argument(fmt::format("{} must be > 0", field));
    }
  };
  check(numThreads, "numThreads");
  check(fibersPerThread, "fibersPerThread");
  check(stackSize, "stackSize");
  return *this;
}

CoroFiberAdapter::CoroFiberAdapter(const Config& config)
    : CoroFiberAdapter(ValidConstructorTag{}, config.validate()) {}

CoroFiberAdapter::CoroFiberAdapter(ValidConstructorTag, const Config& config)
    : fibersPerThread_(config.fibersPerThread) {
  for (size_t i = 0; i < config.numThreads; ++i) {
    XLOG(INFO) << "Starting coro<->fiber adapter thread " << i << " with "
               << fibersPerThread_ << " fibers";
    auto& ctx = workers_.emplace_back(std::make_unique<WorkerContext>(
        fmt::format("{}_{}", config.threadName, i),
        navy::NavyThread::Options(config.stackSize)));
    for (size_t j = 0; j < fibersPerThread_; ++j) {
      ctx->thread_.addTaskRemote([&ctx = *ctx]() { fiberRun(ctx.tasks_); });
    }
  }
}

CoroFiberAdapter::~CoroFiberAdapter() { drain(); }

void CoroFiberAdapter::drain() {
  // Enqueue empty functions to signal fibers to exit
  for (auto& ctx : workers_) {
    for (size_t i = 0; i < fibersPerThread_; ++i) {
      ctx->tasks_.enqueue(folly::Func{});
    }
  }

  for (auto& ctx : workers_) {
    ctx->thread_.drain();
  }
}

CoroFiberAdapter::WorkerContext::WorkerContext(
    folly::StringPiece threadName, navy::NavyThread::Options options)
    : thread_(threadName, options) {}

folly::coro::UnboundedQueue<folly::Func>& CoroFiberAdapter::getTaskQueue() {
  thread_local size_t counter = 0;
  return workers_[counter++ % workers_.size()]->tasks_;
}

} // namespace facebook::cachelib::interface::utils
