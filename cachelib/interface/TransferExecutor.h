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

#include <folly/coro/AsyncScope.h>
#include <folly/coro/Task.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <cstddef>
#include <mutex>
#include <shared_mutex>

#include "cachelib/interface/CacheComponent.h"
#include "cachelib/interface/Connector.h"
#include "cachelib/interface/DetachedItem.h"
#include "cachelib/interface/Result.h"

namespace facebook::cachelib::interface {

/**
 * Asynchronously transfer items between CacheComponents for operations such as
 * eviction and promotion.
 */
class TransferExecutor {
 public:
  explicit TransferExecutor(size_t numThreads = 1);
  ~TransferExecutor();

  TransferExecutor(const TransferExecutor&) = delete;
  TransferExecutor& operator=(const TransferExecutor&) = delete;
  TransferExecutor(TransferExecutor&&) = delete;
  TransferExecutor& operator=(TransferExecutor&&) = delete;

  /**
   * Schedule an item transfer to the destination through the connector.
   * The destination and connector must remain alive until closeAndDrain()
   * returns.
   *
   * @return success if the transfer was accepted for asynchronous execution,
   * or SHUTDOWN_FAILED if shutdown has begun
   */
  UnitResult submit(DetachedItem item,
                    CacheComponent& destination,
                    Connector& connector) noexcept;

  /**
   * Stop accepting work. The first caller waits for every accepted transfer to
   * finish. Concurrent callers return once shutdown has begun.
   */
  void closeAndDrain() noexcept;

 private:
  static folly::coro::Task<void> transferItem(DetachedItem item,
                                              CacheComponent& destination,
                                              Connector& connector);

  // Coordinates admission with shutdown without serializing submissions.
  std::shared_mutex admissionMutex_;
  bool accepting_{true};
  folly::CPUThreadPoolExecutor executor_;
  folly::coro::AsyncScope scope_;
};

} // namespace facebook::cachelib::interface
