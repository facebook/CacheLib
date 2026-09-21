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

#include "cachelib/interface/TransferExecutor.h"

#include <folly/coro/ViaIfAsync.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/logging/xlog.h>

#include <memory>
#include <stdexcept>
#include <utility>

namespace facebook::cachelib::interface {

namespace {

size_t validateNumThreads(size_t numThreads) {
  if (numThreads == 0) {
    throw std::invalid_argument("numThreads must be > 0");
  }
  return numThreads;
}

} // namespace

TransferExecutor::TransferExecutor(size_t numThreads)
    : executor_(validateNumThreads(numThreads),
                std::make_shared<folly::NamedThreadFactory>("transfer")) {}

TransferExecutor::~TransferExecutor() { closeAndDrain(); }

UnitResult TransferExecutor::submit(DetachedItem item,
                                    CacheComponent& destination,
                                    Connector& connector) noexcept {
  std::shared_lock lock{admissionMutex_};
  if (!accepting_) {
    return makeError(Error::Code::SHUTDOWN_FAILED,
                     "transfer executor is shutting down");
  }

  scope_.add(folly::coro::co_withExecutor(
      &executor_, transferItem(std::move(item), destination, connector)));
  return folly::unit;
}

folly::coro::Task<void> TransferExecutor::transferItem(
    DetachedItem item, CacheComponent& destination, Connector& connector) {
  const auto key = item.getKey().str();
  const auto creationTime = item.getCreationTime();
  const auto expiryTime = item.getExpiryTime();
  if (expiryTime != 0 && expiryTime <= creationTime) {
    XLOG_EVERY_MS(WARN, 1000)
        << "Dropping item " << key << " with invalid expiry time " << expiryTime
        << " and creation time " << creationTime;
    co_return;
  }
  const auto ttlSecs = expiryTime == 0 ? 0 : expiryTime - creationTime;
  const auto size = item.getMemorySize();

  auto allocated =
      co_await destination.allocate(key, size, creationTime, ttlSecs);
  if (allocated.hasError()) {
    XLOG_EVERY_MS(WARN, 1000) << "Failed to allocate destination for item "
                              << key << ": " << allocated.error();
    co_return;
  }

  auto moved = co_await connector.transfer(ReadDescriptor(std::move(item)),
                                           std::move(allocated).value());
  if (moved.hasError()) {
    XLOG_EVERY_MS(WARN, 1000)
        << "Failed to transfer item " << key << ": " << moved.error();
    co_return;
  }

  auto inserted =
      co_await destination.insertOrReplace(std::move(moved).value().release());
  if (inserted.hasError()) {
    XLOG_EVERY_MS(WARN, 1000) << "Failed to insert transferred item " << key
                              << ": " << inserted.error();
  }
}

void TransferExecutor::closeAndDrain() noexcept {
  {
    std::unique_lock lock{admissionMutex_};
    if (!accepting_) {
      return;
    }
    accepting_ = false;
  }

  try {
    scope_.cleanup().get();
    executor_.join();
  } catch (const std::exception& e) {
    XLOG(ERR) << "Failed to drain transfer executor: " << e.what();
  }
}

} // namespace facebook::cachelib::interface
