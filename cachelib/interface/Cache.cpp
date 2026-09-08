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

#include "cachelib/interface/Cache.h"

#include <folly/logging/xlog.h>

#include <utility>

namespace facebook::cachelib::interface {

Cache::Cache(std::unique_ptr<CacheComponent> component) noexcept
    : component_(std::move(component)) {
  XCHECK(component_) << "Cache requires a non-null component";
}

folly::coro::Task<Result<AllocatedHandle>> Cache::allocate(
    Key key, uint32_t valueSize, uint32_t creationTime, uint32_t ttlSecs) {
  auto result =
      co_await component_->allocate(key, valueSize, creationTime, ttlSecs);
  co_return std::move(result).then([](AllocatedDescriptor descriptor) {
    return std::move(descriptor).release();
  });
}

folly::coro::Task<UnitResult> Cache::insert(AllocatedHandle&& handle) {
  return component_->insert(std::move(handle));
}

folly::coro::Task<Result<std::optional<AllocatedHandle>>>
Cache::insertOrReplace(AllocatedHandle&& handle) {
  return component_->insertOrReplace(std::move(handle));
}

folly::coro::Task<Result<bool>> Cache::exists(Key key) {
  auto result = co_await component_->find(key);
  if (result.hasError()) {
    co_return folly::makeUnexpected(std::move(result).error());
  }
  co_return result->has_value();
}

folly::coro::Task<Result<std::optional<ReadHandle>>> Cache::find(Key key) {
  auto result = co_await component_->find(key);
  if (result.hasError()) {
    co_return folly::makeUnexpected(std::move(result).error());
  }
  if (!result->has_value()) {
    co_return std::nullopt;
  }
  co_return std::move(result).value().value().release();
}

folly::coro::Task<Result<std::optional<WriteHandle>>> Cache::findToWrite(
    Key key) {
  auto result = co_await component_->findToWrite(key);
  if (result.hasError()) {
    co_return folly::makeUnexpected(std::move(result).error());
  }
  if (!result->has_value()) {
    co_return std::nullopt;
  }
  co_return std::move(result).value().value().release();
}

folly::coro::AsyncGenerator<ReadHandle> Cache::iterator() {
  auto iterator = component_->iterator();
  while (auto item = co_await iterator.next()) {
    co_yield std::move(item).value().release();
  }
}

folly::coro::Task<Result<bool>> Cache::remove(Key key) {
  return component_->remove(key);
}

folly::coro::Task<UnitResult> Cache::remove(ReadHandle&& handle) {
  return component_->remove(std::move(handle));
}

UnitResult Cache::shutdown() { return component_->shutdown(); }

} // namespace facebook::cachelib::interface
