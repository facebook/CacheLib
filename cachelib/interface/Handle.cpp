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

#include "cachelib/interface/Handle.h"

#include <folly/coro/BlockingWait.h>
#include <folly/logging/xlog.h>

#include "cachelib/interface/CacheComponent.h"

namespace facebook::cachelib::interface {

Handle::Handle(CacheComponent& cache, CacheItem& item, bool inserted) noexcept
    : cache_(&cache), item_(&item), inserted_(inserted) {
  item_->incrementRefCount();
}

Handle::Handle(Handle&& other) noexcept
    : cache_(other.cache_), item_(other.item_), inserted_(other.inserted_) {
  XDCHECK_NE(cache_, nullptr) << "Invalid handle";
  other.item_ = nullptr;
}

Handle::~Handle() noexcept {
  if (item_ != nullptr) {
    if (item_->decrementRefCount()) {
      folly::coro::blockingWait(cache_->release(*item_, inserted_));
    }
    item_ = nullptr;
  }
}

WriteHandle::WriteHandle(CacheComponent& cache, CacheItem& item) noexcept
    : Handle(cache, item, /* inserted */ true) {}

WriteHandle::WriteHandle(WriteHandle&& other) noexcept
    : Handle(std::move(other)),
      // NOLINTNEXTLINE(bugprone-use-after-move)
      dirty_(std::exchange(other.dirty_, false)) {}

WriteHandle::~WriteHandle() noexcept {
  if (item_ != nullptr && dirty_) {
    cache_->writeBack(*item_);
  }
}

WriteHandle::WriteHandle(CacheComponent& cache,
                         CacheItem& item,
                         bool inserted) noexcept
    : Handle(cache, item, inserted) {}

AllocatedHandle::AllocatedHandle(CacheComponent& cache,
                                 CacheItem& item) noexcept
    : WriteHandle(cache, item, /* inserted */ false) {}

ReadHandle::ReadHandle(CacheComponent& cache, CacheItem& item) noexcept
    : Handle(cache, item, /* inserted */ true) {}

} // namespace facebook::cachelib::interface
