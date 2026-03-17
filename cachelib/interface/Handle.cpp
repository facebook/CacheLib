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

#include <folly/logging/xlog.h>

#include <cstring>

#include "cachelib/interface/CacheComponent.h"

namespace facebook::cachelib::interface {

Handle::Handle(CacheComponent& cache, CacheItem& item, bool inserted) noexcept
    : cache_(&cache), item_(&item), inserted_(inserted) {
  item_->incrementRefCount();
}

Handle::Handle(CacheComponent& cache, bool inserted, InlineItemTag) noexcept
    : cache_(&cache),
      item_(reinterpret_cast<CacheItem*>(buf_)),
      inserted_(inserted) {}

Handle::Handle(Handle&& other) noexcept
    : cache_(other.cache_), inserted_(other.inserted_) {
  XDCHECK_NE(cache_, nullptr) << "Invalid handle";
  if (other.item_ == reinterpret_cast<CacheItem*>(other.buf_)) {
    std::memcpy(buf_, other.buf_, kInlineBufSize);
    item_ = reinterpret_cast<CacheItem*>(buf_);
  } else {
    item_ = other.item_;
  }
  other.item_ = nullptr;
}

Handle::~Handle() noexcept {
  if (item_ != nullptr) {
    if (item_->decrementRefCount()) {
      cache_->release(*item_, inserted_);
    }
    item_ = nullptr;
  }
}

WriteHandle::WriteHandle(CacheComponent& cache, CacheItem& item) noexcept
    : Handle(cache, item, /* inserted */ true) {}

WriteHandle::WriteHandle(CacheComponent& cache, InlineItemTag) noexcept
    : Handle(cache, /* inserted */ true, InlineItem) {}

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

WriteHandle::WriteHandle(CacheComponent& cache,
                         bool inserted,
                         InlineItemTag) noexcept
    : Handle(cache, inserted, InlineItem) {}

AllocatedHandle::AllocatedHandle(CacheComponent& cache,
                                 CacheItem& item) noexcept
    : WriteHandle(cache, item, /* inserted */ false) {}

AllocatedHandle::AllocatedHandle(CacheComponent& cache, InlineItemTag) noexcept
    : WriteHandle(cache, /* inserted */ false, InlineItem) {}

ReadHandle::ReadHandle(CacheComponent& cache, CacheItem& item) noexcept
    : Handle(cache, item, /* inserted */ true) {}

ReadHandle::ReadHandle(CacheComponent& cache, InlineItemTag) noexcept
    : Handle(cache, /* inserted */ true, InlineItem) {}

} // namespace facebook::cachelib::interface
