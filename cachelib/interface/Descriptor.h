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

#include <folly/CPortability.h>
#include <folly/logging/xlog.h>

#include <concepts>
#include <cstdint>
#include <utility>
#include <variant>

#include "cachelib/interface/DetachedItem.h"
#include "cachelib/interface/Handle.h"

namespace facebook::cachelib::interface {

/**
 * Source side of a transfer: hands a cache item's bytes to a connector, the way
 * a Handle hands them to a user. Move-only, like the handle it owns.
 *
 * The bytes come either from a cache-resident item -- kept alive by the
 * ReadHandle this owns -- or from a DetachedItem holding a detached copy, which
 * is all eviction paths for some components can produce.
 */
class ReadDescriptor {
 public:
  explicit ReadDescriptor(ReadHandle&& handle) noexcept
      : owner_(std::move(handle)) {}

  explicit ReadDescriptor(DetachedItem&& item) noexcept
      : owner_(std::move(item)) {}

  ~ReadDescriptor() noexcept = default;

  // Leaves `other` empty rather than holding a moved-from alternative: a
  // moved-from DetachedItem is indistinguishable from a legitimately empty
  // value, so emptiness needs a state of its own.
  ReadDescriptor(ReadDescriptor&& other) noexcept
      : owner_(std::move(other.owner_)) {
    other.owner_.emplace<Empty>();
  }
  ReadDescriptor(const ReadDescriptor&) = delete;
  ReadDescriptor& operator=(ReadDescriptor&&) = delete;
  ReadDescriptor& operator=(const ReadDescriptor&) = delete;

  // False once moved from or released, and also for a handle alternative whose
  // handle is itself empty. The accessors below are valid only when this is
  // true.
  FOLLY_ALWAYS_INLINE explicit operator bool() const noexcept {
    if (const auto* handle = std::get_if<ReadHandle>(&owner_)) {
      return static_cast<bool>(*handle);
    }
    return std::holds_alternative<DetachedItem>(owner_);
  }

 private:
  // Applies `fn` to whichever alternative is live. CacheItem and DetachedItem
  // accessors match in name and return type, so a generic lambda binds to
  // either.
  template <typename Fn>
  FOLLY_ALWAYS_INLINE auto visitItem(Fn&& fn) const noexcept {
    checkNotEmpty();
    const auto* handle = std::get_if<ReadHandle>(&owner_);
    return handle ? fn(**handle) : fn(std::get<DetachedItem>(owner_));
  }

 public:
  FOLLY_ALWAYS_INLINE const void* data() const noexcept {
    return visitItem([](const auto& item) { return item.getMemory(); });
  }

  // The value size the item was allocated for, not the allocation-class size
  // the slab allocator rounded that up to.
  FOLLY_ALWAYS_INLINE uint32_t size() const noexcept {
    return visitItem([](const auto& item) { return item.getMemorySize(); });
  }

  FOLLY_ALWAYS_INLINE Key key() const noexcept {
    return visitItem([](const auto& item) { return item.getKey(); });
  }

  FOLLY_ALWAYS_INLINE uint32_t creationTime() const noexcept {
    return visitItem([](const auto& item) { return item.getCreationTime(); });
  }

  FOLLY_ALWAYS_INLINE uint32_t expiryTime() const noexcept {
    return visitItem([](const auto& item) { return item.getExpiryTime(); });
  }

  FOLLY_ALWAYS_INLINE bool isHandleBacked() const noexcept {
    return std::holds_alternative<ReadHandle>(owner_);
  }

  // For component.remove(std::move(descriptor).release()).
  ReadHandle release() && noexcept {
    XDCHECK(isHandleBacked());
    auto handle = std::move(std::get<ReadHandle>(owner_));
    owner_.emplace<Empty>();
    return handle;
  }

 private:
  // Moved-from or released. Distinct from an item with a zero-size value.
  using Empty = std::monostate;

  // Accessing an empty descriptor is a use-after-move, so fail with a message
  // rather than dereferencing a null item or tripping a bare
  // std::bad_variant_access inside a noexcept accessor.
  FOLLY_ALWAYS_INLINE void checkNotEmpty() const noexcept {
    XDCHECK(static_cast<bool>(*this)) << "ReadDescriptor accessed while empty";
  }

  std::variant<Empty, ReadHandle, DetachedItem> owner_;
};

/**
 * Destination side of an in-place update: wraps the WriteHandle from
 * findToWrite(). The item is already inserted, so there is nothing to hand
 * back -- ~WriteHandle() flushes once mutableData() has marked it dirty.
 */
class WriteDescriptor {
 public:
  // same_as constrains the *deduced* type. A plain WriteHandle&& would accept
  // an AllocatedHandle rvalue -- it IS-A WriteHandle and adds no members --
  // yielding a WriteHandle with inserted_ == false, which ~WriteHandle() writes
  // back while release() charges the same bytes as a hole. It also rejects
  // lvalues, preserving explicit ownership transfer.
  template <std::same_as<WriteHandle> H>
  explicit WriteDescriptor(H&& handle) noexcept
      : handle_(std::forward<H>(handle)) {}

  ~WriteDescriptor() noexcept = default;

  WriteDescriptor(WriteDescriptor&&) noexcept = default;
  WriteDescriptor(const WriteDescriptor&) = delete;
  WriteDescriptor& operator=(WriteDescriptor&&) = delete;
  WriteDescriptor& operator=(const WriteDescriptor&) = delete;

  FOLLY_ALWAYS_INLINE explicit operator bool() const noexcept {
    return static_cast<bool>(handle_);
  }

  // Reading does not mark the handle dirty; mutableData() does.
  FOLLY_ALWAYS_INLINE const void* data() const noexcept {
    checkNotEmpty();
    return handle_->getMemory();
  }

  // Marks the handle dirty so ~WriteHandle() flushes the write.
  FOLLY_ALWAYS_INLINE void* mutableData() noexcept {
    checkNotEmpty();
    handle_.markDirty();
    return handle_->getMemory();
  }

  FOLLY_ALWAYS_INLINE uint32_t size() const noexcept {
    checkNotEmpty();
    return handle_->getMemorySize();
  }

  // Destination-side spelling of size(); an in-place update writes exactly the
  // bytes already there.
  FOLLY_ALWAYS_INLINE uint32_t capacity() const noexcept {
    checkNotEmpty();
    return handle_->getMemorySize();
  }

 private:
  // Moving leaves the handle empty, so its operator-> would dereference null.
  FOLLY_ALWAYS_INLINE void checkNotEmpty() const noexcept {
    XDCHECK(static_cast<bool>(handle_))
        << "WriteDescriptor accessed while empty";
  }

  WriteHandle handle_;
};

/**
 * Destination side of a transfer for an item that is allocated but not yet in
 * cache. Destroying the descriptor *discards* the allocation, so a completed
 * transfer must release() the handle and insert it.
 */
class AllocatedDescriptor {
 public:
  explicit AllocatedDescriptor(AllocatedHandle&& handle) noexcept
      : handle_(std::move(handle)) {}

  ~AllocatedDescriptor() noexcept = default;

  AllocatedDescriptor(AllocatedDescriptor&&) noexcept = default;
  AllocatedDescriptor(const AllocatedDescriptor&) = delete;
  AllocatedDescriptor& operator=(AllocatedDescriptor&&) = delete;
  AllocatedDescriptor& operator=(const AllocatedDescriptor&) = delete;

  FOLLY_ALWAYS_INLINE explicit operator bool() const noexcept {
    return static_cast<bool>(handle_);
  }

  // Must NOT mark the handle dirty, unlike WriteDescriptor: the item is not
  // inserted, so a writeback would index bytes that release() then charges as
  // a hole. Insertion goes through release().
  FOLLY_ALWAYS_INLINE void* mutableData() noexcept {
    checkNotEmpty();
    return handle_->getMemory();
  }

  // Bytes available to write; connectors define whether this must match the
  // source size.
  FOLLY_ALWAYS_INLINE uint32_t capacity() const noexcept {
    checkNotEmpty();
    return handle_->getMemorySize();
  }

  // For component.insert(std::move(descriptor).release()).
  AllocatedHandle release() && noexcept { return std::move(handle_); }

 private:
  // Moving or releasing leaves the handle empty, so its operator-> would
  // dereference null.
  FOLLY_ALWAYS_INLINE void checkNotEmpty() const noexcept {
    XDCHECK(static_cast<bool>(handle_))
        << "AllocatedDescriptor accessed while empty";
  }

  AllocatedHandle handle_;
};

} // namespace facebook::cachelib::interface
