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

#include <sanitizer/asan_interface.h>

#include <vector>

#include "cachelib/allocator/datastruct/SList.h"

namespace facebook::cachelib {

/**
 * A non-intrusive drop-in replacement for SList that stores allocation
 * pointers in an external vector instead of using intrusive hooks embedded
 * in the freed memory. This allows the freed memory to be fully
 * ASAN-poisoned, catching any stale writes to allocations on the free list.
 *
 * When allocSize > 0 and ASAN is enabled:
 *   - insert() poisons the allocation memory after storing the pointer
 *   - pop() unpoisons the allocation memory before removing the pointer
 *   - saveState() temporarily unpoisons to build an in-memory SList that
 *     persists in shared memory
 *   - the restore constructor reconstructs the external list from the in-memory
 *     SList and poisons each allocation
 *
 * When allocSize == 0 (the default), poisoning is a no-op. This is useful
 * for temporary lists (e.g., in pruneFreeAllocs) that shuffle already-
 * poisoned allocations between containers without changing their ASAN state.
 */
template <typename T, SListHook<T> T::* HookPtr>
class AsanSList {
 public:
  using CompressedPtrType = typename T::CompressedPtrType;
  using PtrCompressor = typename T::PtrCompressor;
  using SListObject = serialization::SListObject;

  // Movable but not copyable
  AsanSList(const AsanSList&) = delete;
  AsanSList& operator=(const AsanSList&) = delete;
  AsanSList(AsanSList&& rhs) noexcept
      : compressor_(rhs.compressor_),
        allocSize_(rhs.allocSize_),
        entries_(std::move(rhs.entries_)) {}
  AsanSList& operator=(AsanSList&& rhs) noexcept {
    if (&rhs != this) {
      // PtrCompressor doesn't support move assignment, so we mirror SList's
      // approach: assert compressors match and only move the data fields.
      assert(compressor_ == rhs.compressor_);
      // Note: don't copy allocSize_ so we don't change ASAN poisoning semantics
      // of either list (allocSize_ of 0 disables poisoning)
      entries_ = std::move(rhs.entries_);
    }
    return *this;
  }

  explicit AsanSList(PtrCompressor compressor, uint32_t allocSize = 0) noexcept
      : compressor_(std::move(compressor)), allocSize_(allocSize) {}

  AsanSList(const SListObject& object,
            PtrCompressor compressor,
            uint32_t allocSize = 0)
      : compressor_(std::move(compressor)), allocSize_(allocSize) {
    // Restore our vector by walking the in-memory chain
    SList<T, HookPtr> tempList(object, compressor_);
    entries_.reserve(tempList.size());
    while (!tempList.empty()) {
      void* mem = reinterpret_cast<void*>(tempList.getHead());
      tempList.pop();
      entries_.push_back(mem);
      ASAN_POISON_MEMORY_REGION(mem, allocSize_);
    }
  }

  ~AsanSList() = default;

  /**
   * Adds a node to the list. If allocSize > 0, poisons the allocation memory.
   */
  void insert(T& node) {
    entries_.push_back(reinterpret_cast<void*>(&node));
    ASAN_POISON_MEMORY_REGION(&node, allocSize_);
  }

  /**
   * Removes the most recently inserted node. If allocSize > 0, unpoisons
   * the allocation memory.
   *
   * @throw std::logic_error if called on empty list.
   */
  void pop() {
    if (empty()) {
      throw std::logic_error("Attempting to pop an empty list");
    }
    void* mem = entries_.back();
    entries_.pop_back();
    ASAN_UNPOISON_MEMORY_REGION(mem, allocSize_);
  }

  /**
   * Returns a pointer to the most recently inserted node, or nullptr if empty.
   */
  T* getHead() const noexcept {
    return empty() ? nullptr : reinterpret_cast<T*>(entries_.back());
  }

  bool empty() const noexcept { return entries_.empty(); }

  size_t size() const noexcept { return entries_.size(); }

  bool operator==(const AsanSList& other) const noexcept {
    return entries_ == other.entries_;
  }

  /**
   * Transfer all elements from 'other' to this list. Executes in linear time
   * proportional to the size of 'other'. ASAN poison state of the underlying
   * allocations is preserved (only pointers are copied, memory is untouched).
   */
  void splice(AsanSList&& other) {
    if (other.empty()) {
      return;
    }
    entries_.insert(entries_.end(), other.entries_.begin(),
                    other.entries_.end());
    other.entries_.clear();
  }

  /**
   * Exports the current state as a thrift SListObject for serialization.
   * Temporarily unpoisons allocations to construct an SList with intrusive
   * hooks, then serializes it. Does not re-poison afterwards since
   * saveState() is only called during shutdown.
   */
  SListObject saveState() const {
    SList<T, HookPtr> tempList{compressor_};
    // Iterate in reverse: SList::insert prepends at head, so reverse iteration
    // produces head→tail order matching entries_[0..n]. The restore constructor
    // pops from head, reproducing the original entries_ order.
    for (auto it = entries_.rbegin(); it != entries_.rend(); ++it) {
      ASAN_UNPOISON_MEMORY_REGION(*it, allocSize_);
      tempList.insert(*reinterpret_cast<T*>(*it));
    }
    return tempList.saveState();
  }

 private:
  PtrCompressor compressor_;
  uint32_t allocSize_{0};
  std::vector<void*> entries_;
};

} // namespace facebook::cachelib
