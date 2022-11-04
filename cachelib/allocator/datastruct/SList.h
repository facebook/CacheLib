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

#include <folly/logging/xlog.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"
#pragma GCC diagnostic pop

#include "cachelib/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {

/**
 * Hook pointing to the next node in the list
 */
template <typename T>
struct CACHELIB_PACKED_ATTR SListHook {
 public:
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;

  T* getNext(const PtrCompressor& compressor) const noexcept {
    return compressor.unCompress(next_);
  }

  void setNext(const T* node, const PtrCompressor& compressor) noexcept {
    next_ = compressor.compress(node);
    assert(getNext(compressor) == node);
  }

 private:
  CompressedPtr next_{};
};

/**
 * Intrusive singly-linked list that allows executing all operations in
 * constant time i.e. O(1). However, this is not thread-safe and does not
 * prevent users from forming cycles.
 */
template <typename T, SListHook<T> T::*HookPtr>
class SList {
 public:
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;
  using SListObject = serialization::SListObject;

  // Movable but not copyable
  SList(const SList&) = delete;
  SList& operator=(const SList&) = delete;
  SList(SList&& rhs) noexcept = default;

  // SList cannot be move assigned when their compressor are not the same, since
  // PtrCompressor doesn't support move assignment.
  SList& operator=(SList&& rhs) noexcept {
    assert(compressor_ == rhs.compressor_);
    size_ = rhs.size_;
    head_ = rhs.head_;
    tail_ = rhs.tail_;
    rhs.size_ = 0;
    rhs.head_ = nullptr;
    rhs.tail_ = nullptr;
    return *this;
  }

  explicit SList(PtrCompressor compressor) noexcept
      : compressor_(std::move(compressor)) {}

  explicit SList(const SListObject& object, PtrCompressor compressor)
      : compressor_(std::move(compressor)),
        size_(*object.size()),
        head_(compressor_.unCompress(CompressedPtr{*object.compressedHead()})) {
    // TODO(bwatling): eventually we'll always have 'compressedTail' and we can
    // remove the loop below.
    if (*object.compressedTail() >= 0) {
      tail_ = compressor_.unCompress(CompressedPtr{*object.compressedTail()});
    } else if (head_) {
      tail_ = head_;
      while (T* next = getNext(*tail_)) {
        tail_ = next;
      }
    }
    // do some sanity checks.
    checkStateOrThrow();
  }

  /**
   * Exports the current state as a thrift object for later restoration.
   */
  SListObject saveState() const {
    SListObject state;
    *state.compressedHead() = compressor_.compress(head_).saveState();
    *state.compressedTail() = compressor_.compress(tail_).saveState();
    *state.size() = size_;
    return state;
  }

  /**
   * Adds a node to the head of the list.
   */
  void insert(T& node) noexcept;

  /**
   * Pops the node at the head of the list.
   *
   * @throw std::logic_error if called on empty list.
   */
  void pop();

  /**
   * Returns a pointer to the head of the list.
   */
  T* getHead() const noexcept { return empty() ? nullptr : head_; }

  bool empty() const noexcept { return size_ == 0; }

  size_t size() const noexcept { return size_; }

  /*
   * Two lists are considered equal if they are both empty or point to the
   * same head node.
   */
  bool operator==(const SList& other) const noexcept {
    if (this->head_ == other.getHead()) {
      assert(this->size() == other.size());
      assert(this->tail_ == other.tail_);
      return true;
    }
    return false;
  }

  class Iterator {
   public:
    Iterator(T* curr, const SList& slist, T* prev = nullptr) noexcept
        : curr_(curr), prev_(prev), slist_(&slist) {}

    // Copyable and movable
    Iterator(const Iterator&) = default;
    Iterator& operator=(const Iterator&) = default;
    Iterator(Iterator&&) noexcept = default;
    Iterator& operator=(Iterator&&) noexcept = default;

    Iterator& operator++() noexcept;

    // Returns a pointer to the previous element
    T* previous() const noexcept { return prev_; }

    // Returns a pointer to the current element
    T* operator->() const noexcept { return curr_; }

    T& operator*() const noexcept { return *curr_; }

    // returns true if the iterator belongs to the SList passed in.
    bool belongsToList(const SList* list) const noexcept {
      return slist_ == list;
    }

    bool operator==(const Iterator& other) const noexcept {
      // With an acyclic list, there is no need to check prev_ after checking
      // slist_ and curr_. This also lets us avoid computing prev_ for the
      // end() iterator.
      return slist_ == other.slist_ && curr_ == other.curr_;
    }

    bool operator!=(const Iterator& other) const noexcept {
      return !(*this == other);
    }

   private:
    void goForward() noexcept;

    // current node the iterator is pointing to
    T* curr_{nullptr};

    // the previous node for the current. This is used to do deletions in O(1)
    T* prev_{nullptr};

    // the list we are iterating.
    const SList* slist_{nullptr};
  };

  Iterator begin() const noexcept;

  Iterator end() const noexcept;

  /**
   * Removes the element that the iterator points to and returns an iterator to
   * the next element and returns a new iterator by advancing.
   *
   * @throw std::logic_error if the end() iterator is passed in or the
   *        iterator does not belong to this list.
   */
  Iterator remove(const Iterator& it);

  /**
   * Transfer all elements from 'other' to the start of this SList. Executes in
   * constant time.
   */
  void splice(SList&& other);

 private:
  T* getNext(const T& node) const noexcept {
    return (node.*HookPtr).getNext(compressor_);
  }

  void setNext(T& node, T* next) const noexcept {
    (node.*HookPtr).setNext(next, compressor_);
  }

  void checkStateOrThrow() const {
    if ((size_ == 0 && head_ != nullptr) || (head_ == nullptr && size_ != 0)) {
      throw std::invalid_argument("Invalid state. Corrupt head");
    }
    if (!head_ != !tail_) {
      throw std::invalid_argument("Invalid state. Corrupt tail");
    }
  }

  PtrCompressor compressor_;

  // Size of the list
  size_t size_{0};

  // First and last element in the list
  T* head_{nullptr};
  T* tail_{nullptr};
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/datastruct/SList-inl.h"
