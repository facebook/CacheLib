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

namespace facebook {
namespace cachelib {

template <typename T, SListHook<T> T::*HookPtr>
void SList<T, HookPtr>::insert(T& node) noexcept {
  XDCHECK_NE(reinterpret_cast<uintptr_t>(head_),
             reinterpret_cast<uintptr_t>(&node));
  XDCHECK_NE(reinterpret_cast<uintptr_t>(tail_),
             reinterpret_cast<uintptr_t>(&node));
  XDCHECK_EQ(size_ == 0, head_ == nullptr);
  XDCHECK_EQ(size_ == 0, tail_ == nullptr);

  // Set next to the current head
  setNext(node, head_);

  // Make this the new head
  head_ = &node;
  size_++;
  if (!tail_) {
    tail_ = head_;
  }
}

template <typename T, SListHook<T> T::*HookPtr>
void SList<T, HookPtr>::pop() {
  if (empty()) {
    throw std::logic_error("Attempting to pop an empty list");
  }

  // Store a reference to the current head
  T& node = *head_;

  // Set new head
  head_ = getNext(node);
  size_--;
  if (!head_) {
    tail_ = nullptr;
  }

  XDCHECK(size_ != 0 || head_ == nullptr);
  checkStateOrThrow();

  // reset the pointer for old node.
  setNext(node, nullptr);
}

template <typename T, SListHook<T> T::*HookPtr>
void SList<T, HookPtr>::splice(SList<T, HookPtr>&& other) {
  if (other.empty()) {
    return;
  }
  XDCHECK_NE(reinterpret_cast<uintptr_t>(head_),
             reinterpret_cast<uintptr_t>(other.head_));
  XDCHECK_EQ(size_ == 0, head_ == nullptr);
  XDCHECK_EQ(size_ == 0, tail_ == nullptr);
  XDCHECK(other.head_);
  XDCHECK_NE(other.size_, 0u);
  XDCHECK(other.tail_);

  // Link in 'other' at head_ in constant time.
  setNext(*other.tail_, head_);
  head_ = other.head_;
  size_ += other.size();
  if (!tail_) {
    tail_ = other.tail_;
  }

  // Leave 'other' empty.
  other.head_ = nullptr;
  other.size_ = 0;
  other.tail_ = nullptr;
  XDCHECK(other.empty());
}

template <typename T, SListHook<T> T::*HookPtr>
void SList<T, HookPtr>::Iterator::goForward() noexcept {
  prev_ = curr_;
  curr_ = slist_->getNext(*curr_);
}

template <typename T, SListHook<T> T::*HookPtr>
typename SList<T, HookPtr>::Iterator&
SList<T, HookPtr>::Iterator::operator++() noexcept {
  XDCHECK(curr_ != nullptr);
  if (curr_ != nullptr) {
    goForward();
  }
  return *this;
}

template <typename T, SListHook<T> T::*HookPtr>
typename SList<T, HookPtr>::Iterator SList<T, HookPtr>::begin() const noexcept {
  return SList<T, HookPtr>::Iterator(head_, *this);
}

template <typename T, SListHook<T> T::*HookPtr>
typename SList<T, HookPtr>::Iterator SList<T, HookPtr>::end() const noexcept {
  return SList<T, HookPtr>::Iterator(nullptr, *this);
}

template <typename T, SListHook<T> T::*HookPtr>
typename SList<T, HookPtr>::Iterator SList<T, HookPtr>::remove(
    const SList<T, HookPtr>::Iterator& it) {
  if (!it.belongsToList(this)) {
    throw std::logic_error("iterator does not belong to this list");
  }

  if (it == end()) {
    throw std::logic_error("Can not remove end");
  }

  T* const prev = it.previous();
  T* const next = getNext(*it);

  if (prev == nullptr) {
    // Deleting the head. So call pop instead.
    pop();
  } else {
    // Node to be deleted
    T& curr = *it;

    // Point the previous node to the next node
    setNext(*prev, next);
    size_--;
    if (tail_ == &curr) {
      tail_ = prev;
    }

    checkStateOrThrow();
    // reset the pointer for old node.
    setNext(curr, nullptr);
  }

  // Return an iterator to the new node at this position
  return SList<T, HookPtr>::Iterator(next, *this, prev);
}
} // namespace cachelib
} // namespace facebook
