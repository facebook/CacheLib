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

/* Iterator Implementation */
template <typename T, DListHook<T> T::*HookPtr>
void MultiDList<T, HookPtr>::Iterator::goForward() noexcept {
  if (index_ == kInvalidIndex) {
    return; // Can't go any further
  }
  // Move iterator forward
  ++currIter_;
  // If we land at the rend of this list, move to the previous list.
  while (index_ != kInvalidIndex &&
         currIter_ == mlist_.lists_[index_]->rend()) {
    --index_;
    if (index_ != kInvalidIndex) {
      currIter_ = mlist_.lists_[index_]->rbegin();
    }
  }
}

template <typename T, DListHook<T> T::*HookPtr>
void MultiDList<T, HookPtr>::Iterator::goBackward() noexcept {
  if (index_ == mlist_.lists_.size()) {
    return; // Can't go backward
  }
  // If we're not at rbegin, we can go backward
  if (currIter_ != mlist_.lists_[index_]->rbegin()) {
    --currIter_;
    return;
  }
  // We're at rbegin, jump to the head of the next list.
  while (index_ < mlist_.lists_.size() &&
         currIter_ == mlist_.lists_[index_]->rbegin()) {
    ++index_;
    if (index_ < mlist_.lists_.size()) {
      currIter_ = DListIterator(mlist_.lists_[index_]->getHead(),
                                DListIterator::Direction::FROM_TAIL,
                                *(mlist_.lists_[index_].get()));
    }
  }
}

template <typename T, DListHook<T> T::*HookPtr>
void MultiDList<T, HookPtr>::Iterator::initToValidRBeginFrom(
    size_t listIdx) noexcept {
  // Find the first non-empty list.
  index_ = listIdx;
  while (index_ != std::numeric_limits<size_t>::max() &&
         mlist_.lists_[index_]->size() == 0) {
    --index_;
  }
  currIter_ = index_ == std::numeric_limits<size_t>::max()
                  ? mlist_.lists_[0]->rend()
                  : mlist_.lists_[index_]->rbegin();
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator&
MultiDList<T, HookPtr>::Iterator::operator++() noexcept {
  goForward();
  return *this;
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator&
MultiDList<T, HookPtr>::Iterator::operator--() noexcept {
  goBackward();
  return *this;
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator MultiDList<T, HookPtr>::rbegin()
    const noexcept {
  return MultiDList<T, HookPtr>::Iterator(*this);
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator MultiDList<T, HookPtr>::rbegin(
    size_t listIdx) const {
  if (listIdx >= lists_.size()) {
    throw std::invalid_argument("Invalid list index for MultiDList iterator.");
  }
  return MultiDList<T, HookPtr>::Iterator(*this, listIdx);
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator MultiDList<T, HookPtr>::rend()
    const noexcept {
  auto it = MultiDList<T, HookPtr>::Iterator(*this);
  it.reset();
  return it;
}
} // namespace cachelib
} // namespace facebook
