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

#include "cachelib/allocator/datastruct/DList.h"
#include "cachelib/common/CompilerUtils.h"

namespace facebook::cachelib {
class MM2Q;
template <typename MMType>
class MMTypeTest;

// Implements an intrusive doubly linked list using DList. This is used to build
// MMContainers with multiple priorities.
template <typename T, DListHook<T> T::*HookPtr>
class MultiDList {
 public:
  using CompressedPtrType = typename T::CompressedPtrType;
  using PtrCompressor = typename T::PtrCompressor;
  using SingleDList = DList<T, HookPtr>;
  using DListIterator = typename SingleDList::Iterator;
  using MultiDListObject = serialization::MultiDListObject;

  MultiDList(const MultiDList&) = delete;
  MultiDList& operator=(const MultiDList&) = delete;

  MultiDList(unsigned int numLists, PtrCompressor compressor) noexcept {
    for (unsigned int i = 0; i < numLists; i++) {
      lists_.emplace_back(std::make_unique<SingleDList>(compressor));
    }
  }

  // Restore MultiDList from saved state.
  //
  // @param object              saved MultiDList object
  // @param compressor          PtrCompressor object
  MultiDList(const MultiDListObject& object, PtrCompressor compressor) {
    for (const auto& list : *object.lists()) {
      lists_.emplace_back(std::make_unique<SingleDList>(list, compressor));
    }
  }

  /**
   * Exports the current state as a thrift object for later restoration.
   */
  MultiDListObject saveState() const {
    MultiDListObject state;
    for (const auto& listPtr : lists_) {
      state.lists()->emplace_back(listPtr->saveState());
    }
    return state;
  }

  SingleDList& getList(int index) const noexcept {
    return *(lists_[index].get());
  }

  size_t size() const noexcept {
    size_t sz = 0;
    for (const auto& list : lists_) {
      sz += list->size();
    }
    return sz;
  }

  void insertEmptyListAt(size_t pos, PtrCompressor compressor) {
    if (pos > lists_.size()) {
      throw std::invalid_argument(
          "Invalid position to insert empty list to MultiDList");
    }
    lists_.insert(lists_.begin() + pos,
                  std::make_unique<SingleDList>(compressor));
  }

  // Iterator interface for the double linked list. Supports both iterating
  // from the tail and head.
  class Iterator {
   public:
    // Initializes the iterator to the beginning.
    explicit Iterator(const MultiDList<T, HookPtr>& mlist) noexcept
        : currIter_(mlist.lists_[mlist.lists_.size() - 1]->rbegin()),
          mlist_(mlist) {
      resetToBegin();
      // We should either point to an element or the end() iterator
      // which has an invalid index_.
      XDCHECK(index_ == kInvalidIndex || currIter_.get() != nullptr);
    }

    explicit Iterator(const MultiDList<T, HookPtr>& mlist,
                      size_t listIdx, bool head) noexcept
        : currIter_(mlist.lists_[mlist.lists_.size() - 1]->rbegin()),
          mlist_(mlist) {
      XDCHECK_LT(listIdx, mlist.lists_.size());
      if (head) {
        initToValidBeginFrom(listIdx);
      } else {
        initToValidRBeginFrom(listIdx);
      }
      // We should either point to an element or the end() iterator
      // which has an invalid index_.
      XDCHECK(index_ == kInvalidIndex || index_ == mlist.lists_.size() || currIter_.get() != nullptr);
    }
    virtual ~Iterator() = default;

    // copyable and movable
    Iterator(const Iterator&) = default;
    Iterator& operator=(const Iterator&) = default;
    Iterator(Iterator&&) noexcept = default;
    Iterator& operator=(Iterator&&) noexcept = default;

    // moves the iterator forward and backward. Calling ++ once the iterator
    // has reached the end is undefined.
    Iterator& operator++() noexcept;
    Iterator& operator--() noexcept;

    T* operator->() const noexcept { return currIter_.operator->(); }
    T& operator*() const noexcept { return currIter_.operator*(); }

    bool operator==(const Iterator& other) const noexcept {
      return &mlist_ == &other.mlist_ && currIter_ == other.currIter_ &&
             index_ == other.index_;
    }

    bool operator!=(const Iterator& other) const noexcept {
      return !(*this == other);
    }

    explicit operator bool() const noexcept {
      return index_ < mlist_.lists_.size();
    }

    T* get() const noexcept { return currIter_.get(); }

    // Invalidates this iterator
    void reset() noexcept {
      // Set index to before first list
      index_ = kInvalidIndex;
      // Point iterator to first list's rend
      currIter_ = mlist_.lists_[0]->rend();
    }

    // Reset the iterator back to the beginning
    void resetToBegin() noexcept {
      initToValidRBeginFrom(mlist_.lists_.size() - 1);
    }

   protected:
    void goForward() noexcept;
    void goBackward() noexcept;

    // reset iterator to the beginning of a speicific queue
    void initToValidRBeginFrom(size_t listIdx) noexcept;
    
    // reset iterator to the head of a specific queue
    void initToValidBeginFrom(size_t listIdx) noexcept;

    // Index of current list
    size_t index_{0};
    // the current position of the iterator in the list
    DListIterator currIter_;
    const MultiDList<T, HookPtr>& mlist_;

    static constexpr size_t kInvalidIndex = std::numeric_limits<size_t>::max();
  };

  // provides an iterator starting from the tail of the linked list.
  Iterator rbegin() const noexcept;

  // provides an iterator starting from the tail of a specific list.
  Iterator rbegin(size_t idx) const;
  
  // provides an iterator starting from the head of a specific list.
  Iterator begin(size_t idx) const;

  // Iterator to compare against for the end.
  Iterator rend() const noexcept;

 private:
  std::vector<std::unique_ptr<SingleDList>> lists_;

  // testing
  FRIEND_TEST(MM2QTest, DeserializeToMoreLists);
};

/* Iterator Implementation */
template <typename T, DListHook<T> T::*HookPtr>
void MultiDList<T, HookPtr>::Iterator::goForward() noexcept {
  if (index_ == kInvalidIndex) {
    return; // Can't go any further
  }
  // Move iterator forward
  ++currIter_;

  if (currIter_.getDirection() == DListIterator::Direction::FROM_HEAD) {
    // If we land at the rend of this list, move to the previous list.
    while (index_ != kInvalidIndex && index_ != mlist_.lists_.size() &&
           currIter_ == mlist_.lists_[index_]->end()) {
      ++index_;
      if (index_ != kInvalidIndex && index_ != mlist_.lists_.size()) {
        currIter_ = mlist_.lists_[index_]->begin();
      } else {
          return;
      }
    }
  } else {
    // If we land at the rend of this list, move to the previous list.
    while (index_ != kInvalidIndex &&
           currIter_ == mlist_.lists_[index_]->rend()) {
      --index_;
      if (index_ != kInvalidIndex) {
        currIter_ = mlist_.lists_[index_]->rbegin();
      }
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
void MultiDList<T, HookPtr>::Iterator::initToValidBeginFrom(
    size_t listIdx) noexcept {
  // Find the first non-empty list.
  index_ = listIdx;
  while (index_ != mlist_.lists_.size() &&
         mlist_.lists_[index_]->size() == 0) {
    ++index_;
  }
  if (index_ == mlist_.lists_.size()) {
    //we reached the end - we should get set to
    //invalid index
    index_ = std::numeric_limits<size_t>::max();
  }
  currIter_ = index_ == std::numeric_limits<size_t>::max()
                  ? mlist_.lists_[0]->begin()
                  : mlist_.lists_[index_]->begin();
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
  return MultiDList<T, HookPtr>::Iterator(*this, listIdx, false);
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator MultiDList<T, HookPtr>::begin(
    size_t listIdx) const {
  if (listIdx >= lists_.size()) {
    throw std::invalid_argument("Invalid list index for MultiDList iterator.");
  }
  return MultiDList<T, HookPtr>::Iterator(*this, listIdx, true);
}

template <typename T, DListHook<T> T::*HookPtr>
typename MultiDList<T, HookPtr>::Iterator MultiDList<T, HookPtr>::rend()
    const noexcept {
  auto it = MultiDList<T, HookPtr>::Iterator(*this);
  it.reset();
  return it;
}
} // namespace facebook::cachelib
