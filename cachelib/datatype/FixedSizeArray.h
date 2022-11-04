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

#include <folly/Format.h>

#include "cachelib/allocator/TypedHandle.h"
#include "cachelib/datatype/DataTypes.h"

namespace facebook {
namespace cachelib {

namespace detail {
// @param T must be a POD like type
// Note that packing is not guaranteed with this data structure. User
// of FixedSizeArray must ensure their type is packed if they need it.
template <typename T>
class FixedSizeArrayLayout {
 public:
  // Compute the storage required for the number of elements
  static uint32_t computeStorageSize(uint32_t numElements) {
    return sizeof(FixedSizeArrayLayout) + numElements * sizeof(T);
  }

  // Construct a new layout and default iniitlaize all elements
  explicit FixedSizeArrayLayout(uint32_t n) : numElements_{n} {
    for (auto& element : *this) {
      new (&element) T{};
    }
  }

  // Returns how many elements there're in this array
  uint32_t size() const { return numElements_; }

  // Accessors. No bounds checking.
  T& operator[](uint32_t index) { return getAt(index); }
  const T& operator[](uint32_t index) const { return getAt(index); }

  // Accessors with bounds checking.
  // @throw std::out_of_range   if index is out of range
  T& at(uint32_t index) {
    checkBounds(index);
    return getAt(index);
  }
  const T& at(uint32_t index) const {
    checkBounds(index);
    return getAt(index);
  }

  // Copy the content in this array into any container that supports
  // an std insertion iterator: (insert_iterator, back_insert_iterator,
  // front_insert_iterator), or alternatively a forward iterator.
  template <typename InsertionIterator>
  void copyTo(InsertionIterator itr) const {
    for (const auto& element : *this) {
      *itr = element;
      ++itr;
    }
  }

  // Copy the content pointed by forward iterator
  // The storage of FixedSizeArray must be enough to finish the copying
  template <typename ForwardIterator>
  void copyFrom(ForwardIterator&& itr, ForwardIterator&& end) {
    auto myItr = begin();
    while (itr != end) {
      *myItr = *itr;
      ++itr;
      ++myItr;
    }
  }

  using Iterator = util::ForwardIterator<T>;
  Iterator begin() { return Iterator{&elements_[0], &elements_[numElements_]}; }
  Iterator end() {
    return Iterator{&elements_[size()], &elements_[numElements_]};
  }

  using ConstIterator = util::ForwardIterator<const T>;
  ConstIterator begin() const {
    return ConstIterator{&elements_[0], &elements_[numElements_]};
  }
  ConstIterator end() const {
    return ConstIterator{&elements_[size()], &elements_[numElements_]};
  }
  ConstIterator cbegin() const {
    return ConstIterator{&elements_[0], &elements_[numElements_]};
  }
  ConstIterator cend() const {
    return ConstIterator{&elements_[size()], &elements_[numElements_]};
  }

  bool operator==(const FixedSizeArrayLayout& rhs) const {
    if (size() != rhs.size()) {
      return false;
    }
    for (uint32_t i = 0; i < size(); ++i) {
      if (getAt(i) != rhs.getAt(i)) {
        return false;
      }
    }
    return true;
  }
  bool operator!=(const FixedSizeArrayLayout& rhs) const {
    return !(*this == rhs);
  }

 private:
  const uint32_t numElements_;
  T elements_[];

  void checkBounds(uint32_t index) const {
    if (index >= numElements_) {
      const auto str =
          folly::sformat("index: {}, numElemnts: {}", index, numElements_);
      throw std::out_of_range(str);
    }
  }

  T& getAt(uint32_t index) { return elements_[index]; }
  const T& getAt(uint32_t index) const { return elements_[index]; }
};

template <typename InputIt1, typename InputIt2>
bool isEqual(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2) {
  while (first1 != last1 && first2 != last2) {
    if (*first1 != *first2) {
      return false;
    }
    ++first1;
    ++first2;
  }
  return true;
}

template <typename T>
bool isEqual(const FixedSizeArrayLayout<T>& l, const std::vector<T>& r) {
  return l.size() == r.size() &&
         isEqual(std::begin(l), std::end(l), std::begin(r), std::end(r));
}

template <typename T, std::size_t N>
bool isEqual(const FixedSizeArrayLayout<T>& l, const std::array<T, N>& r) {
  return l.size() == r.size() &&
         isEqual(std::begin(l), std::end(l), std::begin(r), std::end(r));
}
} // namespace detail

// @param T   must be a POD like type
// @param C   this is an instance of CacheAllocator<> or provides
//            the same functionality
template <typename T, typename C>
class FixedSizeArray {
 public:
  using Element = T;

  using CacheType = C;
  using Item = typename CacheType::Item;
  using WriteHandle = typename Item::WriteHandle;

  using Layout = detail::FixedSizeArrayLayout<Element>;
  using LayoutHandle = TypedHandleImpl<Item, Layout>;
  using Iterator = typename Layout::Iterator;
  using ConstIterator = typename Layout::ConstIterator;

  // Convert to a fixed size array from a WriteHandle
  // This does not modify anything in the item
  static FixedSizeArray fromWriteHandle(WriteHandle handle) {
    return FixedSizeArray<T, C>{std::move(handle)};
  }

  // Compute the storage required for the number of elements
  static uint32_t computeStorageSize(uint32_t numElements) {
    return Layout::computeStorageSize(numElements);
  }

  // Construct a fixed size array from a WriteHandle
  // This modifies the item's memory
  // @throw std::invalid_argument  if the item does not have enough memory
  template <typename SizeT>
  FixedSizeArray(WriteHandle handle, SizeT numElements)
      : layout_{std::move(handle)} {
    const auto requiredSize = computeStorageSize(numElements);
    const auto itemSize = layout_.viewWriteHandle()->getSize();
    if (requiredSize > itemSize) {
      throw std::invalid_argument(folly::sformat(
          "Item size too small. Expected at least: {}, Actual: {}",
          requiredSize, itemSize));
    }
    new (layout_.viewWriteHandle()->getMemory())
        detail::FixedSizeArrayLayout<Element>(numElements);
  }

  // FixedSizeArray can be moved but not copied
  FixedSizeArray(FixedSizeArray&& rhs) : layout_{std::move(rhs.layout_)} {}
  FixedSizeArray& operator=(FixedSizeArray&& rhs) {
    resetToWriteHandle();
    new (this) FixedSizeArray{std::move(rhs)};
  }

  bool operator==(const FixedSizeArray& rhs) const {
    return *layout_ == *(rhs.layout_);
  }
  bool operator!=(const FixedSizeArray& rhs) const {
    return *layout_ != *(rhs.layout_);
  }

  Iterator begin() { return layout_->begin(); }
  Iterator end() { return layout_->end(); }
  ConstIterator begin() const { return layout_->cbegin(); }
  ConstIterator end() const { return layout_->cend(); }
  ConstIterator cbegin() const { return layout_->cbegin(); }
  ConstIterator cend() const { return layout_->cend(); }

  // Return the number of elements in the array
  uint32_t size() const { return layout_->size(); }

  Element& operator[](uint32_t index) { return (*layout_)[index]; }
  const Element& operator[](uint32_t index) const { return (*layout_)[index]; }

  Element& at(uint32_t index) { return layout_->at(index); }
  const Element& at(uint32_t index) const { return layout_->at(index); }

  // Copy elements in this array into the destination. The destination
  // container must have sufficient capacity.
  template <typename InsertionIterator>
  void copyTo(InsertionIterator&& itr) const {
    return layout_->copyTo(std::forward<InsertionIterator>(itr));
  }

  // Copy elements from start to the end. This array must have
  // sufficient capacity.
  template <typename ForwardIterator>
  void copyFrom(ForwardIterator&& itr, ForwardIterator&& end) {
    return layout_->copyFrom(std::forward<ForwardIterator>(itr),
                             std::forward<ForwardIterator>(end));
  }

  // This does not modify the content of this structure.
  // It resets it to a write handle, which can be used with any API in
  // CacheAllocator that deals with WriteHandle. After invoking this function,
  // this structure is left in a null state.
  WriteHandle resetToWriteHandle() && {
    return std::move(layout_).resetToWriteHandle();
  }

  // Borrow the write handle underneath this structure. This is useful to
  // implement insertion into CacheAllocator.
  const WriteHandle& viewWriteHandle() const {
    return layout_.viewWriteHandle();
  }

  bool isNullWriteHandle() const { return layout_ == nullptr; }

 private:
  LayoutHandle layout_;

  // Convert to a fixed size array from a WriteHandle
  // This does not modify anything in the item
  explicit FixedSizeArray(WriteHandle handle) : layout_{std::move(handle)} {}
};

template <typename T, typename C>
bool isEqual(const FixedSizeArray<T, C>& l, const std::vector<T>& r) {
  return l.size() == r.size() && detail::isEqual(std::begin(l), std::end(l),
                                                 std::begin(r), std::end(r));
}

template <typename T, typename C, std::size_t N>
bool isEqual(const FixedSizeArray<T, C>& l, const std::array<T, N>& r) {
  return l.size() == r.size() && detail::isEqual(std::begin(l), std::end(l),
                                                 std::begin(r), std::end(r));
}
} // namespace cachelib
} // namespace facebook
