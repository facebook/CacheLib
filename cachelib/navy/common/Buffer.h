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

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>

#include "cachelib/navy/common/Utils.h"
#include "folly/Range.h"

namespace facebook {
namespace cachelib {
namespace navy {

// View into a buffer. Doesn't own data. Caller must ensure buffer
// lifetime. We offer two versions:
//  1. read-only - BufferView
//  2. mutable - MutableBufferView
template <typename T>
class BufferViewT {
 public:
  using Type = T;

  constexpr BufferViewT() : BufferViewT{0, nullptr} {}

  // @param size  Size of data in bytes
  // @param data  Pointer to the data this view will encapsulate
  constexpr BufferViewT(size_t size, Type* data) : size_{size}, data_{data} {}

  BufferViewT(const BufferViewT&) = default;
  BufferViewT& operator=(const BufferViewT&) = default;

  // Return true if data is nullptr
  bool isNull() const { return data_ == nullptr; }

  // Return byte at specified index. This view must NOT be null. Caller is
  // responsible for ensuring the index is within bounds.
  uint8_t byteAt(size_t idx) const {
    XDCHECK(data_);
    XDCHECK_LT(idx, size_);
    return data_[idx];
  }

  // Return beginning of the data
  Type* data() const { return data_; }

  // Return end of the data
  Type* dataEnd() const { return data_ + size_; }

  // Return size of the view in bytes
  size_t size() const { return size_; }

  // @param dst   Destination buffer into which to copy data from this view
  void copyTo(void* dst) const {
    if (data_ != nullptr) {
      XDCHECK_NE(dst, nullptr);
      std::memcpy(dst, data_, size_);
    }
  }

  // Return a new view from part of this current view. This view must NOT be
  // null.
  // @param offset  start of the data to encapsulate
  // @param size    size of bytes for the new view
  BufferViewT slice(size_t offset, size_t size) const {
    // Use - instead of + to avoid potential overflow problem
    XDCHECK_LE(offset, size_);
    XDCHECK_LE(size, size_ - offset);
    return BufferViewT{size, data_ + offset};
  }

  // Two views are equal if their contents are identical
  bool operator==(BufferViewT other) const {
    return size_ == other.size_ &&
           (size_ == 0 || std::memcmp(other.data_, data_, size_) == 0);
  }

  bool operator!=(BufferViewT other) const { return !(*this == other); }

 private:
  size_t size_{};
  Type* data_{};
};

using BufferView = BufferViewT<const uint8_t>;
using MutableBufferView = BufferViewT<uint8_t>;

struct BufferDeleter {
  void operator()(void* ptr) const { std::free(ptr); }
};

// Byte buffer. Manages buffer lifetime.
class Buffer {
 public:
  Buffer() = default;

  // Copy data from a view
  // @param view    source of data to be copied from. Cannot be null.
  explicit Buffer(BufferView view) : Buffer{view.size()} {
    view.copyTo(data());
  }

  // Copy data from a view into an aligned buffer
  // @param view        source of data to be copied from. Cannot be null.
  // @param alignment   alignment of the buffer data.
  Buffer(BufferView view, size_t alignment) : Buffer{view.size(), alignment} {
    view.copyTo(data());
  }

  // Create a new, empty buffer
  // @param size    size of bytes of the buffer
  explicit Buffer(size_t size) : size_{size}, data_{allocate(size)} {}

  // Create a new, empty, and aligned buffer
  // @param size        size of bytes of the buffer, it must be a
  //                    multiple of the alignment
  // @param alignment   alignment of the buffer
  Buffer(size_t size, size_t alignment)
      : size_{size}, data_{allocate(size, alignment)} {}

  // Use @copy instead to make it explicit and visible
  Buffer(const Buffer&) = delete;
  Buffer& operator=(const Buffer&) = delete;

  Buffer(Buffer&& other) noexcept = default;
  Buffer& operator=(Buffer&&) noexcept = default;

  // Return a read-only view
  BufferView view() const { return BufferView{size_, data()}; }

  // Return a mutable view
  MutableBufferView mutableView() { return MutableBufferView{size_, data()}; }

  // Return true if data is nullptr
  bool isNull() const { return data_ == nullptr; }

  // Return read-only start of the data
  const uint8_t* data() const { return data_.get() + dataStartOffset_; }

  // Return mutable start of the data
  uint8_t* data() { return data_.get() + dataStartOffset_; }

  // Return size in bytes for the data
  size_t size() const { return size_; }

  // Copy copies size_ number of bytes from dataOffsetStart_ to a new buffer
  // and returns the new buffer
  // @param alignment   alignment of the new buffer
  Buffer copy(size_t alignment = 0) const {
    return (alignment == 0) ? copyInternal(Buffer{size_})
                            : copyInternal(Buffer{size_, alignment});
  }

  // This buffer must NOT be null and it must have sufficient capacity
  // to copy the data from the source view.
  // @param offset  copy from the offset for the view until the end
  // @param view    source of the data to be copied from
  void copyFrom(size_t offset, BufferView view) {
    XDCHECK_LE(offset + view.size(), size_);
    XDCHECK_NE(data_, nullptr);
    if (view.data() != nullptr) {
      std::memcpy(data() + offset, view.data(), view.size());
    }
  }

  // Adjust the data start offset forwards to include less valid data
  // This moves the data pointer forwards so that the first amount bytes are no
  // longer considered valid data.  The caller is responsible for ensuring that
  // amount is less than or equal to the actual data length.
  //
  // This does not modify any actual data in the buffer.
  void trimStart(size_t amount) {
    XDCHECK_LE(amount, size_);
    dataStartOffset_ += amount;
    size_ -= amount;
  }

  // Shrink buffer logical size (doesn't reallocate)
  void shrink(size_t size) {
    XDCHECK_LE(size, size_);
    size_ = size;
  }

  // Clear the buffer
  void reset() {
    size_ = 0;
    data_.reset();
  }

 private:
  Buffer copyInternal(Buffer buf) const {
    if (data_) {
      XDCHECK_NE(buf.data_, nullptr);
      std::memcpy(buf.data(), data(), size_);
    }
    return buf;
  }

  static uint8_t* allocate(size_t size) {
    auto ptr = reinterpret_cast<uint8_t*>(std::malloc(size));
    if (!ptr) {
      throw std::bad_alloc();
    }
    return ptr;
  }

  static uint8_t* allocate(size_t size, size_t alignment) {
    XDCHECK(folly::isPowTwo(alignment)); // Also ensures @alignment > 0
    XDCHECK_EQ(size % alignment, 0u);
    auto ptr = reinterpret_cast<uint8_t*>(::aligned_alloc(alignment, size));
    if (!ptr) {
      throw std::bad_alloc();
    }
    return ptr;
  }

  // size_ represents the size of valid data in the data_, i.e., "size_" number
  // of bytes from startOffset in data_ are considered valid in the Buffer
  size_t size_{};

  // dataStartOffset_ is the offset in data_ where the actual(user-interested)
  // data starts. This helps in skipping past unnecessary data in the buffer
  // without having to copy it. There could be unnecessary data in the buffer
  // due to read/write from/to a block-aligned address when the actual data
  // starts somewhere in the middle(ie not at the block aligned address).
  size_t dataStartOffset_{0};
  std::unique_ptr<uint8_t[], BufferDeleter> data_{};
};

inline BufferView toView(MutableBufferView mutableView) {
  return {mutableView.size(), mutableView.data()};
}

// Trailing 0 is not included
inline BufferView makeView(const char* cstr) {
  return {std::strlen(cstr), reinterpret_cast<const uint8_t*>(cstr)};
}

inline BufferView makeView(folly::StringPiece str) {
  return {str.size(), reinterpret_cast<const uint8_t*>(str.data())};
}

inline folly::StringPiece toStringPiece(BufferView view) {
  return {reinterpret_cast<const char*>(view.data()), view.size()};
}

// Convert to string suitable for debug prints the best
std::string toString(BufferView view, bool compact = true);

// For better interaction with gtest
inline std::ostream& operator<<(std::ostream& os, BufferView view) {
  return os << toString(view);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
