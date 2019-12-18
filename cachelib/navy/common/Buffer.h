#pragma once

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>

#include <folly/logging/xlog.h>
#include "cachelib/navy/common/Utils.h"

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
  constexpr BufferViewT(size_t size, Type* data) : size_{size}, data_{data} {}
  BufferViewT(const BufferViewT&) = default;
  BufferViewT& operator=(const BufferViewT&) = default;

  bool isNull() const { return data_ == nullptr; }

  uint8_t byteAt(size_t idx) const {
    XDCHECK_LT(idx, size_);
    return data_[idx];
  }

  Type* data() const { return data_; }

  Type* dataEnd() const { return data_ + size_; }

  size_t size() const { return size_; }

  void copyTo(void* dst) const {
    if (data_ != nullptr) {
      XDCHECK_NE(dst, nullptr);
      std::memcpy(dst, data_, size_);
    }
  }

  // Slices buffer in place: starting with @offset and of @size bytes.
  BufferViewT slice(size_t offset, size_t size) const {
    // Use - instead of + to avoid potential overflow problem
    XDCHECK_LE(offset, size_);
    XDCHECK_LE(size, size_ - offset);
    return BufferViewT{size, data_ + offset};
  }

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

  explicit Buffer(BufferView view) : Buffer{view.size()} {
    view.copyTo(data_.get());
  }

  Buffer(BufferView view, size_t alignment) : Buffer{view.size(), alignment} {
    view.copyTo(data_.get());
  }

  explicit Buffer(size_t size) : size_{size}, data_{allocate(size)} {}

  // @size must be multiple of @alignment
  Buffer(size_t size, size_t alignment)
      : size_{size}, data_{allocate(size, alignment)} {}

  // Use @copy instead to make it explicit and visible
  Buffer(const Buffer&) = delete;
  Buffer& operator=(const Buffer&) = delete;

  Buffer(Buffer&&) noexcept = default;
  Buffer& operator=(Buffer&&) noexcept = default;

  BufferView view() const { return BufferView{size_, data_.get()}; }

  MutableBufferView mutableView() {
    return MutableBufferView{size_, data_.get()};
  }

  bool isNull() const { return data_ == nullptr; }

  const uint8_t* data() const { return data_.get(); }

  uint8_t* data() { return data_.get(); }

  size_t size() const { return size_; }

  Buffer copy() const {
    Buffer buf{size_};
    if (data_) {
      XDCHECK_NE(buf.data_, nullptr);
      std::memcpy(buf.data_.get(), data_.get(), size_);
    }
    return buf;
  }

  Buffer copy(size_t alignment) const {
    Buffer buf{size_, alignment};
    if (data_) {
      XDCHECK_NE(buf.data_, nullptr);
      std::memcpy(buf.data_.get(), data_.get(), size_);
    }
    return buf;
  }

  void copyFrom(size_t offset, BufferView data) {
    XDCHECK_LE(offset + data.size(), size_);
    XDCHECK_NE(data_, nullptr);
    if (data.data() != nullptr) {
      std::memcpy(data_.get() + offset, data.data(), data.size());
    }
  }

  // Shrink buffer logical size (doesn't reallocate)
  void shrink(size_t size) {
    XDCHECK_LE(size, size_);
    size_ = size;
  }

  void reset() {
    size_ = 0;
    data_.reset();
  }

  // Release buffer ownership and reset
  uint8_t* release() {
    size_ = 0;
    return data_.release();
  }

 private:
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

  size_t size_{};
  std::unique_ptr<uint8_t[], BufferDeleter> data_{};
};

inline BufferView toView(MutableBufferView mutableView) {
  return {mutableView.size(), mutableView.data()};
}

// Trailing 0 is not included
inline BufferView makeView(const char* cstr) {
  return {std::strlen(cstr), reinterpret_cast<const uint8_t*>(cstr)};
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
