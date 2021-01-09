#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace facebook {
namespace cachelib {
namespace cachebench {
// Fixed size ring buffer. We do not wrap indices using modulus. This way,
// iteration over the buffer is much
// simpler.
template <typename T, size_t Capacity>
class RingBuffer {
 public:
  using Value = T;

  static constexpr size_t kCapacity{Capacity};

  RingBuffer() = default;

  // Returns object index you can use to read/write directly.
  size_t write(const Value& val) {
    if (last_ - first_ >= kCapacity) {
      throw std::runtime_error("ring buffer overflow");
    }
    auto index = last_;
    last_++;
    data_[index % kCapacity] = val;
    return index;
  }

  Value read() {
    if (first_ >= last_) {
      throw std::runtime_error("ring buffer underflow");
    }
    auto index = first_;
    first_++;
    return data_[index % kCapacity];
  }

  Value getAt(size_t index) const {
    if (!(first_ <= index && index < last_)) {
      throw std::out_of_range("ring buffer index out of bounds for a get");
    }
    return data_[index % kCapacity];
  }

  void setAt(size_t index, const Value& val) const {
    if (!(first_ <= index && index < last_)) {
      throw std::out_of_range("ring buffer index out of bounds for a set");
    }
    data_[index % kCapacity] = val;
  }

  size_t first() const { return first_; }
  size_t last() const { return last_; }

  size_t size() const {
    XDCHECK_GE(last_, first_);
    return last_ - first_;
  }

 private:
  mutable Value data_[kCapacity]{};

  // We do not wrap with modulus to have ability check wrong index access in
  // getAt/setAt. Also, this makes iteration code simpler.
  size_t first_{0};
  size_t last_{0};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
