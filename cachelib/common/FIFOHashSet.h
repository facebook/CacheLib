#pragma once

#include <folly/Format.h>
#include <folly/Math.h>
#include <folly/container/F14Set.h>
#include <folly/synchronization/DistributedMutex.h>

#include <cstdint>
#include <deque>
#include <stdexcept>

namespace facebook::cachelib::util {
namespace detail {

template <typename KeyT = uint32_t>
class FIFOHashSetBase {
 public:
  explicit FIFOHashSetBase(size_t capacity) : maxSize_{capacity} {
    if (capacity == 0) {
      throw std::invalid_argument{"GhostQueue capacity must be > 0"};
    }
  }

  FIFOHashSetBase() = default;

  FIFOHashSetBase(const FIFOHashSetBase&) = delete;
  FIFOHashSetBase& operator=(const FIFOHashSetBase&) = delete;


  bool contains(KeyT key) {
    return mutex_.lock_combine([&]() { return set_.contains(key); });
  }

  size_t size() {
    return mutex_.lock_combine([&]() { return set_.size(); });
  }

  size_t capacity() {
    return mutex_.lock_combine([&]() { return maxSize_; });
  }

  void reserve(size_t newCap) {
    mutex_.lock_combine([&]() { set_.reserve(newCap); });
  }

  void insert(KeyT key) {
    mutex_.lock_combine([&]() {
      auto [it, inserted] = set_.insert(key);
      if (inserted) {
        fifo_.push_back(key);
      }
      enforceCapacityLocked();
    });
  }

  void resize(size_t newSize) {
    mutex_.lock_combine([&]() {
      maxSize_ = newSize;
    });
  }

 private:
  // Enforce max removal to control tail latency
  void enforceCapacityLocked() {
    constexpr int max_rem = 2;
    int removed = 0;

    while (fifo_.size() > maxSize_ && removed < max_rem) {
      KeyT k = fifo_.front();
      fifo_.pop_front();
      set_.erase(k);
      ++removed;
    }
  }

 private:
  folly::DistributedMutex mutex_;

  size_t maxSize_{0};
  std::deque<KeyT> fifo_;
  folly::F14FastSet<KeyT> set_;
};

} // namespace detail

using FIFOHashSet32 = detail::FIFOHashSetBase<uint32_t>;

} // namespace facebook::cachelib::util