#pragma once

#include <folly/Format.h>
#include <folly/Math.h>
#include <folly/container/F14Set.h>

#include <cstdint>
#include <deque>
#include <stdexcept>
#include <unordered_set>

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

  FIFOHashSetBase(FIFOHashSetBase&& other) noexcept {
    maxSize_ = other.maxSize_;
    fifo_ = std::move(other.fifo_);
    set_ = std::move(other.set_);
    other.maxSize_ = 0;
  }

  FIFOHashSetBase& operator=(FIFOHashSetBase&& other) noexcept {
    if (this != &other) {
      maxSize_ = other.maxSize_;
      fifo_ = std::move(other.fifo_);
      set_ = std::move(other.set_);
      other.maxSize_ = 0;
    }
    return *this;
  }

  bool contains(KeyT key) {
    return set_.contains(key);
  }

  // Enforce size variant of contains
  bool containsEnforce(KeyT key) {
    enforceCapacityLocked();
    return set_.contains(key);
  }

  size_t size() {
    return set_.size();
  }

  size_t capacity() {
    return maxSize_;
  }

  void reserve(size_t newCap) {
    set_.reserve(newCap);
  }

  void insert(KeyT key) {
    auto [it, inserted] = set_.insert(key);
    if (inserted) {
      fifo_.push_back(key); 
    }
    enforceCapacityLocked();
  }

  // No enforce variant of insert
  void insertNoEnforce(KeyT key) {
    auto [it, inserted] = set_.insert(key);
    if (inserted) {
      fifo_.push_back(key);
    }
  }

  void resize(size_t newSize) {
    maxSize_ = newSize;
    enforceCapacityLocked();
  }

  // No enforce variant of resize
  void resizeNoEnforce(size_t newSize) {
    maxSize_ = newSize;
  }

 private:
  // To prevent tail latency, limits max elements removed per call.
  void enforceCapacityLocked() {
    constexpr int max_rem = 5;
    int removed = 0;

    while (fifo_.size() > maxSize_ && removed < max_rem) {
      KeyT k = fifo_.front();
      fifo_.pop_front();
      set_.erase(k);
      removed++;
    }
  }

 private:
  // mutable folly::SpinLock lock_;
  size_t maxSize_{0};
  std::deque<KeyT> fifo_;
  folly::F14FastSet<KeyT> set_;
};

} // namespace detail

using FIFOHashSet = detail::FIFOHashSetBase<uint64_t>;

} // namespace facebook::cachelib::util
