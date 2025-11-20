/*
 * Copyright (c) Meta Platforms, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...
 */

#pragma once

#include <cstdint>
#include <deque>
#include <stdexcept>
#include <unordered_set>

namespace facebook::cachelib::util {

namespace detail {

// A bounded FIFO ghost list with O(1) membership check using a hash set.
// Used by caching algorithms such as S3-FIFO, ARC, LIRS, CLOCK-Pro, etc.
//
// Semantics:
//  - Tracks recently-evicted keys
//  - Fixed capacity; evicts oldest entry (FIFO)
//  - Membership test is O(1)
//  - No TTL, no ranking, no frequency tracking
//  - User must synchronize external concurrency
//
// Structure:
//   deque<uint32_t>    : stores FIFO eviction order
//   unordered_set<uint32_t> : stores O(1) membership
//
template <typename KeyT = uint32_t>
class FIFOHashSetBase {
 public:
  // @param capacity   Maximum number of ghost entries to keep.
  explicit FIFOHashSetBase(size_t capacity) : maxSize_{capacity} {
    if (capacity == 0) {
      throw std::invalid_argument{"GhostQueue capacity must be > 0"};
    }
  }

  FIFOHashSetBase() = default;

  FIFOHashSetBase(const FIFOHashSetBase&) = delete;
  FIFOHashSetBase& operator=(const FIFOHashSetBase&) = delete;

  FIFOHashSetBase(FIFOHashSetBase&& other) noexcept
      : maxSize_{other.maxSize_},
        fifo_{std::move(other.fifo_)},
        set_{std::move(other.set_)} {
    other.maxSize_ = 0;
  }

  FIFOHashSetBase& operator=(FIFOHashSetBase&& other) noexcept {
    if (this != &other) {
      this->~FIFOHashSetBase();
      new (this) FIFOHashSetBase(std::move(other));
    }
    return *this;
  }

  // O(1) membership check.
  bool contains(KeyT key) const { return set_.find(key) != set_.end(); }

  // Insert a key into ghost queue. If already present, no-op.
  void insert(KeyT key) {
    if (set_.count(key) > 0) {
      return;
    }

    fifo_.push_back(key);
    set_.insert(key);

    enforceCapacity();
  }

  // Resize queue capacity (evicts oldest entries if shrinking).
  void resize(size_t newSize) {
    if (newSize == 0) {
      throw std::invalid_argument{"GhostQueue capacity must be > 0"};
    }
    maxSize_ = newSize;
    enforceCapacity();
  }

  size_t size() const { return set_.size(); }

  size_t capacity() const { return maxSize_; }

 private:
  // Remove oldest items until size <= capacity.
  void enforceCapacity() {
    while (fifo_.size() > maxSize_) {
      KeyT k = fifo_.front();
      fifo_.pop_front();
      set_.erase(k);
    }
  }

 private:
  size_t maxSize_{0};
  std::deque<KeyT> fifo_{};
  std::unordered_set<KeyT> set_{};
};

} // namespace detail
using FIFOHashSet = detail::FIFOHashSetBase<uint32_t>;

} // namespace facebook::cachelib::util
