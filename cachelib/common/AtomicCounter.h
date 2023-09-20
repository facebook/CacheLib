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

#include <atomic>
#include <cstdint>

#include "cachelib/common/FastStats.h"

namespace facebook {
namespace cachelib {

// Atomic counter for statistics using std::atomic
class AtomicCounter {
 public:
  AtomicCounter() = default;

  explicit AtomicCounter(uint64_t init) : val_{init} {}

  ~AtomicCounter() = default;

  AtomicCounter(const AtomicCounter& rhs)
      : val_{rhs.val_.load(std::memory_order_relaxed)} {}

  AtomicCounter& operator=(const AtomicCounter& rhs) {
    val_ = rhs.val_.load(std::memory_order_relaxed);
    return *this;
  }

  uint64_t get() const { return val_.load(std::memory_order_relaxed); }

  void set(uint64_t n) { val_.store(n, std::memory_order_relaxed); }

  uint64_t add_fetch(uint64_t n) {
    return val_.fetch_add(n, std::memory_order_relaxed) + n;
  }

  void add(uint64_t n) { val_.fetch_add(n, std::memory_order_relaxed); }

  uint64_t sub_fetch(uint64_t n) {
    return val_.fetch_sub(n, std::memory_order_relaxed) - n;
  }

  void sub(uint64_t n) { val_.fetch_sub(n, std::memory_order_relaxed); }

  void inc() { add(1); }

  void dec() { sub(1); }

 private:
  std::atomic<uint64_t> val_{0};
};

// provides the same interface as the Counter, but uses a thread local
// approach. Does not provide atomic fetch_add and fetch_sub semantics.
class TLCounter {
 public:
  TLCounter() = default;
  explicit TLCounter(uint64_t init) : val_{init} {}
  ~TLCounter() = default;

  uint64_t get() const { return val_.getSnapshot(); }

  void set(uint64_t n) { val_.tlStats() = n; }

  uint64_t add_fetch(uint64_t) {
    throw std::runtime_error("add_fetch not supported");
  }
  void add(uint64_t n) { val_.tlStats() += n; }

  uint64_t sub_fetch(uint64_t) {
    throw std::runtime_error("sub_fetch not supported");
  }
  void sub(uint64_t n) { val_.tlStats() -= n; }

  void inc() { ++val_.tlStats(); }
  void dec() { --val_.tlStats(); }

 private:
  util::FastStats<uint64_t> val_{};
};

} // namespace cachelib
} // namespace facebook
