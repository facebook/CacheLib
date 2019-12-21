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
    new (this) AtomicCounter(rhs);
    return *this;
  }

  uint64_t get() const { return val_.load(std::memory_order_relaxed); }

  void set(uint64_t n) { val_.store(n, std::memory_order_relaxed); }

  // @add, @sub, @inc and @dec all return counter's new value
  uint64_t add(uint64_t n) {
    return val_.fetch_add(n, std::memory_order_relaxed) + n;
  }

  uint64_t sub(uint64_t n) {
    return val_.fetch_sub(n, std::memory_order_relaxed) - n;
  }

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

  // this is not an atomic fetch_add equivalent.
  uint64_t add(uint64_t n) {
    val_.tlStats() += n;
    return get();
  }

  // this is not an atomic fetch_sub equivalent.
  uint64_t sub(uint64_t n) {
    val_.tlStats() -= n;
    return get();
  }

  void inc() { ++val_.tlStats(); }
  void dec() { --val_.tlStats(); }

 private:
  util::FastStats<uint64_t> val_{};
};

} // namespace cachelib
} // namespace facebook
