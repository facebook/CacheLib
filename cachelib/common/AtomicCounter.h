#pragma once

#include <atomic>
#include <cstdint>

namespace facebook {
namespace cachelib {

// Atomic counter for statistics
class AtomicCounter {
 public:
  AtomicCounter() = default;

  explicit AtomicCounter(uint64_t init) : val_{init} {}

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

  uint64_t inc() { return add(1); }

  uint64_t dec() { return sub(1); }

 private:
  std::atomic<uint64_t> val_{0};
};

} // namespace cachelib
} // namespace facebook
