
#pragma once
#include <folly/concurrency/ConcurrentHashMap.h>

#include <atomic>
#include <deque>
#include <mutex>

namespace facebook::cachelib::util {
namespace detail {

template <typename KeyT = uint32_t>
class FIFOConcurrentHashSetBase {
 public:
  using Key = KeyT; // hashed object id
  using TS = uint64_t;  // logical time / FIFO order

  explicit FIFOConcurrentHashSetBase(size_t capacity)
      : ghost_(capacity), seq_(0), queueSize_(0) {}

  FIFOConcurrentHashSetBase() : seq_(0), queueSize_(0) {}

  struct PendingEntry {
    Key k;
    TS ts;
  };

  inline void insert(Key k) {
    TS ts = seq_.fetch_add(1, std::memory_order_relaxed);
    ghost_.insert_or_assign(k, ts);
    // If it overflows and wraps around, we reset the entire structure.
    if (UNLIKELY(ts == std::numeric_limits<TS>::max())) {
      ghost_.clear();
    }
  }

  inline bool contains(Key k)  {
    auto it = ghost_.find(k);
    if (it == ghost_.end())
      return false;
    // Seq - current must be larger
    auto currDiff = seq_.load(std::memory_order_relaxed) - it->second;
    auto isValid = currDiff <= queueSize_.load(std::memory_order_relaxed);

    if (!isValid) {
      // remove stale entry
      ghost_.erase(k);
    }

    return isValid;
  }

  inline void resize(TS limitTS) {
    queueSize_.store(limitTS, std::memory_order_relaxed);
  }

 private:
  folly::ConcurrentHashMap<Key, TS> ghost_;

  // Logical TS counter
  std::atomic<TS> seq_;
  // Maximum size of the FIFO queue
  std::atomic<TS> queueSize_;
};

} // namespace detail
using FIFOConcurrentHashSet32 = detail::FIFOConcurrentHashSetBase<uint32_t>;
using FIFOConcurrentHashSet64 = detail::FIFOConcurrentHashSetBase<uint64_t>;
  
} // namespace facebook::cachelib::util