
#pragma once
#include <folly/concurrency/ConcurrentHashMap.h>

#include <atomic>
#include <deque>
#include <mutex>

namespace facebook::cachelib::util {
namespace detail {

class FIFOConcurrentHashSet {
 public:
  using Key = uint64_t; // hashed object id
  using TS = uint64_t;  // logical time / FIFO order

  explicit FIFOConcurrentHashSet(size_t capacity)
      : ghost_(capacity), seq_(0), queueSize_(0) {}

  FIFOConcurrentHashSet() : seq_(0), queueSize_(0) {}

  struct PendingEntry {
    Key k;
    TS ts;
  };

  inline void insert(Key k) {
    TS ts = seq_.fetch_add(1, std::memory_order_relaxed);
    ghost_.insert_or_assign(k, ts);
  }

  inline bool contains(Key k) const {
    auto it = ghost_.find(k);
    if (it == ghost_.end())
      return false;
    // Seq - current must be larger
    auto currDiff = seq_.load(std::memory_order_relaxed) - it->second;
    return currDiff <=
           queueSize_.load(std::memory_order_relaxed); // stale == not present
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
} // namespace facebook::cachelib::util