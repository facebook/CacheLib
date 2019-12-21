#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/ThreadLocal.h>
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {
namespace util {

// forward declare
template <typename T>
class FastStats;

namespace detail {

// implementation detail that wraps a T and has a destructor that can
// accumulate into a global instance
template <typename T>
class SafeStat {
 public:
  SafeStat(FastStats<T>& parent) noexcept : parent_(parent) {}

  T& stats() { return stats_; }
  const T& stats() const { return stats_; }

  ~SafeStat() {
    // while destroying detail::SafeStat, to sum up the stats to parent stats.
    parent_.accumulateOnDestroy(stats_);
  }

 private:
  // parent stats for collecting.
  FastStats<T>& parent_;

  // this instance of the stats
  T stats_{};
};

} // namespace detail

// Wraps a folly::ThreadLocal<T> in a way it safely preserves the stats when
// threads are destroyed. T needs to have an operator += that is safe to
// execute while being accessed and written by others. uint64_t's or a POD of
// uint64_t is usually okay.
template <typename T>
class FastStats {
 public:
  explicit FastStats(const T& t)
      : parent_{t},
        tlStats_([this]() { return new detail::SafeStat<T>(*this); }) {}

  FastStats() : tlStats_([this]() { return new detail::SafeStat<T>(*this); }) {}

  // return a reference to the original T
  T& tlStats() { return tlStats_->stats(); }
  const T& tlStats() const { return tlStats_->stats(); }

  // get a snapshot across all instances.
  T getSnapshot() const {
    T res = parent_;
    for (const auto& tl : tlStats_.accessAllThreads()) {
      res += tl.stats();
    }
    return res;
  }

  // returns the current number of active stats instances held by threads.
  size_t getActiveThreadCount() const {
    const auto t = tlStats_.accessAllThreads();
    return std::distance(t.begin(), t.end());
  }

  // apply a read across all the instances including the accumulated parent
  // version from destroyed threads.
  template <typename ApplyFn = std::function<void(const T&)>>
  void forEach(ApplyFn applyFn) const {
    applyFn(parent_);
    for (const auto& t : tlStats_.accessAllThreads()) {
      applyFn(t.stats());
    }
  }

 private:
  friend detail::SafeStat<T>;
  // called on destruction of a ThreadLocal
  void accumulateOnDestroy(const T& t) {
    std::lock_guard<std::mutex> l(mutex_);
    parent_ += t;
  }

  // the global accumulated value of destroyed instances
  T parent_{};

  // thread local instances
  folly::ThreadLocal<detail::SafeStat<T>, FastStats> tlStats_;

  // mutex protecting the accumulation into the parent_
  mutable std::mutex mutex_;
};

} // namespace util
} // namespace cachelib
} // namespace facebook
