#pragma once

#include <folly/stats/QuantileEstimator.h>

#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/ReinsertionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Hits based reinsertion policy.
// By enabling this policy, we will reinsert item that had been accessed more
// than the threshold since the last time it was written into block cache. This
// can better approximate a LRU than the region-based LRU. Typically users
// apply this with a region-granularity FIFO eviction policy, or SFIFO eviction
// policy.
class HitsReinsertionPolicy : public ReinsertionPolicy {
 public:
  struct AccessStats {
    // Total hits during this item's entire lifetime in cache
    uint8_t totalHits{0};

    // Hits during the current window for this item (e.g. before re-admission)
    uint8_t currHits{0};
  };

  // @param hitsThreshold how many hits for an item is eligible for reinsertion
  explicit HitsReinsertionPolicy(uint8_t hitsThreshold);

  // Sets the index for hits based reinsertion policy.
  void setIndex(Index* index) override { index_ = index; }

  // Applies hits based policy to determine whether or not we should keep
  // this key around longer in cache.
  bool shouldReinsert(HashedKey hk) override;

  // Persists metadata associated with hits based reinsertion policy.
  void persist(RecordWriter& rw) override;

  void recover(RecordReader& /* rr */) override {}

  // Exports hits based reinsertion policy stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const override;

  // Gets the @AccessStats of a hashed key.
  // Returns empty when the key is not found.
  AccessStats getAccessStats(HashedKey hk) const;

 private:
  // TODO: T95755384 clean up kNumLocks
  static constexpr size_t kNumLocks = (2 << 10);

  const uint8_t hitsThreshold_{};

  Index* index_;

  mutable util::PercentileStats hitsOnReinsertionEstimator_{
      Index::kQuantileWindowSize};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
