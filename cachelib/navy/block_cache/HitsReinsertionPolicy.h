#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

#include <folly/stats/QuantileEstimator.h>

#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/ReinsertionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
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

  void setIndex(Index* index) override { index_ = index; }

  bool shouldReinsert(HashedKey hk) override;

  void persist(RecordWriter& rw) override;

  void recover(RecordReader& rr) override;

  void getCounters(const CounterVisitor& visitor) const override;

  AccessStats getAccessStats(HashedKey hk) const;

 private:
  // TDOD: remove kNumLocks once all bigcache hosts being upgraded
  // Each access map is guarded by its own lock. We shard 1K ways.
  static constexpr size_t kNumLocks = (2 << 10);

  const uint8_t hitsThreshold_{};

  Index* index_;

  mutable util::PercentileStats hitsOnReinsertionEstimator_{
      Index::kQuantileWindowSize};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
