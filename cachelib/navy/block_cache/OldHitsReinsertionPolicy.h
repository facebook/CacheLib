#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

#include <folly/container/F14Map.h>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/ReinsertionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
class OldHitsReinsertionPolicy : public ReinsertionPolicy {
 public:
  struct AccessStats {
    // Total hits during this item's entire lifetime in cache
    uint8_t totalHits{0};

    // Hits during the current window for this item (e.g. before re-admission)
    uint8_t currHits{0};

    // How many times this item has been re-inserted while in cache
    uint8_t numReinsertions{0};
  };

  // @param hitsThreshold how many hits for an item is eligible for reinsertion
  explicit OldHitsReinsertionPolicy(uint8_t hitsThreshold);

  void setIndex(Index* /* index */) override {}

  void track(HashedKey hk);

  void touch(HashedKey hk) override;

  bool shouldReinsert(HashedKey hk) override;

  void remove(HashedKey hk) override;

  void reset() override;

  void persist(RecordWriter& rw) override;

  void recover(RecordReader& rr) override;

  void getCounters(const CounterVisitor& visitor) const override;

  AccessStats getAccessStats(HashedKey hk) const;

 private:
  // Each access map is guarded by its own lock. We shard 1K ways.
  static constexpr size_t kNumLocks = (2 << 10);
  // Specify 1 second window size for quantile estimator.
  static constexpr std::chrono::seconds kQuantileWindowSize{1};
  // We track up to 5 reinsertion windows. This should be sufficient
  // to understand how hit rate decays overtime.
  static constexpr size_t kReinsertionWindows = 5;

  const uint8_t hitsThreshold_{};

  std::array<folly::F14FastMap<uint64_t, AccessStats>, kNumLocks> accessMaps_;
  mutable std::array<std::mutex, kNumLocks> locks_;

  mutable AtomicCounter itemsEvictedWithNoAccess_;

  mutable util::PercentileStats hitsEstimator_{kQuantileWindowSize};
  mutable util::PercentileStats reinsertionEstimator_{kQuantileWindowSize};
  mutable util::PercentileStats hitsOnReinsertionEstimator_{
      kQuantileWindowSize};

  // Track up to kReinsertionWindows-worth's hits. This tells us how items'
  // popularity decay over time.
  mutable std::array<util::PercentileStats, kReinsertionWindows>
      hitsDecayEstimator_{util::PercentileStats{kQuantileWindowSize},
                          util::PercentileStats{kQuantileWindowSize},
                          util::PercentileStats{kQuantileWindowSize},
                          util::PercentileStats{kQuantileWindowSize},
                          util::PercentileStats{kQuantileWindowSize}};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
