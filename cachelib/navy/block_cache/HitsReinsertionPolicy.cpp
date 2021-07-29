#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"

#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {
namespace navy {
HitsReinsertionPolicy::HitsReinsertionPolicy(uint8_t hitsThreshold)
    : hitsThreshold_{hitsThreshold} {}

bool HitsReinsertionPolicy::shouldReinsert(HashedKey hk) {
  XDCHECK(index_);
  const auto lr = index_->peek(hk.keyHash());
  if (!lr.found() || lr.currentHits() < hitsThreshold_) {
    return false;
  }

  hitsOnReinsertionEstimator_.trackValue(lr.currentHits());
  return true;
}

// TODO: T95755384 delete persist API after BigCache has rolled out the release
// that no longer tries to "recover" the persistence policy.
void HitsReinsertionPolicy::persist(RecordWriter& rw) {
  // disable future recover by writing kNumLocks empty AccessTrackers
  serializeProto(kNumLocks, rw);
  for (size_t i = 0; i < kNumLocks; i++) {
    serialization::AccessTracker trackerData;
    serializeProto(trackerData, rw);
  }
}

void HitsReinsertionPolicy::getCounters(const CounterVisitor& visitor) const {
  hitsOnReinsertionEstimator_.visitQuantileEstimator(
      visitor, "navy_bc_item_reinsertion_hits");
}

HitsReinsertionPolicy::AccessStats HitsReinsertionPolicy::getAccessStats(
    HashedKey hk) const {
  XDCHECK(index_);
  const auto lr = index_->peek(hk.keyHash());
  if (lr.found()) {
    return AccessStats{lr.totalHits(), lr.currentHits()};
  }
  return {};
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
