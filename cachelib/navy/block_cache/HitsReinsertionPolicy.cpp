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

// TODO: T95755384 delete persist API
void HitsReinsertionPolicy::persist(RecordWriter& rw) {
  // disable future recover by writing kNumLocks empty AccessTrackers
  serializeProto(kNumLocks, rw);
  for (size_t i = 0; i < kNumLocks; i++) {
    serialization::AccessTracker trackerData;
    serializeProto(trackerData, rw);
  }
}

// TODO: T95755384 delete recover API
void HitsReinsertionPolicy::recover(RecordReader& rr) {
  XDCHECK(index_);

  size_t numTrackers = deserializeProto<size_t>(rr);

  for (size_t i = 0; i < numTrackers; i++) {
    const auto& trackerData =
        deserializeProto<serialization::AccessTracker>(rr);

    XDCHECK(trackerData.deprecated_data_ref()->empty());

    for (auto& kv : *trackerData.data_ref()) {
      auto stats = kv.stats_ref();
      auto key = kv.key_ref();
      auto totalHits = static_cast<uint8_t>(*stats->totalHits_ref());
      auto currHits = static_cast<uint8_t>(*stats->currHits_ref());
      index_->setHits(*key, currHits, totalHits);
    }
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
