#include <folly/logging/xlog.h>

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
HitsReinsertionPolicy::HitsReinsertionPolicy(uint8_t hitsThreshold)
    : hitsThreshold_{hitsThreshold} {}

void HitsReinsertionPolicy::touch(HashedKey hk) {
  XDCHECK(index_);
  index_->touch(hk.keyHash());
}

bool HitsReinsertionPolicy::shouldReinsert(HashedKey hk) {
  XDCHECK(index_);
  const auto lr = index_->lookup(hk.keyHash());
  if (!lr.found() || lr.currentHits() < hitsThreshold_) {
    return false;
  }

  hitsOnReinsertionEstimator_.trackValue(lr.currentHits());
  index_->setCurrentHits(hk.keyHash(), 0);
  return true;
}

void HitsReinsertionPolicy::remove(HashedKey hk) {
  XDCHECK(index_);
  const auto lr = index_->lookup(hk.keyHash());
  if (lr.found()) {
    hitsEstimator_.trackValue(lr.totalHits());
    if (lr.totalHits() == 0) {
      itemsEvictedWithNoAccess_.inc();
    }
  }
}

void HitsReinsertionPolicy::persist(RecordWriter& rw) {
  // disable future recover by writing kNumLocks empty AccessTrackers
  serializeProto(kNumLocks, rw);
  for (size_t i = 0; i < kNumLocks; i++) {
    serialization::AccessTracker trackerData;
    serializeProto(trackerData, rw);
  }
}

void HitsReinsertionPolicy::recover(RecordReader& rr) {
  XDCHECK(index_);

  size_t numTrackers = deserializeProto<size_t>(rr);

  for (size_t i = 0; i < numTrackers; i++) {
    const auto& trackerData =
        deserializeProto<serialization::AccessTracker>(rr);

    // For compatibility, remove after BigCache release 145 is out.
    // Deprecated data shouldn't contain anything since we do not have any
    // bigcache host running on Navy for release 144 or prior releases.
    XDCHECK(trackerData.deprecated_data_ref()->empty());

    for (auto& kv : trackerData.data) {
      auto stats = kv.stats_ref();
      auto key = kv.key_ref();
      auto totalHits = static_cast<uint8_t>(*stats->totalHits_ref());
      auto currHits = static_cast<uint8_t>(*stats->currHits_ref());
      index_->setTotalHits(*key, totalHits);
      index_->setCurrentHits(*key, currHits);
    }
  }
}

void HitsReinsertionPolicy::getCounters(const CounterVisitor& visitor) const {
  hitsEstimator_.visitQuantileEstimator(visitor, "navy_bc_item_{}_{}", "hits");
  hitsOnReinsertionEstimator_.visitQuantileEstimator(
      visitor, "navy_bc_item_{}_{}", "reinsertion_hits");
  visitor("navy_bc_item_evicted_with_no_access",
          itemsEvictedWithNoAccess_.get());
}

HitsReinsertionPolicy::AccessStats HitsReinsertionPolicy::getAccessStats(
    HashedKey hk) const {
  XDCHECK(index_);
  const auto lr = index_->lookup(hk.keyHash());
  if (lr.found()) {
    return AccessStats{lr.totalHits(), lr.currentHits()};
  }
  return {};
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
