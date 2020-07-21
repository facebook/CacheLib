#include <folly/logging/xlog.h>

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
// Return val + 1 if no overflow, otherwise return val
uint8_t safeInc(uint8_t val) {
  if (val < std::numeric_limits<uint8_t>::max()) {
    return val + 1;
  }
  return val;
}
} // namespace
constexpr size_t HitsReinsertionPolicy::kNumLocks;
constexpr std::chrono::seconds HitsReinsertionPolicy::kQuantileWindowSize;
constexpr size_t HitsReinsertionPolicy::kReinsertionWindows;

HitsReinsertionPolicy::HitsReinsertionPolicy(uint8_t hitsThreshold)
    : hitsThreshold_{hitsThreshold} {}

void HitsReinsertionPolicy::track(HashedKey hk) {
  const size_t idx = hk.keyHash() % kNumLocks;
  std::lock_guard<std::mutex> l{locks_[idx]};
  accessMaps_[idx][hk.keyHash()] = AccessStats{};
}

void HitsReinsertionPolicy::touch(HashedKey hk) {
  const size_t idx = hk.keyHash() % kNumLocks;
  std::lock_guard<std::mutex> l{locks_[idx]};
  auto itr = accessMaps_[idx].find(hk.keyHash());
  if (itr == accessMaps_[idx].end()) {
    return;
  }
  auto& ka = itr->second;
  ka.totalHits = safeInc(ka.totalHits);
  ka.currHits = safeInc(ka.currHits);
}

bool HitsReinsertionPolicy::shouldReinsert(HashedKey hk) {
  const size_t idx = hk.keyHash() % kNumLocks;
  std::lock_guard<std::mutex> l{locks_[idx]};
  auto itr = accessMaps_[idx].find(hk.keyHash());
  if (itr == accessMaps_[idx].end()) {
    return false;
  }

  auto& ka = itr->second;
  const size_t hitsIdx = std::min(static_cast<size_t>(ka.numReinsertions),
                                  hitsDecayEstimator_.size() - 1);
  hitsDecayEstimator_[hitsIdx].trackValue(ka.currHits);

  if (ka.currHits < hitsThreshold_) {
    return false;
  }
  hitsOnReinsertionEstimator_.trackValue(ka.currHits);
  ka.currHits = 0;
  ka.numReinsertions = safeInc(ka.numReinsertions);
  return true;
}

void HitsReinsertionPolicy::remove(HashedKey hk) {
  const size_t idx = hk.keyHash() % kNumLocks;
  std::lock_guard<std::mutex> l{locks_[idx]};
  auto itr = accessMaps_[idx].find(hk.keyHash());
  if (itr != accessMaps_[idx].end()) {
    auto& ka = itr->second;
    hitsEstimator_.trackValue(ka.totalHits);
    reinsertionEstimator_.trackValue(ka.numReinsertions);
    if (ka.totalHits == 0) {
      itemsEvictedWithNoAccess_.inc();
    }
    accessMaps_[idx].erase(itr);
  }
}

void HitsReinsertionPolicy::reset() {
  for (size_t i = 0; i < accessMaps_.size(); i++) {
    std::lock_guard<std::mutex> l{locks_[i % kNumLocks]};
    accessMaps_[i].clear();
  }
}

void HitsReinsertionPolicy::persist(RecordWriter& rw) {
  serializeProto(size_t(accessMaps_.size()), rw);
  for (size_t i = 0; i < accessMaps_.size(); i++) {
    std::lock_guard<std::mutex> l{locks_[i % kNumLocks]};
    serialization::AccessTracker trackerData;
    trackerData.data_ref()->reserve(accessMaps_[i].size());
    for (auto& kv : accessMaps_[i]) {
      serialization::AccessStatsPair pair;
      *pair.key_ref() = static_cast<int64_t>(kv.first);
      *pair.stats_ref()->totalHits_ref() =
          static_cast<int8_t>(kv.second.totalHits);
      *pair.stats_ref()->currHits_ref() =
          static_cast<int8_t>(kv.second.currHits);
      *pair.stats_ref()->numReinsertions_ref() =
          static_cast<int8_t>(kv.second.numReinsertions);
      trackerData.data_ref()->push_back(std::move(pair));
    }
    serializeProto(trackerData, rw);
  }
}

void HitsReinsertionPolicy::recover(RecordReader& rr) {
  size_t numTrackers = deserializeProto<size_t>(rr);
  if (accessMaps_.size() != numTrackers) {
    throw std::invalid_argument(
        folly::sformat("Access Map Size Mismatch. Expected: {}, Actual: {}.",
                       accessMaps_.size(),
                       numTrackers));
  }

  for (size_t i = 0; i < numTrackers; i++) {
    const auto& trackerData =
        deserializeProto<serialization::AccessTracker>(rr);
    auto& map = accessMaps_[i];

    // For compatibility, remove after BigCache release 145 is out.
    // Deprecated data shouldn't contain anything since we do not have any
    // bigcache host running on Navy for release 144 or prior releases.
    XDCHECK(trackerData.deprecated_data_ref()->empty());

    for (auto& kv : *trackerData.data_ref()) {
      AccessStats access;
      access.totalHits = static_cast<uint8_t>(*kv.stats_ref()->totalHits_ref());
      access.currHits = static_cast<uint8_t>(*kv.stats_ref()->currHits_ref());
      access.numReinsertions =
          static_cast<uint8_t>(*kv.stats_ref()->numReinsertions_ref());
      map[static_cast<uint64_t>(*kv.key_ref())] = access;
    }
  }
}

void HitsReinsertionPolicy::getCounters(const CounterVisitor& visitor) const {
  hitsEstimator_.visitQuantileEstimator(visitor, "navy_bc_item_{}_{}", "hits");
  reinsertionEstimator_.visitQuantileEstimator(
      visitor, "navy_bc_item_{}_{}", "reinsertions");
  hitsOnReinsertionEstimator_.visitQuantileEstimator(
      visitor, "navy_bc_item_{}_{}", "reinsertion_hits");
  for (size_t i = 0; i < hitsDecayEstimator_.size(); i++) {
    auto name = folly::sformat("reinsertion_hits_{}", i);
    hitsDecayEstimator_[i].visitQuantileEstimator(
        visitor, "navy_bc_item_{}_{}", name);
  }
  visitor("navy_bc_item_evicted_with_no_access",
          itemsEvictedWithNoAccess_.get());
}

HitsReinsertionPolicy::AccessStats HitsReinsertionPolicy::getAccessStats(
    HashedKey hk) const {
  const size_t idx = hk.keyHash() % kNumLocks;
  std::lock_guard<std::mutex> l{locks_[idx]};
  auto itr = accessMaps_[idx].find(hk.keyHash());
  if (itr != accessMaps_[idx].end()) {
    return itr->second;
  } else {
    return {};
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
