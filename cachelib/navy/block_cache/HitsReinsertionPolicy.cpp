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
  hitsDecayEstimator_[hitsIdx].addValue(ka.currHits);

  if (ka.currHits < hitsThreshold_) {
    return false;
  }
  hitsOnReinsertionEstimator_.addValue(ka.currHits);
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
    hitsEstimator_.addValue(ka.totalHits);
    reinsertionEstimator_.addValue(ka.numReinsertions);
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
  serialization::AccessTrackerSet accessTrackerSet;
  for (size_t i = 0; i < accessMaps_.size(); i++) {
    std::lock_guard<std::mutex> l{locks_[i % kNumLocks]};
    serialization::AccessTracker trackerData;
    for (auto& kv : accessMaps_[i]) {
      serialization::AccessStats access;
      access.totalHits = static_cast<int8_t>(kv.second.totalHits);
      access.currHits = static_cast<int8_t>(kv.second.currHits);
      access.numReinsertions = static_cast<int8_t>(kv.second.numReinsertions);
      trackerData.data[kv.first] = access;
    }
    accessTrackerSet.trackers.push_back(std::move(trackerData));
  }
  serializeProto(accessTrackerSet, rw);
}

void HitsReinsertionPolicy::recover(RecordReader& rr) {
  auto accessTrackerSet = deserializeProto<serialization::AccessTrackerSet>(rr);
  if (accessMaps_.size() != accessTrackerSet.trackers.size()) {
    throw std::invalid_argument(
        folly::sformat("Access Map Size Mismatch. Expected: {}, Actual: {}.",
                       accessMaps_.size(),
                       accessTrackerSet.trackers.size()));
  }

  for (size_t i = 0; i < accessTrackerSet.trackers.size(); i++) {
    const auto& trackerData = accessTrackerSet.trackers[i];
    auto& map = accessMaps_[i];
    for (auto& kv : trackerData.data) {
      AccessStats access;
      access.totalHits = static_cast<uint8_t>(kv.second.totalHits);
      access.currHits = static_cast<uint8_t>(kv.second.currHits);
      access.numReinsertions = static_cast<uint8_t>(kv.second.numReinsertions);
      map[kv.first] = access;
    }
  }
}

void HitsReinsertionPolicy::getCounters(const CounterVisitor& visitor) const {
  auto visitQuantileEstimator = [](const CounterVisitor& v,
                                   folly::SlidingWindowQuantileEstimator<>& qe,
                                   folly::StringPiece prefix) {
    qe.flush();
    static const std::array<const char*, 8> kStatNames{
        "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"};
    static const std::array<double, 8> kQuantiles{0.05, 0.1, 0.25, 0.5,
                                                  0.75, 0.9, 0.95, 0.99};
    auto q = qe.estimateQuantiles(
        folly::Range<const double*>(kQuantiles.begin(), kQuantiles.end()));
    assert(q.quantiles.size() == kQuantiles.size());
    for (size_t i = 0; i < kQuantiles.size(); i++) {
      auto p = folly::sformat("navy_bc_item_{}_{}", prefix, kStatNames[i]);
      v(p, q.quantiles[i].second);
    }
  };
  visitQuantileEstimator(visitor, hitsEstimator_, "hits");
  visitQuantileEstimator(visitor, reinsertionEstimator_, "reinsertions");
  visitQuantileEstimator(
      visitor, hitsOnReinsertionEstimator_, "reinsertion_hits");
  for (size_t i = 0; i < hitsDecayEstimator_.size(); i++) {
    auto name = folly::sformat("reinsertion_hits_{}", i);
    visitQuantileEstimator(visitor, hitsDecayEstimator_[i], name);
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
