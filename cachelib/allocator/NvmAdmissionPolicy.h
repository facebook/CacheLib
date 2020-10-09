#pragma once

#include <folly/Range.h>
#include "cachelib/common/ApproxSplitSet.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"

namespace facebook {
namespace cachelib {

template <typename Cache>
class NvmAdmissionPolicy {
 public:
  using Item = typename Cache::Item;
  using ChainedItemIter = typename Cache::ChainedItemIter;
  virtual ~NvmAdmissionPolicy() = default;

  // The method that the outside class calls to get the admission decision.
  // It captures the common logics (e.g statistics) then
  // delicates the detailed implementation to subclasses.
  virtual bool accept(const Item& item,
                      folly::Range<ChainedItemIter> chainItem) final {
    util::LatencyTracker overallTracker(overallLatency_);
    overallCount_.inc();
    const bool decision = acceptImpl(item, chainItem);
    if (decision) {
      accepted_.inc();
    } else {
      rejected_.inc();
    }
    return decision;
  }

  // The method that exposes statuses.
  virtual std::unordered_map<std::string, double> getCounters() final {
    auto ctrs = getCountersImpl();
    ctrs["ap.called"] = overallCount_.get();
    ctrs["ap.accepted"] = accepted_.get();
    ctrs["ap.rejected"] = rejected_.get();
    util::LatencyTracker::visitLatencyStatsUs(
        ctrs, overallLatency_, "ap.latency");
    return ctrs;
  }

 protected:
  // Implement this method for the detailed admission decision logic.
  virtual bool acceptImpl(const Item&, folly::Range<ChainedItemIter>) = 0;
  // Implementation specific statistics.
  // Please include a prefix/postfix with the name of implementation to avoid
  // collision with base level stats.
  virtual std::unordered_map<std::string, double> getCountersImpl() = 0;

 private:
  util::PercentileStats overallLatency_;
  AtomicCounter overallCount_{0};
  AtomicCounter accepted_{0};
  AtomicCounter rejected_{0};
};

template <typename Cache>
class RejectFirstAP : public NvmAdmissionPolicy<Cache> {
 public:
  using Item = typename Cache::Item;
  using ChainedItemIter = typename Cache::ChainedItemIter;
  RejectFirstAP(uint64_t numEntries,
                uint32_t numSplits,
                size_t suffixIgnoreLength,
                bool useDramHitSignal)
      : tracker_{numEntries, numSplits},
        suffixIgnoreLength_{suffixIgnoreLength},
        useDramHitSignal_{useDramHitSignal} {}

 protected:
  virtual bool acceptImpl(const Item& it,
                          folly::Range<ChainedItemIter>) final override {
    const bool wasDramHit =
        useDramHitSignal_ && it.getLastAccessTime() > it.getCreationTime();

    auto key = it.getKey();
    size_t len = key.size() > suffixIgnoreLength_
                     ? key.size() - suffixIgnoreLength_
                     : key.size();
    uint64_t keyHash = folly::hash::SpookyHashV2::Hash64(key.data(), len, 0);

    // if key already existed, we want to admit. If we are inserting it for
    // the first time, we insert and track.
    bool seenBefore = tracker_.insert(keyHash);
    if (!seenBefore && wasDramHit) {
      admitsByDramHits_.inc();
    }

    return seenBefore || wasDramHit;
  }

  virtual std::unordered_map<std::string, double> getCountersImpl()
      final override {
    std::unordered_map<std::string, double> ctrs;
    ctrs["ap.reject_first_keys_tracked"] = tracker_.numKeysTracked();
    ctrs["ap.reject_first_tracking_window_secs"] =
        tracker_.trackingWindowDurationSecs();
    ctrs["ap.reject_first_admits_by_dram_hit"] = admitsByDramHits_.get();
    return ctrs;
  }

 private:
  ApproxSplitSet tracker_;
  const size_t suffixIgnoreLength_;
  AtomicCounter admitsByDramHits_{0};
  const bool useDramHitSignal_{true};
};
} // namespace cachelib
} // namespace facebook
