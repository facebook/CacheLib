#pragma once

#include <folly/Range.h>
#include "cachelib/common/ApproxSplitSet.h"

namespace facebook {
namespace cachelib {

template <typename Cache>
class NvmAdmissionPolicy {
 public:
  using Item = typename Cache::Item;
  using ChainedItemIter = typename Cache::ChainedItemIter;
  virtual ~NvmAdmissionPolicy() = default;
  virtual bool accept(const Item&, folly::Range<ChainedItemIter>) = 0;
  virtual std::unordered_map<std::string, double> getCounters() = 0;
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

  virtual bool accept(const Item& it,
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
      ++admitsByDramHits_;
    }

    return seenBefore || wasDramHit;
  }

  virtual std::unordered_map<std::string, double> getCounters() final override {
    std::unordered_map<std::string, double> ctrs;
    ctrs["nvm_reject_first_keys_tracked"] = tracker_.numKeysTracked();
    ctrs["nvm_reject_first_tracking_window_secs"] =
        tracker_.trackingWindowDurationSecs();
    ctrs["nvm_reject_first_admits_by_dram_hit"] = admitsByDramHits_;
    return ctrs;
  }

 private:
  ApproxSplitSet tracker_;
  const size_t suffixIgnoreLength_;
  std::atomic<uint64_t> admitsByDramHits_{0};
  const bool useDramHitSignal_{true};
};
} // namespace cachelib
} // namespace facebook
