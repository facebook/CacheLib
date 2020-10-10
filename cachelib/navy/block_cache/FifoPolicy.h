#pragma once

#include <deque>
#include <mutex>
#include <set>

#include "cachelib/navy/block_cache/EvictionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Simple FIFO policy
class FifoPolicy final : public EvictionPolicy {
 public:
  FifoPolicy();
  FifoPolicy(const FifoPolicy&) = delete;
  FifoPolicy& operator=(const FifoPolicy&) = delete;
  ~FifoPolicy() override = default;

  void touch(RegionId /* rid */) override {}
  void track(const Region& region) override;
  RegionId evict() override;
  void reset() override;

  size_t memorySize() const override {
    std::lock_guard<std::mutex> lock{mutex_};
    return sizeof(*this) + sizeof(RegionId) * queue_.size();
  }

  void getCounters(const CounterVisitor&) const override {}

 private:
  std::deque<RegionId> queue_;
  mutable std::mutex mutex_;
};

// Segmented FIFO policy
//
// It divides the fifo queue into N segments. Each segment holds
// number of items proportional to its segment ratio. For example,
// if we have 3 segments and the ratio of [2, 1, 1], the lowest
// priority segment will hold 50% of the items whereas the other
// two higher priority segments will hold 25% each.
//
// On insertion, a priority is used as an Insertion Point. E.g. a pri-2
// region will be inserted into the third highest priority segment. After
// the insertion is completed, we will trigger rebalance, where this
// region may be moved to below the insertion point, if the segment it
// was originally inserted into had exceeded the size allowed by its ratio.
//
// Our rebalancing scheme allows the lowest priority segment to grow beyond
// its ratio allows for, since there is no lower segment to move into.
//
// Also note that rebalancing is also triggered after an eviction.
//
// The rate of inserting new regions look like the following:
// Pri-2 ---
//          \
// Pri-1 -------
//              \
// Pri-0 ------------
// When pri-2 exceeds its ratio, it effectively downgrades the oldest region in
// pri-2 to pri-1, and that region is now pushed down at the combined rate of
// (new pri-1 regions + new pri-2 regions), so effectively it gets evicted out
// of the system faster once it is beyond the pri-2 segment ratio. Segment
// ratio is put in place to prevent the lower segments getting so small a
// portion of the flash device.
class SegmentedFifoPolicy final : public EvictionPolicy {
 public:
  // @segmentRatio  ratio of the size of each segment. Size of this param also
  //                indicates the number of segments in the sfifo. This cannot
  //                be empty.
  explicit SegmentedFifoPolicy(std::vector<unsigned int> segmentRatio);
  SegmentedFifoPolicy(const SegmentedFifoPolicy&) = delete;
  SegmentedFifoPolicy& operator=(const SegmentedFifoPolicy&) = delete;
  ~SegmentedFifoPolicy() override = default;

  void touch(RegionId /* rid */) override {}
  void track(const Region& region) override;
  RegionId evict() override;
  void reset() override;

  size_t memorySize() const override;

  void getCounters(const CounterVisitor& v) const override;

 private:
  struct Node {
    const RegionId rid{};
    // Indicate when this region was initially tracked
    const std::chrono::seconds trackTime{};

    std::chrono::seconds secondsSinceTracking() const {
      return getSteadyClockSeconds() - trackTime;
    }
  };

  void rebalanceLocked();
  size_t numElementsLocked();

  // Following are used to compute the ratio of each segment's
  // size. The formula is as follows:
  //  (total_segments * (segment's ratio / sum(ratio))
  const std::vector<unsigned int> segmentRatio_;
  const unsigned int totalRatioWeight_;

  std::vector<std::deque<Node>> segments_;
  mutable std::mutex mutex_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
