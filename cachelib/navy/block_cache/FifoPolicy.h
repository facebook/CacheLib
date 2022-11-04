/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <deque>
#include <mutex>
#include <set>

#include "cachelib/navy/block_cache/EvictionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace detail {
struct Node {
  const RegionId rid{};
  // Indicate when this region was initially tracked
  const std::chrono::seconds trackTime{};

  std::chrono::seconds secondsSinceTracking() const {
    return getSteadyClockSeconds() - trackTime;
  }
};
} // namespace detail

// Simple FIFO policy
class FifoPolicy final : public EvictionPolicy {
 public:
  FifoPolicy();
  FifoPolicy(const FifoPolicy&) = delete;
  FifoPolicy& operator=(const FifoPolicy&) = delete;
  ~FifoPolicy() override = default;

  void touch(RegionId /* rid */) override {}

  // Adds a new region to the queue for tracking.
  void track(const Region& region) override;

  // Evicts the first added region and stops tracking.
  RegionId evict() override;

  // Resets FIFO policy to the initial state.
  void reset() override;

  // Gets memory used by FIFO policy.
  size_t memorySize() const override {
    std::lock_guard<std::mutex> lock{mutex_};
    return sizeof(*this) + sizeof(detail::Node) * queue_.size();
  }

  // Exports FIFO policy stats via CounterVisitor.
  void getCounters(const CounterVisitor& v) const override;

  // Persists metadata associated with FIFO policy.
  void persist(RecordWriter& rw) const override;

  // Recovers from previously persisted metadata associated with FIFO policy.
  void recover(RecordReader& rr) override;

 private:
  std::deque<detail::Node> queue_;
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

  // Adds a new region to the segments for tracking.
  void track(const Region& region) override;

  // Evicts the region with the lowest priority and stops tracking.
  RegionId evict() override;

  // Resets Segmented FIFO policy to the initial state.
  void reset() override;

  // Gets memory used by Segmented FIFO policy.
  size_t memorySize() const override;

  // Exports Segmented FIFO policy stats via CounterVisitor.
  void getCounters(const CounterVisitor& v) const override;

  // Persists metadata associated with segmented FIFO policy.
  void persist(RecordWriter& rw) const override;

  // Recovers from previously persisted metadata associated with segmented FIFO
  // policy.
  void recover(RecordReader& rr) override;

 private:
  void rebalanceLocked();
  size_t numElementsLocked();

  // Following are used to compute the ratio of each segment's
  // size. The formula is as follows:
  //  (total_segments * (segment's ratio / sum(ratio))
  const std::vector<unsigned int> segmentRatio_;
  const unsigned int totalRatioWeight_;

  std::vector<std::deque<detail::Node>> segments_;
  mutable std::mutex mutex_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
