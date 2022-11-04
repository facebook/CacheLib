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

#include "cachelib/navy/block_cache/FifoPolicy.h"

#include <folly/Format.h>

#include <numeric>

namespace facebook {
namespace cachelib {
namespace navy {
namespace detail {
unsigned int accumulate(const std::vector<unsigned int> nums) {
  return std::accumulate(
      nums.begin(), nums.end(), 0u, [](unsigned int a, unsigned int b) {
        if (b == 0) {
          throw std::invalid_argument(
              folly::sformat("Expected non-zero element. Actual: {}", b));
        }
        return a + b;
      });
}
} // namespace detail

FifoPolicy::FifoPolicy() { XLOG(INFO, "FIFO policy"); }

void FifoPolicy::track(const Region& region) {
  std::lock_guard<std::mutex> lock{mutex_};
  queue_.push_back(detail::Node{region.id(), getSteadyClockSeconds()});
}

RegionId FifoPolicy::evict() {
  std::lock_guard<std::mutex> lock{mutex_};
  if (queue_.empty()) {
    return RegionId{};
  }
  auto rid = queue_.front().rid;
  queue_.pop_front();
  return rid;
}

void FifoPolicy::reset() {
  std::lock_guard<std::mutex> lock{mutex_};
  queue_.clear();
}

void FifoPolicy::persist(RecordWriter& rw) const {
  serialization::FifoPolicyData fifoPolicyData;
  fifoPolicyData.queue()->resize(queue_.size());

  for (uint32_t i = 0; i < queue_.size(); i++) {
    auto& proto = (*fifoPolicyData.queue())[i];
    proto.idx() = queue_[i].rid.index();
    proto.trackTime() = queue_[i].trackTime.count();
  }

  serializeProto(fifoPolicyData, rw);
}

void FifoPolicy::recover(RecordReader& rr) {
  auto fifoPolicyData = deserializeProto<serialization::FifoPolicyData>(rr);
  queue_.clear();
  for (uint32_t i = 0; i < fifoPolicyData.queue().value().size(); i++) {
    const auto& proto = fifoPolicyData.queue().value()[i];
    queue_.push_back(
        detail::Node{RegionId{static_cast<uint32_t>(proto.idx().value())},
                     std::chrono::seconds{proto.trackTime().value()}});
  }
}

void FifoPolicy::getCounters(const CounterVisitor& v) const {
  std::lock_guard<std::mutex> lock{mutex_};
  v(folly::sformat("navy_bc_fifo_size"), queue_.size());
  v(folly::sformat("navy_bc_fifo_age"),
    queue_.empty() ? 0 : queue_.front().secondsSinceTracking().count());
}

SegmentedFifoPolicy::SegmentedFifoPolicy(std::vector<unsigned int> segmentRatio)
    : segmentRatio_{std::move(segmentRatio)},
      totalRatioWeight_{detail::accumulate(segmentRatio_)},
      segments_{segmentRatio_.size()} {
  if (segments_.empty()) {
    throw std::invalid_argument("Cannot initialize SFIFO without any segments");
  }
  XDCHECK_GT(totalRatioWeight_, 0u);
}

void SegmentedFifoPolicy::track(const Region& region) {
  auto priority = region.getPriority();
  XDCHECK_LT(priority, segments_.size());
  std::lock_guard<std::mutex> lock{mutex_};
  segments_[priority].push_back(
      detail::Node{region.id(), getSteadyClockSeconds()});
  rebalanceLocked();
}

RegionId SegmentedFifoPolicy::evict() {
  std::lock_guard<std::mutex> lock{mutex_};
  auto& lowestPri = segments_.front();
  if (lowestPri.empty()) {
    XDCHECK_EQ(0ul, numElementsLocked());
    return RegionId{};
  }
  auto rid = lowestPri.front().rid;
  lowestPri.pop_front();
  rebalanceLocked();
  return rid;
}

void SegmentedFifoPolicy::rebalanceLocked() {
  auto regionsTracked = numElementsLocked();

  // Rebalance from highest-pri segment to lowest-pri segment. This means the
  // lowest-pri segment can grow to far larger than its ratio suggests. This
  // is okay, as we only need higher-pri segments for items that are deemed
  // important.
  // e.g. {[a, b, c], [d], [e]} is a valid state for a SFIFO with 3 segments
  //      and a segment ratio of [1, 1, 1]
  auto currSegment = segments_.rbegin();
  auto currSegmentRatio = segmentRatio_.rbegin();
  auto nextSegment = std::next(currSegment);
  while (nextSegment != segments_.rend()) {
    auto currSegmentLimit =
        regionsTracked * *currSegmentRatio / totalRatioWeight_;
    while (currSegmentLimit < currSegment->size()) {
      nextSegment->push_back(currSegment->front());
      currSegment->pop_front();
    }

    currSegment = nextSegment;
    currSegmentRatio++;
    nextSegment++;
  }
}

size_t SegmentedFifoPolicy::numElementsLocked() {
  return std::accumulate(
      segments_.begin(),
      segments_.end(),
      0ul,
      [](size_t size, const auto& segment) { return size + segment.size(); });
}

void SegmentedFifoPolicy::reset() {
  std::lock_guard<std::mutex> lock{mutex_};
  for (auto& segment : segments_) {
    segment.clear();
  }
}

size_t SegmentedFifoPolicy::memorySize() const {
  size_t memSize = sizeof(*this);
  std::lock_guard<std::mutex> lock{mutex_};
  for (const auto& segment : segments_) {
    memSize += sizeof(std::deque<detail::Node>) +
               sizeof(detail::Node) * segment.size();
  }
  return memSize;
}

void SegmentedFifoPolicy::getCounters(const CounterVisitor& v) const {
  int idx = 0;
  std::lock_guard<std::mutex> lock{mutex_};
  for (auto& segment : segments_) {
    v(folly::sformat("navy_bc_sfifo_segment_{}_size", idx), segment.size());
    v(folly::sformat("navy_bc_sfifo_segment_{}_age", idx),
      segment.empty() ? 0 : segment.front().secondsSinceTracking().count());
    idx++;
  }
}

void SegmentedFifoPolicy::persist(RecordWriter& rw) const {
  std::ignore = rw;
  throw std::runtime_error("Not Implemented.");
}

void SegmentedFifoPolicy::recover(RecordReader& rr) {
  std::ignore = rr;
  throw std::runtime_error("Not Implemented");
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
