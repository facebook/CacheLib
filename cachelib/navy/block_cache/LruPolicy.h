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

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
// LRU policy with optional deferred LRU insert
class LruPolicy final : public EvictionPolicy {
 public:
  // Constructs LRU policy.
  // @expectedNumRegions is a hint how many regions to expect in LRU.
  explicit LruPolicy(uint32_t expectedNumRegions);

  LruPolicy(const LruPolicy&) = delete;
  LruPolicy& operator=(const LruPolicy&) = delete;

  ~LruPolicy() override {}

  // Records the hit of the region.
  void touch(RegionId rid) override;

  // Adds a new region to the array for tracking.
  void track(const Region& region) override;

  // Evicts the least recently used region and stops tracking.
  RegionId evict() override;

  // Resets LRU policy to the initial state.
  void reset() override;

  // Gets memory used by LRU policy.
  size_t memorySize() const override;

  // Exports LRU policy stats via CounterVisitor.
  void getCounters(const CounterVisitor& v) const override;

  // Persists metadata associated with LRU policy.
  void persist(RecordWriter& rw) const override;

  // Recovers from previously persisted metadata associated with LRU policy.
  void recover(RecordReader& rr) override;

 private:
  static constexpr uint32_t kInvalidIndex = 0xffffffffu;

  // Double linked list with index as a pointer and kInvalidIndex as nullptr
  struct ListNode {
    uint32_t prev{kInvalidIndex};
    uint32_t next{kInvalidIndex};
    // seconds since epoch stamp of time events
    std::chrono::seconds creationTime{};
    std::chrono::seconds lastUpdateTime{};
    uint32_t hits{};

    bool inList() const {
      return prev != kInvalidIndex || next != kInvalidIndex;
    }

    std::chrono::seconds secondsSinceCreation() const {
      return getSteadyClockSeconds() - creationTime;
    }

    std::chrono::seconds secondsSinceAccess() const {
      return getSteadyClockSeconds() - lastUpdateTime;
    }
  };

  void unlink(uint32_t i);
  void linkAtHead(uint32_t i);
  void linkAtTail(uint32_t i);
  void dump(uint32_t n) const;
  void dumpList(const char* tag,
                uint32_t n,
                uint32_t first,
                uint32_t ListNode::*link) const;

  static constexpr std::chrono::seconds kEstimatorWindow{5};

  std::vector<ListNode> array_;
  uint32_t head_{kInvalidIndex};
  uint32_t tail_{kInvalidIndex};
  mutable std::mutex mutex_;

  // various counters that are populated when we evict a region.
  mutable util::PercentileStats secSinceInsertionEstimator_;
  mutable util::PercentileStats secSinceAccessEstimator_;
  mutable util::PercentileStats hitsEstimator_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
