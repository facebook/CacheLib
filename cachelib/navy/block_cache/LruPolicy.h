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

  void touch(RegionId rid) override;
  void track(RegionId rid) override;
  RegionId evict() override;
  void reset() override;
  size_t memorySize() const override;
  void getCounters(const CounterVisitor& v) const override;

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
