#pragma once

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"

namespace facebook {
namespace cachelib {
class ReleaseStats {
 public:
  using SlabReleaseEventsBuffer = std::deque<SlabReleaseData>;

  ReleaseStats() {}
  ReleaseStats(const ReleaseStats&) = delete;
  ReleaseStats& operator=(const ReleaseStats& other) = delete;

  void addSlabReleaseEvent(const ClassId from,
                           const ClassId to,
                           const uint64_t elapsedTime,
                           const PoolId pid,
                           const unsigned int numSlabsInVictim,
                           const unsigned int numSlabsInReceiver,
                           const uint32_t victimAllocSize,
                           const uint32_t receiverAllocSize,
                           const uint64_t victimEvictionAge,
                           const uint64_t receiverEvictionAge,
                           const uint64_t numFreeAllocsInVictim);

  SlabReleaseEvents getSlabReleaseEvents(const PoolId pid) const;

 private:
  // max number of events kept in buffer before being consumed by
  // getSlabReleaseEvents. once the capacity is reached, oldest recors are
  // kicked out.
  static constexpr size_t kMaxThreshold{600};

  // buffer per pool,  holding the events added so far.
  std::array<SlabReleaseEventsBuffer, MemoryPoolManager::kMaxPools>
      slabReleaseEventsBuffer_{};

  // current sequence number of the slab release. this is monotonically bumped
  // up everytime a slab release event is added
  mutable uint64_t currentSequenceNum_{1};

  mutable std::mutex lock_;
};
} // namespace cachelib
} // namespace facebook
