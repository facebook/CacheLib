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

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"

namespace facebook {
namespace cachelib {
// Stats for slab releases
class ReleaseStats {
 public:
  using SlabReleaseEventsBuffer = std::deque<SlabReleaseData>;

  ReleaseStats() {}
  ReleaseStats(const ReleaseStats&) = delete;
  ReleaseStats& operator=(const ReleaseStats& other) = delete;

  // add slab release event
  // @param from              the victim allocation class
  // @param to                the receiver allocation class.
  // @param elapsedTime       time took for the cache to release the slab given
  //                          the victim and the receiver
  // @param pid               pool id
  // @param numSlabsInVictim  number of slabs in the victim class after the
  // event.
  // @param numSlabsInReceiver number of slabs in the receiver class after the
  // event.
  // @param victimAllocSize   victim class allocation size.
  // @param receiverAllocSize receiver class allocation size.
  // @param victimEvictionAge eviction age of victim class defined by the
  // lifetime of the oldest item.
  // @param receiverEvictionAge     eviction age of receiver class defined by
  // the lifetime of the oldest item.
  // @param numFreeAllocsInVictim   number of free allocations in the victim
  // class after the event.
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

  // @return slab release events of the given pool.
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
