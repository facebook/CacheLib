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

#include "cachelib/allocator/SlabReleaseStats.h"

#include <folly/logging/xlog.h>

#include <stdexcept>
#include <thread>

namespace facebook {
namespace cachelib {

void ReleaseStats::addSlabReleaseEvent(const ClassId from,
                                       const ClassId to,
                                       const uint64_t elapsedTime,
                                       const PoolId pid,
                                       const unsigned int numSlabsInVictim,
                                       const unsigned int numSlabsInReceiver,
                                       const uint32_t victimAllocSize,
                                       const uint32_t receiverAllocSize,
                                       const uint64_t victimEvictionAge,
                                       const uint64_t receiverEvictionAge,
                                       const uint64_t numFreeAllocsInVictim) {
  std::lock_guard<std::mutex> l(lock_);
  XDCHECK_GE(kMaxThreshold, slabReleaseEventsBuffer_[pid].size());

  // If we are at capacity, delete the last element
  if (slabReleaseEventsBuffer_[pid].size() == kMaxThreshold) {
    slabReleaseEventsBuffer_[pid].pop_back();
  }

  slabReleaseEventsBuffer_[pid].push_front(
      {std::chrono::system_clock::now() /* timeOfRelease */, from, to,
       currentSequenceNum_, elapsedTime, pid, numSlabsInVictim,
       numSlabsInReceiver, victimAllocSize, receiverAllocSize,
       victimEvictionAge, receiverEvictionAge, numFreeAllocsInVictim});
  currentSequenceNum_++;
}

/* When logging data: use this to avoid duplicates */
SlabReleaseEvents ReleaseStats::getSlabReleaseEvents(const PoolId pid) const {
  /* extend the vector of capacity */
  SlabReleaseEvents res;
  std::lock_guard<std::mutex> l(lock_);

  res.reserve(slabReleaseEventsBuffer_[pid].size());

  if (!slabReleaseEventsBuffer_[pid].empty()) {
    /* Copy the data to the result vector */
    std::copy(std::begin(slabReleaseEventsBuffer_[pid]),
              std::end(slabReleaseEventsBuffer_[pid]),
              std::back_inserter(res));
  }
  return res;
}

} // namespace cachelib
} // namespace facebook
