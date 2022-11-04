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

#include <set>
#include <unordered_map>

#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {

// structure to query the stats corresponding to an AllocationClass.
struct ACStats {
  // the allocation size for this allocation class
  uint32_t allocSize;

  // number of allocations per slab
  unsigned long long allocsPerSlab;

  // number of slabs that are currently used for active allocations.
  unsigned long long usedSlabs;

  // number of free slabs in this allocation class.
  unsigned long long freeSlabs;

  // number of freed allocations in this allocation class.
  unsigned long long freeAllocs;

  // number of active allocations in this class.
  unsigned long long activeAllocs;

  // true if the allocation class is full.
  bool full;

  constexpr unsigned long long totalSlabs() const noexcept {
    return freeSlabs + usedSlabs;
  }

  constexpr size_t getTotalFreeMemory() const noexcept {
    return Slab::kSize * freeSlabs + freeAllocs * allocSize;
  }
};

// structure to query stats corresponding to a MemoryPool
struct MPStats {
  // set of allocation class ids in this
  std::set<ClassId> classIds;

  // stats for all the allocation classes belonging to this pool
  std::unordered_map<ClassId, ACStats> acStats;

  // total number of free slabs in this memory pool that are not allocated to
  // any allocation class, but have been acquired from the slab allocator.
  unsigned long long freeSlabs;

  // number of slabs in this pool can still allocate from the slab allocator.
  unsigned long long slabsUnAllocated;

  // number of slabs released for resizing the pool.
  unsigned int numSlabResize;

  // number of slabs released for rebalancing the pool.
  unsigned int numSlabRebalance;

  // number of slabs advised away currently for the pool.
  uint64_t numSlabAdvise;

  // number of slabs under this memory pool allocated from the slab allocator.
  // this includes freeSlabs.
  unsigned long long allocatedSlabs() const noexcept {
    long long total = 0;
    for (const auto& a : acStats) {
      total += a.second.totalSlabs();
    }
    total += freeSlabs;
    return total;
  }

  uint64_t numFreeAllocs() const noexcept {
    uint64_t freeAllocs = 0;
    for (const auto& ac : acStats) {
      freeAllocs += ac.second.freeAllocs;
    }
    return freeAllocs;
  }

  // total amount of memory that is in the free lists and unallocated and
  // unused slabs.
  size_t freeMemory() const noexcept {
    size_t ret = 0;
    for (const auto& a : acStats) {
      ret += a.second.getTotalFreeMemory();
    }
    return ret + (slabsUnAllocated + freeSlabs) * Slab::kSize;
  }
};
} // namespace cachelib
} // namespace facebook
