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

#include <cstdint>
#include <cstring>
#include <set>
#include <stdexcept>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/futures/Future.h>
#pragma GCC diagnostic pop
#include <cachelib/allocator/KAllocation.h>
#include <cachelib/allocator/memory/MemoryAllocator.h>
#include <cachelib/allocator/memory/Slab.h>

namespace facebook {
namespace cachelib {
namespace util {

// inserts the IOBuf and its chain into the cache by copying the bufffers into
// bunch of parent and child items. The chain can be accessed by calling
// find() on the parent key. If there is already an allocation with the same
// key, it is replaced.
//
// @param cache     the cache to make the allocations from
// @param pid       the pool id for allocation
// @param key       the key for the allocation
// @param bufs      the chain of IOBufs to insert into the cache
// @param ttlSecs   the ttl for the allocation.
//
// @return  true if the chain was successfully inserted inot cache. false if
//          there was any failure allocating memory.
template <typename T>
bool insertIOBufInCache(T& cache,
                        PoolId pid,
                        typename T::Key key,
                        const folly::IOBuf& bufs,
                        uint32_t ttlSecs = 0) {
  auto parent = cache.allocate(pid, key, bufs.length(), ttlSecs);
  if (!parent) {
    return false;
  }

  std::memcpy(parent->getMemory(), bufs.data(), bufs.length());

  if (bufs.isChained()) {
    auto itr = bufs.begin();
    ++itr; // we already processed the first in chain
    while (itr != bufs.end()) {
      auto alloc = cache.allocateChainedItem(parent, itr->size());
      if (!alloc) {
        // this will release the parent all its existing children.
        return false;
      }

      // copy data
      // TODO (sathya) use the extra space in this allocation to sneak more
      // bits from the next IOBuf ?
      std::memcpy(alloc->getMemory(), itr->data(), itr->size());

      cache.addChainedItem(parent, std::move(alloc));
      XDCHECK(parent->hasChainedItem());
      ++itr;
    }
  }
  cache.insertOrReplace(parent);
  return true;
}

// Allocates and inserts an item in the cache without initializing the memory.
// Typically used for tests where we dont have synchronous readers/writers.
//
// @param cache     the cache to make the allocations from
// @param poolId    the pool id for allocation
// @param key       the key for the allocation
// @param size      the size of the allocation
// @param ttlSecs   Time To Live (second) for the item,
//                  default with 0 means no expiration time
//
// @return      the handle for the item or an invalid handle(nullptr) if the
//              allocation/insertion failed.
template <typename T>
typename T::WriteHandle allocateAccessible(T& cache,
                                           PoolId poolId,
                                           typename T::Item::Key key,
                                           uint32_t size,
                                           uint32_t ttlSecs = 0) {
  auto allocHandle = cache.allocate(poolId, key, size, ttlSecs);
  if (!allocHandle) {
    return typename T::WriteHandle{};
  }

  const auto inserted = cache.insert(allocHandle);
  if (!inserted) {
    // this will destroy the allocated handle and release it back to the
    // allocator.
    return typename T::WriteHandle{};
  }

  return allocHandle;
}

// Allocates and inserts an item in the cache without initializing the memory
// via insertOrReplace.
// Typically used for tests where we dont have synchronous readers/writers.
//
// @param cache     the cache to make the allocations from
// @param poolId    the pool id for allocation
// @param key       the key for the allocation
// @param size      the size of the allocation
// @param ttlSecs   Time To Live (second) for the item,
//                  default with 0 means no expiration time
//
// @return      the handle for the item or an invalid handle(nullptr) if the
//              allocation failed.
template <typename T>
typename T::WriteHandle allocateAndReplace(T& cache,
                                           PoolId poolId,
                                           typename T::Item::Key key,
                                           uint32_t size,
                                           uint32_t ttlSecs = 0) {
  auto allocHandle = cache.allocate(poolId, key, size, ttlSecs);
  if (!allocHandle) {
    return typename T::WriteHandle{};
  }

  cache.insertOrReplace(allocHandle);

  return allocHandle;
}

// Convenience method: cast a type with standard layout as a key
template <typename T>
folly::StringPiece castToKey(const T& type) noexcept {
  static_assert(std::is_standard_layout<T>::value,
                "Key must be standard layout");
  return folly::StringPiece{reinterpret_cast<const char*>(&type), sizeof(T)};
}

// Returns item's fragmentation size caused by the bad allocation.
// this should be equal to alloc_size - required_size.
//
// @param item    reference of the cache
// @param item    reference of the item
// @return        the memory fragmentation size of this item.

template <typename T, typename U>
uint32_t getFragmentation(const T& cache, const U& item) {
  return cache.getUsableSize(item) - item.getSize();
}

// helper function to generate the allocation sizes for addPool()
inline std::set<uint32_t> generateAllocSizes(
    double allocationClassSizeFactor,
    uint32_t maxAllocationClassSize = Slab::kSize,
    uint32_t minAllocationClassSize = 72,
    bool reduceFragmentationInAllocationClass = false) {
  return MemoryAllocator::generateAllocSizes(
      allocationClassSizeFactor,
      maxAllocationClassSize,
      minAllocationClassSize,
      reduceFragmentationInAllocationClass);
}

// Helper function to generate allocation sizes as powers of 2.
// This provides a simple, uniform distribution of allocation sizes where each
// size is double the previous one. This can be useful when you want predictable
// allocation class sizes, when your workload's size distribution aligns well
// with power-of-2 buckets, or as a reasonable default/safeguard when you don't
// have detailed knowledge of your item size distribution.
//
// @param minPowerOf2  The minimum power of 2 for allocation sizes
//                     (must be >= Slab::kMinAllocPower, i.e., >= 6 for 64
//                     bytes)
// @param maxPowerOf2  The maximum power of 2 for allocation sizes
//                     (must be <= Slab::kNumSlabBits, i.e., <= 22 for 4MB)
//
// @return  A set of allocation sizes where each size is 2^i for i in
//          [minPowerOf2, maxPowerOf2]. For example, minPowerOf2=6 and
//          maxPowerOf2=10 generates {64, 128, 256, 512, 1024}.
//
// @throw std::invalid_argument if minPowerOf2 > maxPowerOf2
// @throw std::invalid_argument if minPowerOf2 < Slab::kMinAllocPower
// @throw std::invalid_argument if maxPowerOf2 > Slab::kNumSlabBits
inline std::set<uint32_t> generateAllocSizesPowerOf2(uint32_t minPowerOf2,
                                                     uint32_t maxPowerOf2) {
  if (minPowerOf2 > maxPowerOf2) {
    throw std::invalid_argument("minPowerOf2 must be <= maxPowerOf2");
  }
  if (minPowerOf2 < Slab::kMinAllocPower) {
    throw std::invalid_argument(folly::sformat(
        "minPowerOf2 must be >= {} because allocation size must be >= {}",
        Slab::kMinAllocPower,
        Slab::kMinAllocSize));
  }
  if (maxPowerOf2 > Slab::kNumSlabBits) {
    throw std::invalid_argument(
        folly::sformat("maxPowerOf2 must be <= {} because allocation size "
                       "must be <= {} (Slab::kSize)",
                       Slab::kNumSlabBits,
                       Slab::kSize));
  }

  std::set<uint32_t> allocSizes;
  for (uint32_t i = minPowerOf2; i <= maxPowerOf2; i++) {
    allocSizes.insert(1 << i);
  }
  return allocSizes;
}

// Returns set of allocation sizes that are chosen in a way to minimize worst
// case cache efficiency loss. Efficiency loss for an item size is defined in
// the following way: If item size is s, x is the assigned allocation size and
// y is the optimal allocation size, then efficiency loss is: (4MB/y) /
// (4MB/x)
// - 4MB/y is the number of allocations per slab using the optimal
// allocation size
// - 4MB/x is the number of allocations per slab using the
// assigned allocation size
// Efficiency loss tells us how many more items we could have stored if we had
// used the optimal allocation size. These allocation sizes try to provide a
// good default for any cache
inline std::set<uint32_t> genAllocClassesTuned() {
  return {64,     72,     80,     88,      104,     120,     136,    152,
          168,    184,    200,    216,     232,     248,     264,    296,
          328,    360,    392,    424,     456,     488,     520,    552,
          584,    616,    648,    712,     776,     848,     912,    992,
          1088,   1168,   1232,   1304,    1416,    1488,    1608,   1680,
          1832,   1928,   2064,   2216,    2312,    2504,    2624,   2792,
          2968,   3120,   3328,   3584,    3912,    4160,    4440,   4632,
          4904,   5096,   5384,   5848,    6120,    6416,    6752,   7120,
          7528,   7984,   8504,   9096,    9776,    10560,   11488,  12016,
          12592,  13224,  13928,  14712,   15592,   16576,   17696,  18976,
          20456,  22192,  24240,  25416,   26712,   28144,   29744,  31536,
          33552,  35848,  38472,  41520,   45096,   49344,   54464,  57456,
          60784,  64520,  68752,  73584,   79136,   85592,   93200,  102296,
          113352, 127096, 135296, 144624,  155344,  167768,  182360, 199728,
          220752, 246720, 279616, 322632,  381296,  419424,  466032, 524288,
          599184, 699048, 838856, 1048576, 1398096, 2097152, 4194304};
}

// Calculates curr - prev, returning 0 if curr is less than prev
inline uint64_t safeDiff(const uint64_t prev, const uint64_t curr) {
  return curr > prev ? curr - prev : 0;
}

// Calculates hit ratio, based on total operations and total misses
// @param ops total operations
// @param miss operations that missed
inline double hitRatioCalc(uint64_t ops, uint64_t miss) {
  return ops == 0 ? 0.0 : 100.0 - 100.0 * miss / ops;
}

} // namespace util
} // namespace cachelib
} // namespace facebook
