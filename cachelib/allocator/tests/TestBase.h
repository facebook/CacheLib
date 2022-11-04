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
#include <folly/Random.h>
#include <gtest/gtest.h>

#include <string>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/allocator/memory/SlabAllocator.h"
#include "cachelib/allocator/memory/tests/TestBase.h"

namespace facebook {
namespace cachelib {

class AllocationClass;
class MemoryPool;
class MemoryPoolManager;
class MemoryAllocator;

namespace tests {

// type for TYPED_TEST_CASE
// in tests, 0 means LruAllocator, 1 means Lru2QAllocator, 2 means
// TinyLFUAllocator, 4 is LruAllocatorSpinBuckets
typedef ::testing::Types<LruAllocator,
                         Lru2QAllocator,
                         TinyLFUAllocator,
                         LruAllocatorSpinBuckets>
    AllocatorTypes;

template <typename AllocatorT>
class AllocatorTest : public SlabAllocatorTestBase {
 public:
  AllocatorTest()
      : cacheDir_("/tmp/cachelib_lru_allocator_test" +
                  folly::to<std::string>(folly::Random::rand32())) {
    util::makeDir(cacheDir_);
  }

  ~AllocatorTest() override { util::removePath(cacheDir_); }

  // for the given lru allocator, figure out a random set of allocation sizes
  // that are valid with given key length. This is to ensure that using these
  // sizes with the keylen will never throw invalid allocation size.
  std::vector<uint32_t> getValidAllocSizes(AllocatorT& alloc,
                                           PoolId poolId,
                                           unsigned int nSizes,
                                           unsigned int keyLen);

  // given a pool, fills it up with allocations until it can no longer allocate
  // without evictions for all possible allocations.
  void fillUpPoolUntilEvictions(AllocatorT& alloc,
                                PoolId pid,
                                const std::vector<uint32_t>& sizes,
                                unsigned int keyLen);
  void fillUpOneSlab(AllocatorT& alloc,
                     PoolId poolId,
                     const uint32_t size,
                     unsigned int keyLen);

  // make allocations in the pool and ensure that they are only evictions and
  // up to totalAllocSize
  void ensureAllocsOnlyFromEvictions(AllocatorT& alloc,
                                     PoolId pid,
                                     const std::vector<uint32_t>& sizes,
                                     unsigned int keyLen,
                                     size_t totalAllocSize,
                                     bool check = true);

  // given a pool ensure that it can still allocate without any evictions.
  void testAllocWithoutEviction(AllocatorT& alloc,
                                PoolId poolId,
                                const std::vector<uint32_t>& sizes,
                                unsigned int keyLen);

  // generate a random key that is not present in the cache.
  std::string getRandomNewKey(AllocatorT& alloc, unsigned int keyLen);

  // given an allocator that is prepped for evictions, tries to estimate the
  // size of the lru by watching the keys that are evicted from it.
  void estimateLruSize(AllocatorT& alloc,
                       PoolId poolId,
                       size_t size,
                       size_t keyLen,
                       const std::set<std::string>& evictedKeys,
                       size_t& outLruSize);

  // fill up the pool with allocations and ensure that the evictions then cycle
  // through the lru and the lru is fixed in length.
  void testLruLength(AllocatorT& alloc,
                     PoolId poolId,
                     const std::vector<uint32_t>& sizes,
                     size_t keyLen,
                     const std::set<std::string>& evictedKeys);

  void runSerializationTest(typename AllocatorT::Config config);

  static void testInfoShmIsRemoved(typename AllocatorT::Config config);

  static void testShmIsRemoved(typename AllocatorT::Config config);

  static void testShmIsNotRemoved(typename AllocatorT::Config config);

  const std::string cacheDir_;

  std::string createFileForAllocator(size_t size);

 private:
  const std::string kShmInfoName = "cachelib_serialization";
  const size_t kShmInfoSize = 10 * 1024 * 1024; // 10 MB
};
} // namespace tests
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/tests/TestBase-inl.h"
