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

#include "cachelib/allocator/memory/CompressedPtr.h"
#include "cachelib/allocator/memory/tests/TestBase.h"

using namespace facebook::cachelib;
using namespace facebook::cachelib::tests;

using CompressedPtrTest = SlabAllocatorTestBase;
using Config = typename MemoryAllocator::Config;

std::vector<int> getRandomNumbers(int N, int max) {
  // Create a random number generator
  std::random_device rd;
  std::mt19937 gen(rd());
  // Create a uniform distribution for the range [0, max]
  std::uniform_int_distribution<int> dis(0, max);
  // Generate N random numbers
  std::vector<int> randomNumbers;
  for (int i = 0; i < N; ++i) {
    randomNumbers.push_back(dis(gen));
  }
  return randomNumbers;
}

TEST_F(CompressedPtrTest, Stats) {
  auto compressedPtr = CompressedPtr4B();
  auto compressedPtr5b = CompressedPtr5B();

  // The 5 byte pointer can address 64 TB of memory compared
  // to 256 GB for the 4 byte pointer.
  ASSERT_EQ(compressedPtr.getMaxAddressableSize(),
            (size_t)256 * 1024 * 1024 * 1024 /* 256 GB */);
  ASSERT_EQ(compressedPtr5b.getMaxAddressableSize(),
            (size_t)32 * 1024 * 1024 * 1024 * 1024 /* 32 TB */);

  // Number of bits used to encode the allocation class should be the same.
  ASSERT_EQ(getCompressedPtrNumAllocIdxBits<CompressedPtr4B>(),
            getCompressedPtrNumAllocIdxBits<CompressedPtr5B>());

  // 4 byte pointer uses 16 bits (15 fot multi-tiering) to encode slab index.
  ASSERT_EQ(
      getCompressedPtrNumSlabIdxBits<CompressedPtr4B>(compressedPtr, false),
      16);
  ASSERT_EQ(
      getCompressedPtrNumSlabIdxBits<CompressedPtr4B>(compressedPtr, true), 15);

  // 5 byte pointer uses 23 bits (22 fot multi-tiering) to encode slab index.
  ASSERT_EQ(
      getCompressedPtrNumSlabIdxBits<CompressedPtr5B>(compressedPtr5b, false),
      23);
  ASSERT_EQ(
      getCompressedPtrNumSlabIdxBits<CompressedPtr5B>(compressedPtr5b, true),
      22);

  // The values returned by compress should be the same for 4 byte
  // and 5 byte compressed pointer for slab and alloc index referring
  // to allocations between 0-256GB.
  // Evaluating 1000 random allocation indexes for 1000 unique slab indexes
  // so a total of 1 million slab and allocation index combinations.
  uint16_t numCompare = 1000;
  for (uint32_t curSlabIdx :
       getRandomNumbers(numCompare, std::numeric_limits<uint16_t>::max())) {
    for (uint32_t curAllocIdx :
         getRandomNumbers(numCompare, std::numeric_limits<uint16_t>::max())) {
      ASSERT_EQ(compress<CompressedPtr4B>(
                    compressedPtr, curSlabIdx, curAllocIdx, false),
                compress<CompressedPtr5B>(
                    compressedPtr5b, curSlabIdx, curAllocIdx, false));
    }
  }

  uint32_t slabIdx = std::numeric_limits<uint16_t>::max();
  // Compressed pointer should fail when asking to compress a slab index
  // max(uint16_t)
  EXPECT_DEATH(compress<CompressedPtr4B>(compressedPtr, slabIdx, 0, false), "");

  // Large compressed pointer should be fine when asking to compress a slab
  // index max(uint16_t).
  ASSERT_EQ(compress<CompressedPtr5B>(compressedPtr5b, slabIdx, 0, false),
            static_cast<uint64_t>(
                slabIdx << getCompressedPtrNumAllocIdxBits<CompressedPtr5B>()));

  // This is max value of 23 bits so it should fail for 5 byte compressed
  // pointer.
  slabIdx = (1u << getCompressedPtrNumSlabIdxBits<CompressedPtr5B>(
                 compressedPtr5b, false)) -
            1;
  EXPECT_DEATH(compress<CompressedPtr5B>(compressedPtr5b, slabIdx, 0, false),
               "");

  // Reducing 1 from slab index should work for 5 byte compressed pointer.
  EXPECT_EQ(compress<CompressedPtr5B>(compressedPtr5b, slabIdx - 1, 0, false),
            (slabIdx - 1) << 16);
  EXPECT_EQ(
      compress<CompressedPtr5B>(compressedPtr5b, slabIdx - 1, 1, false),
      ((slabIdx - 1) << getCompressedPtrNumAllocIdxBits<CompressedPtr5B>()) +
          1);

  // Trying the max slab index with max allocation index with 5 byte compressed
  // pointer.
  EXPECT_EQ(
      compress<CompressedPtr5B>(compressedPtr5b,
                                slabIdx - 1,
                                std::numeric_limits<uint16_t>::max(),
                                false),
      ((slabIdx - 1) << getCompressedPtrNumAllocIdxBits<CompressedPtr5B>()) +
          std::numeric_limits<uint16_t>::max());
}

TEST_F(CompressedPtrTest, Compare) {
  const unsigned int numClasses = 10;
  const unsigned int numPools = 4;
  // create enough memory for 4 pools with 5 allocation classes each and 5 slabs
  // each for each allocation class.
  const size_t poolSize = numClasses * 5 * Slab::kSize;
  // allocate enough memory for all the pools plus slab headers.
  const size_t totalSize = numPools * poolSize + 2 * Slab::kSize;
  void* memory = allocate(totalSize);
  // ensure that the allocation sizes are compatible for pointer compression.
  auto allocSizes = getRandomAllocSizes(numClasses, getMinAllocSize());
  MemoryAllocator m(
      getDefaultMemoryAllocatorConfig(allocSizes), memory, totalSize);

  std::unordered_map<PoolId, const std::set<uint32_t>> pools;
  for (unsigned int i = 0; i < numPools; i++) {
    auto nClasses = folly::Random::rand32() % numClasses + 1;
    auto sizes = getRandomAllocSizes(nClasses, getMinAllocSize());
    auto pid = m.addPool(getRandomStr(), poolSize, sizes);
    ASSERT_NE(pid, Slab::kInvalidPoolId);
    pools.insert({pid, sizes});
  }

  ASSERT_EQ(pools.size(), numPools);

  using PoolAllocs = std::unordered_map<PoolId, std::vector<void*>>;
  PoolAllocs poolAllocs;
  auto makeAllocsOutOfPool = [&m, &poolAllocs, &pools](PoolId pid) {
    const auto& sizes = pools[pid];
    std::vector<void*> allocs;
    unsigned int numAllocations = 0;
    do {
      uint32_t prev = getMinAllocSize();
      numAllocations = 0;
      for (const auto size : sizes) {
        const auto range = prev == size ? 1 : size - prev;
        uint32_t randomSize = folly::Random::rand32() % range + prev;
        void* alloc = m.allocate(pid, randomSize);
        if (alloc != nullptr) {
          allocs.push_back(alloc);
          numAllocations++;
        }
        prev = size + 1;
      }
    } while (numAllocations > 0);
    poolAllocs[pid] = allocs;
  };

  for (const auto& pool : pools) {
    makeAllocsOutOfPool(pool.first);
  }

  // now we have a list of allocations across all the pools. go through them
  // and ensure that they do well with pointer compression.
  for (const auto& pool : poolAllocs) {
    const auto& allocs = pool.second;
    for (const auto* alloc : allocs) {
      CompressedPtr4B ptr =
          m.compress<CompressedPtr4B>(alloc, false /* isMultiTiered */);
      CompressedPtr5B ptr5b =
          m.compress<CompressedPtr5B>(alloc, false /* isMultiTiered */);

      // Both pointer should not be null
      ASSERT_FALSE((ptr.isNull()) || (ptr5b.isNull()));

      // Both pointer types should return the same raw value when uncompressed
      ASSERT_EQ(ptr.getRaw(), ptr5b.getRaw());

      // Both pointers should return the input value when compressed and
      // uncompressed immediately.
      ASSERT_EQ(alloc,
                m.unCompress<CompressedPtr4B>(ptr, false /* isMultiTiered */));
      ASSERT_EQ(
          alloc,
          m.unCompress<CompressedPtr5B>(ptr5b, false /* isMultiTiered */));
    }
  }

  // Return nullptr when compressing nullptr.
  ASSERT_EQ(nullptr,
            m.unCompress<CompressedPtr4B>(
                m.compress<CompressedPtr4B>(nullptr, false /* isMultiTiered */),
                false /* isMultiTiered */));
  ASSERT_EQ(nullptr,
            m.unCompress<CompressedPtr5B>(
                m.compress<CompressedPtr5B>(nullptr, false /* isMultiTiered */),
                false /* isMultiTiered */));

  // test pointer compression with multi-tier
  for (const auto& pool : poolAllocs) {
    const auto& allocs = pool.second;
    for (const auto* alloc : allocs) {
      CompressedPtr4B ptr =
          m.compress<CompressedPtr4B>(alloc, true /* isMultiTiered */);
      CompressedPtr5B ptr5b =
          m.compress<CompressedPtr5B>(alloc, true /* isMultiTiered */);
      ASSERT_FALSE(ptr.isNull());
      ASSERT_FALSE(ptr5b.isNull());
      ASSERT_EQ(alloc,
                m.unCompress<CompressedPtr4B>(ptr, true /* isMultiTiered */));
      ASSERT_EQ(alloc,
                m.unCompress<CompressedPtr5B>(ptr5b, true /* isMultiTiered */));
    }
  }

  ASSERT_EQ(nullptr,
            m.unCompress(
                m.compress<CompressedPtr4B>(nullptr, true /* isMultiTiered */),
                true /* isMultiTiered */));
  ASSERT_EQ(nullptr,
            m.unCompress(
                m.compress<CompressedPtr5B>(nullptr, true /* isMultiTiered */),
                true /* isMultiTiered */));
}
