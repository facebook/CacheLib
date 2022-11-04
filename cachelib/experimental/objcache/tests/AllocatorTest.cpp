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

#include <gtest/gtest.h>

#include <vector>

#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/tests/Common.h"

namespace facebook {
namespace cachelib {
namespace objcache {
namespace test {
namespace {
template <typename T>
using Alloc = Allocator<T, TestAllocatorResource>;

template <typename T, typename... U>
using ScopedAlloc = std::scoped_allocator_adaptor<Alloc<T>, Alloc<U>...>;
} // namespace

TEST(Allocator, Propogation) {
  // This test is to ensure our allocator behaves as expected per our
  // propogation rules. I.e. no propagation allowed for copy-assignment,
  // move-assignment, and swap. And "move" is forced into a "copy" when
  // allocators do not compare equal.
  using TestVector = std::vector<int, Allocator<int, TestAllocatorResource>>;

  Allocator<int, TestAllocatorResource> allocator1{TestAllocatorResource{"1"}};
  Allocator<int, TestAllocatorResource> allocator2{TestAllocatorResource{"2"}};
  Allocator<int, TestAllocatorResource> allocator3{TestAllocatorResource{"1"}};

  TestVector vec1{allocator1};
  TestVector vec2{allocator1};
  TestVector vec3{allocator1};
  TestVector vec4{allocator2};
  TestVector vec5{allocator3};

  vec1.push_back(1);
  EXPECT_EQ(1, allocator1.getAllocatorResource().getNumAllocs());
  vec1.push_back(2);
  EXPECT_EQ(2, allocator1.getAllocatorResource().getNumAllocs());

  // Copy assignment we expect to see another allocation
  vec2 = vec1;
  EXPECT_EQ(3, allocator1.getAllocatorResource().getNumAllocs());

  // Move assignment with same allocator, so we'll move
  vec3 = std::move(vec1);
  EXPECT_EQ(3, allocator1.getAllocatorResource().getNumAllocs());
  EXPECT_EQ(0, vec1.size());
  EXPECT_EQ(2, vec3.size());

  // Move assignment with different and unequal allocator, so we'll copy
  vec4 = std::move(vec2);
  EXPECT_EQ(1, allocator2.getAllocatorResource().getNumAllocs());
  // vec2 is still cleared since it was "moved" even tho we forced a copy
  EXPECT_EQ(0, vec2.size());

  // Move assignment with a different but equal allocator, so we'll move
  vec5 = std::move(vec3);
  EXPECT_EQ(0, allocator3.getAllocatorResource().getNumAllocs());
  EXPECT_EQ(0, vec3.size());
  EXPECT_EQ(2, vec5.size());
}

TEST(Allocator, ScopedAllocatorWithVector) {
  // This test is to ensure scoped_allocator_adaptor works as expected when
  // used with our flavor of allocator. The expected behavior is that all
  // containers aware of scoped_allocator_adaptor will forward the allocator
  // to its inner member.

  {
    // If our ScopedAlloc only has a single allocator, it will be used
    // for all inner members.
    using InnerVector = std::vector<int, Alloc<int>>;
    using Vector = std::vector<InnerVector, ScopedAlloc<InnerVector>>;
    ScopedAlloc<InnerVector> alloc{
        Alloc<InnerVector>{TestAllocatorResource{"1"}}};
    Vector vec{alloc};
    vec = {{1, 2}, {3, 4}};

    EXPECT_EQ("1", vec.get_allocator().getAllocatorResource().getName());
    auto vecItr = vec.begin();
    ASSERT_NE(vecItr, vec.end());
    for (; vecItr != vec.end(); vecItr++) {
      EXPECT_EQ("1", vecItr->get_allocator().getAllocatorResource().getName());
    }
  }

  {
    // If a type has all "ScopedAlloc" type, then using a single allocator
    // ScopedAlloc will correctly forward the allocator to all levels
    using InnerInnerVector = std::vector<int, Alloc<int>>;
    using InnerVector =
        std::vector<InnerInnerVector, ScopedAlloc<InnerInnerVector>>;
    using Vector = std::vector<InnerVector, ScopedAlloc<InnerVector>>;
    ScopedAlloc<InnerVector> alloc{
        Alloc<InnerVector>{TestAllocatorResource{"1"}}};
    Vector vec{alloc};
    vec = {{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}};

    EXPECT_EQ("1", vec.get_allocator().getAllocatorResource().getName());
    auto vecItr = vec.begin();
    ASSERT_NE(vecItr, vec.end());
    for (; vecItr != vec.end(); vecItr++) {
      EXPECT_EQ("1", vecItr->get_allocator().getAllocatorResource().getName());
      auto vecItr2 = vecItr->begin();
      ASSERT_NE(vecItr2, vecItr->end());
      for (; vecItr2 != vecItr->end(); vecItr2++) {
        EXPECT_EQ("1",
                  vecItr2->get_allocator().getAllocatorResource().getName());
      }
    }
  }

  {
    // This verifies the behavior that with a two-level container and a
    // ScopeAlloc with two allocators. "1" will be used for the top level,
    // and "2" will be used for the lower level.
    using InnerVector = std::vector<int, Alloc<int>>;
    using Vector = std::vector<InnerVector, ScopedAlloc<InnerVector, int>>;
    ScopedAlloc<InnerVector, int> alloc{
        Alloc<InnerVector>{TestAllocatorResource{"1"}},
        Alloc<int>{TestAllocatorResource{"2"}}};
    Vector vec{alloc};
    vec = {{1, 2}, {3, 4}};

    EXPECT_EQ("1", vec.get_allocator().getAllocatorResource().getName());
    auto vecItr = vec.begin();
    ASSERT_NE(vecItr, vec.end());
    for (; vecItr != vec.end(); vecItr++) {
      EXPECT_EQ("2", vecItr->get_allocator().getAllocatorResource().getName());
    }
  }

  {
    // This verifies the behavior that with a three-level container and a
    // ScopeAlloc with three allocators. "1" will be used for the top level,
    // "2" will be used for the second level, and "3" for the last level.
    using InnerInnerVector = std::vector<int, Alloc<int>>;
    using ScopedAllocInner = ScopedAlloc<InnerInnerVector, int>;
    using InnerVector = std::vector<InnerInnerVector, ScopedAllocInner>;
    using ScopedAllocOuter = ScopedAlloc<InnerVector, InnerInnerVector, int>;
    using Vector = std::vector<InnerVector, ScopedAllocOuter>;
    ScopedAllocOuter alloc{Alloc<InnerVector>{TestAllocatorResource{"1"}},
                           Alloc<InnerInnerVector>{TestAllocatorResource{"2"}},
                           Alloc<int>{TestAllocatorResource{"3"}}};
    Vector vec{alloc};
    vec = {{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}};

    EXPECT_EQ("1", vec.get_allocator().getAllocatorResource().getName());
    auto vecItr = vec.begin();
    ASSERT_NE(vecItr, vec.end());
    for (; vecItr != vec.end(); vecItr++) {
      EXPECT_EQ("2", vecItr->get_allocator().getAllocatorResource().getName());
      auto vecItr2 = vecItr->begin();
      ASSERT_NE(vecItr2, vecItr->end());
      for (; vecItr2 != vecItr->end(); vecItr2++) {
        EXPECT_EQ("3",
                  vecItr2->get_allocator().getAllocatorResource().getName());
      }
    }
  }
}

TEST(Allocator, ScopedAllocatorWithMap) {
  // This test is to ensure scoped_allocator_adaptor works as expected when
  // used with our flavor of allocator. We test using a map where each level
  // has multiple different element types.

  {
    // If our ScopedAlloc only has a single allocator, it will be used
    // for all inner members
    using InnerVector = std::vector<int, Alloc<int>>;
    using Vector = std::vector<InnerVector, ScopedAlloc<InnerVector>>;
    using NestedContainer =
        std::map<InnerVector, Vector, std::less<InnerVector>,
                 ScopedAlloc<std::pair<const InnerVector, Vector>>>;
    ScopedAlloc<std::pair<const InnerVector, Vector>> alloc{
        Alloc<std::pair<const InnerVector, Vector>>{
            TestAllocatorResource{"1"}}};
    NestedContainer nc{alloc};
    nc[{1, 2, 3}] = {{4, 5, 6}, {7, 8, 9}};

    EXPECT_EQ("1", nc.get_allocator().getAllocatorResource().getName());

    auto itr = nc.begin();
    ASSERT_NE(itr, nc.end());
    EXPECT_EQ("1", itr->first.get_allocator().getAllocatorResource().getName());
    EXPECT_EQ("1",
              itr->second.get_allocator().getAllocatorResource().getName());

    auto vecItr = itr->second.begin();
    ASSERT_NE(vecItr, itr->second.end());
    for (; vecItr != itr->second.end(); vecItr++) {
      EXPECT_EQ("1", vecItr->get_allocator().getAllocatorResource().getName());
    }
  }

  {
    // This verifies the behavior that with a three-level container and a
    // ScopeAlloc with three allocators. "1" will be used for the top level,
    // "2" will be used for the second level, and "3" for the last level.
    using InnerVector = std::vector<int, Alloc<int>>;
    using Vector = std::vector<InnerVector, ScopedAlloc<InnerVector, int>>;
    using NestedContainer = std::map<
        InnerVector, Vector, std::less<InnerVector>,
        ScopedAlloc<std::pair<const InnerVector, Vector>, InnerVector, int>>;
    ScopedAlloc<std::pair<const InnerVector, Vector>, InnerVector, int> alloc{
        Alloc<std::pair<const InnerVector, Vector>>{TestAllocatorResource{"1"}},
        Alloc<InnerVector>{TestAllocatorResource{"2"}},
        Alloc<int>{TestAllocatorResource{"3"}}};
    NestedContainer nc{alloc};
    nc[{1, 2, 3}] = {{4, 5, 6}, {7, 8, 9}};

    EXPECT_EQ("1", nc.get_allocator().getAllocatorResource().getName());

    auto itr = nc.begin();
    ASSERT_NE(itr, nc.end());
    EXPECT_EQ("2", itr->first.get_allocator().getAllocatorResource().getName());
    EXPECT_EQ("2",
              itr->second.get_allocator().getAllocatorResource().getName());

    auto vecItr = itr->second.begin();
    ASSERT_NE(vecItr, itr->second.end());
    for (; vecItr != itr->second.end(); vecItr++) {
      EXPECT_EQ("3", vecItr->get_allocator().getAllocatorResource().getName());
    }
  }
}

TEST(Allocator, Exception) {
  Allocator<int, TestAllocatorResource> myAllocator{
      TestAllocatorResource{"abc"}};
  myAllocator.getAllocatorResource().setThrow(true);
  EXPECT_THROW(myAllocator.allocate(1), exception::ObjectCacheAllocationError);
  EXPECT_THROW(myAllocator.deallocate(reinterpret_cast<int*>(&myAllocator), 1),
               exception::ObjectCacheDeallocationBadArgs);

  using TestVector = std::vector<int, Allocator<int, TestAllocatorResource>>;
  Allocator<int, TestAllocatorResource> allocator{
      TestAllocatorResource{"test"}};
  TestVector vec{allocator};

  vec.reserve(2);
  EXPECT_EQ(1, allocator.getAllocatorResource().getNumAllocs());
  vec.push_back(1);
  EXPECT_EQ(1, allocator.getAllocatorResource().getNumAllocs());

  allocator.getAllocatorResource().setThrow(true);
  // First allocation shouldn't throw, because we have storage for two slots.
  EXPECT_NO_THROW(vec.push_back(1));
  EXPECT_EQ(1, allocator.getAllocatorResource().getNumAllocs());
  // Now we would throw, because we need to allocate additional storage.
  EXPECT_THROW(vec.push_back(1), exception::ObjectCacheAllocationError);
  EXPECT_EQ(2, allocator.getAllocatorResource().getNumAllocs());

  allocator.getAllocatorResource().setThrow(false);
}

// Define shortcuts for testing MonotonicBufferResource
using Mbr = MonotonicBufferResource<CacheDescriptor<LruAllocator>>;
template <typename T>
using MbrAlloc = Allocator<T, Mbr>;
template <typename T>
using MbrVector = std::vector<T, MbrAlloc<T>>;

TEST(MonotonicBufferResource, SimpleAllocation) {
  // Not associated with cache. Use fallback
  Mbr fallbackMbr;
  auto* alloc = fallbackMbr.allocate(10, 1 /* alignment */);
  EXPECT_TRUE(alloc);
  // comment this out will trigger ASAN
  fallbackMbr.deallocate(alloc, 10, 1 /* alignment */);

  // Associated with a cache item
  auto cache = createCache();
  auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
      *cache, 0 /* poolId */, "my_alloc", 0 /* reserved bytes */,
      10 /* additional bytes */, 1 /* alignment */, 0 /* ttlSec */,
      0 /* creationTime */);
  EXPECT_FALSE(hdl->hasChainedItem());

  // First allocation uses the additional reserved bytes so we don't need
  // to allocate a chained item.
  mbr.allocate(10, 1 /* alignment */);
  EXPECT_FALSE(hdl->hasChainedItem());

  // This allocation will trigger a storage expansion (factor: 2x)
  mbr.allocate(10, 1 /* alignment */);
  EXPECT_TRUE(hdl->hasChainedItem());
  EXPECT_EQ(1, cache->viewAsChainedAllocs(hdl).computeChainLength());
  mbr.allocate(10, 1 /* alignment */);
  EXPECT_EQ(1, cache->viewAsChainedAllocs(hdl).computeChainLength());

  // We will trigger another storage expansion (factor: 2x)
  mbr.allocate(1, 1 /* alignment */);
  EXPECT_EQ(2, cache->viewAsChainedAllocs(hdl).computeChainLength());

  // Copying an allocator is fine and will still be associated with
  // the same item.
  auto mbr2 = mbr;
  mbr.allocate(1, 1 /* alignment */);
  EXPECT_EQ(2, cache->viewAsChainedAllocs(hdl).computeChainLength());
  EXPECT_TRUE(mbr.isEqual(mbr2));
  EXPECT_FALSE(mbr.isEqual(fallbackMbr));
}

TEST(MonotonicBufferResource, Alignment) {
  std::vector<size_t> alignments = {1, 2, 4, 8, 16};
  auto cache = createCache();
  for (int loops = 0; loops < 10; loops++) {
    for (auto alignment : alignments) {
      auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
          *cache, 0 /* poolId */, folly::sformat("key_{}", loops),
          100 /* reserved bytes */, 100 /* additional bytes */, alignment,
          0 /* ttlSec */, 0 /* creationTime */);

      uintptr_t reservedStorageStart = reinterpret_cast<uintptr_t>(
          Mbr::getReservedStorage(hdl->getMemory(), alignment));
      EXPECT_EQ(0, reservedStorageStart % alignment);
      EXPECT_EQ(reinterpret_cast<uintptr_t>(mbr.viewMetadata()->buffer),
                reservedStorageStart + 100);

      for (int i = 0; i < 100; i++) {
        void* alloc = mbr.allocate(10, alignment);
        EXPECT_EQ(0, reinterpret_cast<uintptr_t>(alloc) % alignment);
      }
      cache->insertOrReplace(hdl);
    }
  }
}

TEST(MonotonicBufferResource, VectorAllocation) {
  // Not associated with cache. Use fallback
  MbrVector<int> intVecFallback;
  for (int i = 0; i < 10; i++) {
    intVecFallback.push_back(i);
  }
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(i, intVecFallback[i]);
  }

  // Associated with a cache item
  auto cache = createCache();
  auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
      *cache, 0 /* poolId */, "my_alloc", 0 /* reserved bytes */,
      0 /* additional bytes */, 1 /* alignment */, 0 /* ttlSec */,
      0 /* creationTime */);
  MbrVector<int> intVec{MbrAlloc<int>{mbr}};
  for (int i = 0; i < 10; i++) {
    intVec.push_back(i);
  }
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(i, intVec[i]);
  }
  EXPECT_EQ(5, cache->viewAsChainedAllocs(hdl).computeChainLength());
}

TEST(MonotonicBufferResource, VectorAllocation2) {
  // Test allocating a vector only using cache memory
  auto cache = createCache();
  auto [hdl, mbr] = createMonotonicBufferResource<Mbr>(
      *cache, 0 /* poolId */, "my_alloc",
      sizeof(MbrVector<int>) /* reserved bytes */, 0 /* additional bytes */,
      1 /* alignment */, 0 /* ttlSec */, 0 /* creationTime */);
  auto* intVec = new (hdl->getMemoryAs<uint8_t>() + Mbr::metadataSize())
      MbrVector<int>(MbrAlloc<int>{mbr});
  for (int i = 0; i < 10; i++) {
    intVec->push_back(i);
  }
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(i, intVec->at(i));
  }
  EXPECT_EQ(5, cache->viewAsChainedAllocs(hdl).computeChainLength());
}
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook
