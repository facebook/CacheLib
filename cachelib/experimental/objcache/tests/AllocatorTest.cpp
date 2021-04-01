#include <gtest/gtest.h>

#include <vector>

#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/tests/Common.h"

namespace facebook {
namespace cachelib {
namespace objcache {
namespace test {
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
      10 /* additional bytes */, 1 /* alignment */);
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
          100 /* reserved bytes */, 100 /* additional bytes */, alignment);

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
      0 /* additional bytes */, 1 /* alignment */);
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
      1 /* alignment */);
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
