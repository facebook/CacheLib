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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "cachelib/allocator/memory/AllocationClass.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/allocator/memory/SlabAllocator.h"
#include "cachelib/allocator/memory/tests/TestBase.h"
#include "cachelib/common/Serialization.h"

using namespace facebook::cachelib::tests;
using namespace facebook::cachelib;

using AllocationClassTest = SlabAllocatorTestBase;

constexpr size_t SerializationBufferSize = 100 * 1024;

namespace {
unsigned int forEachAllocationCount;
bool test_callback(void* /* unused */, AllocInfo /* unused */) {
  forEachAllocationCount++;
  return true;
}
} // namespace

namespace facebook {
namespace cachelib {

TEST_F(AllocationClassTest, Basic) {
  // create a dummy allocator to instantiate the AllocationClass.
  const auto slabAlloc = createSlabAllocator(2);
  const ClassId cid = 5;
  // test with a size not more than the slab size.
  const size_t allocSize = getRandomAllocSize();
  AllocationClass ac(cid, 0, allocSize, *slabAlloc);
  ASSERT_EQ(ac.getId(), cid);
  ASSERT_EQ(ac.getAllocSize(), allocSize);

  // trying to allocate without adding any slabs should fail.
  ASSERT_EQ(ac.allocate(), nullptr);
  ASSERT_GT(ac.getAllocsPerSlab(), 0);

  ASSERT_LE(ac.getAllocsPerSlab(), Slab::kSize);
}

TEST_F(AllocationClassTest, AllocFree) {
  // allocator with ~100 slabs.
  auto slabAlloc = createSlabAllocator(100);
  const unsigned int nUsable = slabAlloc->getNumUsableSlabs();
  std::vector<Slab*> freeSlabs;
  const PoolId pid = 0;
  const ClassId cid = 0;
  for (unsigned int i = 0; i < nUsable; i++) {
    auto slab = slabAlloc->makeNewSlab(pid);
    ASSERT_NE(slab, nullptr);
    freeSlabs.push_back(slab);
  }

  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);

  std::vector<Slab*> usedSlabs;
  auto grabFreeSlab = [&freeSlabs, &usedSlabs]() -> Slab* {
    assert(!freeSlabs.empty());
    Slab* s = freeSlabs.back();
    freeSlabs.pop_back();
    usedSlabs.push_back(s);
    return s;
  };

  // add a slab and ensure that we can make sufficient number of
  // allocations.
  auto slab = grabFreeSlab();
  ac.addSlab(slab);
  const unsigned int nPerSlab = ac.getAllocsPerSlab();
  std::vector<void*> allocations;
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }

  // must not be able to allocate.
  ASSERT_EQ(ac.allocate(), nullptr);
  ASSERT_TRUE(ac.isFull());

  // ensure that the allocations dont overlap by sorting them and seeing if
  // the distance of bytes between them is at least ac->getAllocSize.
  auto checkAllocsDontOverlap = [&allocations, &ac]() {
    auto tmp = allocations;
    std::sort(tmp.begin(), tmp.end());
    for (unsigned int i = 1; i < tmp.size(); i++) {
      size_t diff = (char*)tmp[i] - (char*)tmp[i - 1];
      ASSERT_GE(diff, ac.getAllocSize()) << tmp[i] << " " << tmp[i - 1];
    }
  };

  checkAllocsDontOverlap();

  // no further allocations should be possible.
  ASSERT_EQ(ac.allocate(), nullptr);

  // adding a slab should resume the allocations.
  ac.addSlab(grabFreeSlab());
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    allocations.push_back(allocation);
  }

  checkAllocsDontOverlap();

  // no more allocations possible.
  ASSERT_EQ(ac.allocate(), nullptr);

  // release some allocations back to the allocation class.
  const unsigned int nFreed =
      std::min(static_cast<int>(allocations.size() / 2), 5);
  std::vector<void*> freedAllocations;
  for (unsigned int i = 0; i < nFreed; i++) {
    void* allocation = allocations.back();
    allocations.pop_back();
    ASSERT_NE(allocation, nullptr);
    ac.free(allocation);
    freedAllocations.push_back(allocation);
  }

  // calling allocate now should succed up to the number of allocations we
  // freed back.
  for (unsigned int i = 0; i < nFreed; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    allocations.push_back(allocation);
    auto it =
        std::find(freedAllocations.begin(), freedAllocations.end(), allocation);
    ASSERT_NE(it, freedAllocations.end());
    freedAllocations.erase(it);
  }

  checkAllocsDontOverlap();

  // any more allocations should fail.
  ASSERT_EQ(ac.allocate(), nullptr);
}

TEST_F(AllocationClassTest, BadFree) {
  // trying to free memory not belonging to this allocation class should throw.
  auto slabAlloc = createSlabAllocator(100);
  const ClassId cid = 5;
  const PoolId pid = 0;
  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);

  ac.addSlab(slabAlloc->makeNewSlab(pid));
  // random memory being freed
  ASSERT_THROW(ac.free(allocate(ac.getAllocSize())), std::invalid_argument);

  // memory belonging to a slab that is not part of this MC.
  auto slab = slabAlloc->makeNewSlab(pid);
  auto header = slabAlloc->getSlabHeader(slab);
  header->classId = 6; // belongs to diff class.
  void* memory = slab->memoryAtOffset(folly::Random::rand32() % Slab::kSize);
  ASSERT_THROW(ac.free(memory), std::invalid_argument);
}

TEST_F(AllocationClassTest, AddReleaseSlab) {
  // make some allocations from a slab, free some allocations back and call
  // release slab and ensure that no more allocations are possible.
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  auto firstSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(firstSlab, nullptr);

  auto header = slabAlloc->getSlabHeader(firstSlab);
  header->classId = Slab::kInvalidClassId;

  // use 1K allocations.
  const auto allocSize = 1 << 10;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  // add the slab and check that the classId of the slab reflects that.
  ac.addSlab(firstSlab);

  ASSERT_EQ(header->classId, ac.getId());

  std::vector<void*> firstSlabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    firstSlabAllocations.push_back(alloc);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, firstSlab));
  }

  // no more allocations.
  ASSERT_EQ(ac.allocate(), nullptr);

  //  add another slab and allocate from that. add another slab that is not
  //  used at all.
  auto secondSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(secondSlab, nullptr);
  ac.addSlab(secondSlab);
  std::vector<void*> secondSlabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    secondSlabAllocations.push_back(alloc);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, secondSlab));
  }

  auto thirdSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(thirdSlab, nullptr);
  ac.addSlab(thirdSlab);

  // release some allocations back to the allocation class.
  const unsigned int nFreedFirst =
      std::min(static_cast<int>(firstSlabAllocations.size() / 2), 30);
  const unsigned int nFreedSecond =
      std::min(static_cast<int>(secondSlabAllocations.size() / 2), 30);
  std::vector<void*> firstFreed;
  std::vector<void*> secondFreed;

  // randomly free from the first slab and the second slab.
  while (firstFreed.size() != nFreedFirst ||
         secondFreed.size() != nFreedSecond) {
    const auto r = folly::Random::rand32() % 2;
    if (r == 0 && firstFreed.size() != nFreedFirst) {
      auto alloc = firstSlabAllocations.back();
      ac.free(alloc);
      firstSlabAllocations.pop_back();
      firstFreed.push_back(alloc);
    } else if (r == 1 && secondFreed.size() != nFreedSecond) {
      auto alloc = secondSlabAllocations.back();
      ac.free(alloc);
      secondSlabAllocations.pop_back();
      secondFreed.push_back(alloc);
    }
  }

  header = slabAlloc->getSlabHeader(secondSlab);
  ASSERT_EQ(header->classId, ac.getId());

  const SlabReleaseMode dummyMode = SlabReleaseMode::kResize;
  // release the second slab. after this, we should not be getting back any
  // memory allocations from this slab. everything inside secondFreed is
  // purged inside the ac's free list.
  auto secondSlabReleaseContext =
      ac.startSlabRelease(dummyMode, secondSlabAllocations.back());
  ASSERT_FALSE(secondSlabReleaseContext.isReleased());
  // trying to release the same slab while one is already in progress
  // should fail.
  ASSERT_THROW(ac.startSlabRelease(dummyMode, secondSlabAllocations.back()),
               std::invalid_argument);

  ASSERT_EQ(ac.getId(), header->classId);
  ASSERT_EQ(ac.getAllocSize(), header->allocSize);
  ASSERT_EQ(pid, header->poolId);
  ASSERT_TRUE(header->isMarkedForRelease());

  auto freeActiveAllocs = [&ac](const SlabReleaseContext& sr,
                                std::vector<void*>& activeAllocs) {
    for (auto alloc : sr.getActiveAllocations()) {
      auto it = std::find(activeAllocs.begin(), activeAllocs.end(), alloc);
      ASSERT_NE(activeAllocs.end(), it);
      activeAllocs.erase(it);
      ac.free(alloc);
    }
  };

  auto secondSlabActiveAllocs = secondSlabReleaseContext.getActiveAllocations();
  ASSERT_EQ(secondSlabActiveAllocs.size(), secondSlabAllocations.size());
  ASSERT_TRUE(std::is_permutation(secondSlabActiveAllocs.begin(),
                                  secondSlabActiveAllocs.end(),
                                  secondSlabAllocations.begin()));

  freeActiveAllocs(secondSlabReleaseContext, secondSlabActiveAllocs);

  // trying to double free the active allocs should throw.
  for (auto alloc : secondSlabReleaseContext.getActiveAllocations()) {
    ASSERT_THROW(ac.free(alloc), std::invalid_argument);
  }

  ASSERT_NO_THROW(ac.completeSlabRelease(std::move(secondSlabReleaseContext)));

  // the header should reflect that the slab does not belong to the allocation
  // class anymore.
  ASSERT_EQ(Slab::kInvalidClassId, header->classId);
  ASSERT_EQ(0, header->allocSize);
  ASSERT_EQ(pid, header->poolId);
  ASSERT_FALSE(header->isMarkedForRelease());

  // trying to release the second slab again should fail.
  ASSERT_THROW(ac.startSlabRelease(dummyMode, secondSlabAllocations.back()),
               std::invalid_argument);

  ASSERT_EQ(header->classId, Slab::kInvalidClassId);
  ASSERT_FALSE(header->isMarkedForRelease());

  header = slabAlloc->getSlabHeader(thirdSlab);
  ASSERT_EQ(header->classId, ac.getId());
  ASSERT_FALSE(header->isMarkedForRelease());

  // release the third slab as well. This is unused inside the ac at this
  // point.
  auto thirdSlabReleaseContext =
      ac.startSlabRelease(dummyMode, reinterpret_cast<void*>(thirdSlab));
  ASSERT_TRUE(thirdSlabReleaseContext.isReleased());

  // the header should reflect that the slab does not belong to the allocation
  // class anymore.
  ASSERT_EQ(Slab::kInvalidClassId, header->classId);
  ASSERT_EQ(0, header->allocSize);
  ASSERT_EQ(pid, header->poolId);
  ASSERT_FALSE(header->isMarkedForRelease());

  auto thirdSlabActiveAllocs = thirdSlabReleaseContext.getActiveAllocations();
  ASSERT_TRUE(thirdSlabActiveAllocs.empty());
  // calling completeSlabRelease with a released slab is a no-op
  ASSERT_NO_THROW(ac.completeSlabRelease(std::move(thirdSlabReleaseContext)));

  ASSERT_EQ(header->classId, Slab::kInvalidClassId);

  // any allocation after this should not contain the second or third slab.
  // all allocated memory should belong to first slab and we should be able to
  // allocate corrresponding to the number we freed before releasing the
  // second and third slab. this ensures that in the process of releasing the
  // slabs, we dont lose any previously freed allocations belonging to a
  // different slab.
  void* alloc = nullptr;
  unsigned int nFirstAlloc = 0;
  while ((alloc = ac.allocate()) != nullptr) {
    ASSERT_FALSE(slabAlloc->isMemoryInSlab(alloc, secondSlab));
    ASSERT_FALSE(slabAlloc->isMemoryInSlab(alloc, thirdSlab));
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, firstSlab));
    nFirstAlloc++;
  }

  ASSERT_EQ(nFirstAlloc, firstFreed.size());

  // at this point, we have exhausted the allocation class. no more allocations
  // should be possible.
  int ntries = 100;
  while (ntries-- > 0) {
    ASSERT_EQ(ac.allocate(), nullptr);
  }

  ac.addSlab(slabAlloc->makeNewSlab(pid));
  alloc = ac.allocate();
  ASSERT_NE(alloc, nullptr);
  ASSERT_FALSE(slabAlloc->isMemoryInSlab(alloc, firstSlab));
  ASSERT_FALSE(slabAlloc->isMemoryInSlab(alloc, secondSlab));
  ASSERT_FALSE(slabAlloc->isMemoryInSlab(alloc, thirdSlab));
}

TEST_F(AllocationClassTest, CurrentSlabRelease) {
  // make some allocations from a slab, free some allocations back and call
  // release slab and ensure that no more allocations are possible.
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  auto slab = slabAlloc->makeNewSlab(pid);

  // use 1K allocations.
  const auto allocSize = 1 << 10;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  ac.addSlab(slab);

  std::vector<void*> allocations;
  for (unsigned int i = ac.getAllocsPerSlab(); i > 5; i--) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    allocations.push_back(alloc);
  }

  auto context = ac.startSlabRelease(SlabReleaseMode::kResize, allocations[0]);
  auto activeAllocs = context.getActiveAllocations();
  ASSERT_EQ(allocations.size(), activeAllocs.size());
  ASSERT_EQ(allocations, activeAllocs);

  for (auto alloc : allocations) {
    ac.free(alloc);
  }
  ac.completeSlabRelease(std::move(context));
}

TEST_F(AllocationClassTest, SlabReleaseAbort) {
  // make some allocations from a slab, free some allocations back and call
  // release slab and ensure that no more allocations are possible.
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  // Make two slabs
  auto slab = slabAlloc->makeNewSlab(pid);
  auto slab2 = slabAlloc->makeNewSlab(pid);

  // use 1K allocations.
  const auto allocSize = 1 << 10;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  // Add the two slabs to the allocation class
  ac.addSlab(slab);
  ac.addSlab(slab2);

  // Allocate all the Allocs from the first slab
  std::vector<void*> allocations;
  for (unsigned int i = ac.getAllocsPerSlab(); i > 0; i--) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    allocations.push_back(alloc);
  }

  // Allocate most of the Allocs from the second slab
  std::vector<void*> allocations2;
  for (unsigned int i = ac.getAllocsPerSlab(); i > 5; i--) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    allocations2.push_back(alloc);
  }

  // Check stats
  auto statsBeforeRelease = ac.getStats();
  ASSERT_EQ(statsBeforeRelease.totalSlabs(), 2);
  ASSERT_EQ(statsBeforeRelease.activeAllocs,
            allocations.size() + allocations2.size());
  ASSERT_EQ(statsBeforeRelease.freeAllocs, 0);

  // Now start releasing the first(not current) slab and abort it.
  auto context = ac.startSlabRelease(SlabReleaseMode::kResize, allocations[0]);
  auto activeAllocs = context.getActiveAllocations();
  ASSERT_EQ(allocations.size(), activeAllocs.size());
  ASSERT_EQ(allocations, activeAllocs);
  ac.abortSlabRelease(std::move(context));

  // Check stats again to make sure all stats are same
  auto statsAfterAbort = ac.getStats();
  ASSERT_EQ(statsBeforeRelease.totalSlabs(), statsAfterAbort.totalSlabs());
  ASSERT_EQ(statsBeforeRelease.activeAllocs, statsAfterAbort.activeAllocs);
  ASSERT_EQ(statsBeforeRelease.freeAllocs, statsAfterAbort.freeAllocs);

  // Now start releasing the second(current) slab and abort it.
  auto ctx2 = ac.startSlabRelease(SlabReleaseMode::kResize, allocations2[0]);
  activeAllocs = ctx2.getActiveAllocations();
  ASSERT_EQ(allocations2.size(), activeAllocs.size());
  ASSERT_EQ(allocations2, activeAllocs);
  ac.abortSlabRelease(std::move(ctx2));

  auto stats = ac.getStats();
  ASSERT_EQ(stats.totalSlabs(), statsBeforeRelease.totalSlabs());
  ASSERT_EQ(stats.activeAllocs, statsBeforeRelease.activeAllocs);
  // The number of free allocs should be different because un-allocated Allocs
  // in the second slab are put in to the freeAllocations_
  ASSERT_EQ(stats.freeAllocs, ac.getAllocsPerSlab() - allocations2.size());

  // Try to complete the release of the Slab by freeing the memory
  auto ctx3 = ac.startSlabRelease(SlabReleaseMode::kResize, allocations2[0]);
  activeAllocs = ctx3.getActiveAllocations();
  ASSERT_EQ(allocations2.size(), activeAllocs.size());
  ASSERT_EQ(allocations2, activeAllocs);
  for (auto alloc : allocations2) {
    ac.free(alloc);
  }
  ac.completeSlabRelease(std::move(ctx3));
}

TEST_F(AllocationClassTest, SlabReleaseAllFree) {
  // make some allocations from a slab, free all allocations back and ensure
  // that we can release the slab again.
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  auto slab = slabAlloc->makeNewSlab(pid);

  // use 1K allocations.
  const auto allocSize = 1 << 10;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  ac.addSlab(slab);

  std::vector<void*> allocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    allocations.push_back(alloc);
  }

  for (auto alloc : allocations) {
    ac.free(alloc);
  }

  {
    auto context =
        ac.startSlabRelease(SlabReleaseMode::kResize, allocations[0]);
    ASSERT_EQ(slab, context.getSlab());
    ASSERT_TRUE(context.getActiveAllocations().empty());
    ASSERT_TRUE(context.isReleased());

    ac.addSlab(slab);
    allocations.clear();
  }

  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    allocations.push_back(alloc);
  }

  {
    auto context =
        ac.startSlabRelease(SlabReleaseMode::kResize, allocations[0]);
    ASSERT_EQ(allocations, context.getActiveAllocations());

    for (auto alloc : allocations) {
      ac.free(alloc);
    }
    ac.completeSlabRelease(std::move(context));
  }
}

TEST_F(AllocationClassTest, ReleaseSlabWithActiveAllocations) {
  // allocate one slab. allocate some memory and release the slab.
  // we should get back a list of memory allocs that have been
  // previously allocated.
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  auto firstSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(firstSlab, nullptr);

  auto firstSlabHeader = slabAlloc->getSlabHeader(firstSlab);
  firstSlabHeader->classId = Slab::kInvalidClassId;

  // use 1K allocations.
  const auto allocSize = 1 << 10;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  // add the slab and check that the classId of the slab reflects that.
  ac.addSlab(firstSlab);
  ASSERT_EQ(firstSlabHeader->classId, ac.getId());

  std::vector<void*> firstSlabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, firstSlab));
    firstSlabAllocations.push_back(alloc);
  }

  // release the slab and see if we get back all the active allocations
  auto firstSlabReleaseContext = ac.startSlabRelease(
      SlabReleaseMode::kResize, firstSlabAllocations.back());
  ASSERT_FALSE(firstSlabReleaseContext.isReleased());
  const auto firstSlabActiveAllocs =
      firstSlabReleaseContext.getActiveAllocations();
  ASSERT_EQ(firstSlabActiveAllocs.size(), firstSlabAllocations.size());
  ASSERT_TRUE(std::is_permutation(firstSlabActiveAllocs.begin(),
                                  firstSlabActiveAllocs.end(),
                                  firstSlabAllocations.begin()));
  for (auto alloc : firstSlabActiveAllocs) {
    ac.free(alloc);
  }
  ASSERT_NO_THROW(ac.completeSlabRelease(std::move(firstSlabReleaseContext)));
  ASSERT_EQ(firstSlabHeader->classId, Slab::kInvalidClassId);

  // allocate another slab. allocate some memory and release some memory.
  // release the slab we should get back a list of memory allocs that have not
  // been freed.
  auto secondSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(secondSlab, nullptr);

  auto secondSlabHeader = slabAlloc->getSlabHeader(secondSlab);
  secondSlabHeader->classId = Slab::kInvalidClassId;

  ac.addSlab(secondSlab);
  ASSERT_EQ(secondSlabHeader->classId, ac.getId());

  std::vector<void*> secondSlabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, secondSlab));
    secondSlabAllocations.push_back(alloc);
  }

  // release some memory
  size_t allocsToRelease =
      1 + folly::Random::rand32() % (ac.getAllocsPerSlab() / 2);
  while (allocsToRelease--) {
    const size_t i = folly::Random::rand32() % secondSlabAllocations.size();
    ac.free(secondSlabAllocations[i]);
    secondSlabAllocations.erase(std::next(secondSlabAllocations.begin(), i));
  }

  // release the slab and see if we get back all the active allocations
  auto secondSlabReleaseContext = ac.startSlabRelease(
      SlabReleaseMode::kResize, secondSlabAllocations.back());
  ASSERT_FALSE(secondSlabReleaseContext.isReleased());
  const auto secondSlabActiveAllocs =
      secondSlabReleaseContext.getActiveAllocations();
  ASSERT_EQ(secondSlabActiveAllocs.size(), secondSlabAllocations.size());
  ASSERT_TRUE(std::is_permutation(secondSlabActiveAllocs.begin(),
                                  secondSlabActiveAllocs.end(),
                                  secondSlabAllocations.begin()));
  for (auto alloc : secondSlabActiveAllocs) {
    ac.free(alloc);
  }
  ASSERT_NO_THROW(ac.completeSlabRelease(std::move(secondSlabReleaseContext)));
  ASSERT_EQ(secondSlabHeader->classId, Slab::kInvalidClassId);
}

// 1. Add two slabs to the AC
// 2. Allocate until full for each slab
// 3. Free 2 * kFreeAllocsPruneLimit from each slab
// 4. Asynchronously free 500 allocs from first slab
// 5. Release slab and check the active allocations are
//    GREATER or EQUAL to
//    (total allocs - 2 * kFreeAllocsPruneLimit - 500)
// 6. Verify the source of truth "slabReleaseAllocMap" that the freed allocs
//    are indeed 2 * kFreeAllocsPruneLimit + 500
TEST_F(AllocationClassTest, ReleaseSlabMultithread) {
  // allocate one slab. allocate some memory and release the slab.
  // we should get back a list of memory allocs that have been
  // previously allocated.
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  // use 64 bytes allocations.
  const auto allocSize = 1 << 6;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  // add first slab and allocate until full
  auto firstSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(firstSlab, nullptr);

  auto firstSlabHeader = slabAlloc->getSlabHeader(firstSlab);
  firstSlabHeader->classId = Slab::kInvalidClassId;

  ac.addSlab(firstSlab);
  ASSERT_EQ(firstSlabHeader->classId, ac.getId());

  std::vector<void*> firstSlabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, firstSlab));
    firstSlabAllocations.push_back(alloc);
  }

  // add second slab and allocate until full
  auto secondSlab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(secondSlab, nullptr);

  auto secondSlabHeader = slabAlloc->getSlabHeader(secondSlab);
  secondSlabHeader->classId = Slab::kInvalidClassId;

  ac.addSlab(secondSlab);
  ASSERT_EQ(secondSlabHeader->classId, ac.getId());

  std::vector<void*> secondSlabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, secondSlab));
    secondSlabAllocations.push_back(alloc);
  }

  // release 4 * kPruneFreeAllocsLimit
  // 2 from each slab.
  ASSERT_LT(4 * AllocationClass::kFreeAllocsPruneLimit,
            firstSlabAllocations.size());
  ASSERT_LT(4 * AllocationClass::kFreeAllocsPruneLimit,
            secondSlabAllocations.size());
  for (unsigned int i = 0; i < 4 * AllocationClass::kFreeAllocsPruneLimit;
       ++i) {
    bool freeFirstSlab = i % 2;
    auto alloc = freeFirstSlab ? firstSlabAllocations.back()
                               : secondSlabAllocations.back();
    ac.free(alloc);

    if (freeFirstSlab) {
      firstSlabAllocations.pop_back();
    } else {
      secondSlabAllocations.pop_back();
    }
  }

  // release 500 allocs from first allocations
  std::thread releaseThread{[&] {
    ASSERT_LT(500, firstSlabAllocations.size());
    for (unsigned int i = 0; i < 500; ++i) {
      auto alloc = firstSlabAllocations.back();
      firstSlabAllocations.pop_back();
      ac.free(alloc);
    }
  }};

  // release first slab, we once `freeThread` is done, we should
  // see the number of active allocations is greater or equal to
  // the number of total allocations per slab
  // minus (2 * kPruneFreeAllocsLimit + 50)
  //
  // But the alloc state should match exactly the number of allocs freed
  auto firstSlabReleaseContext = ac.startSlabRelease(
      SlabReleaseMode::kResize, firstSlabAllocations.back());

  releaseThread.join();

  ASSERT_FALSE(firstSlabReleaseContext.isReleased());
  const auto firstSlabActiveAllocs =
      firstSlabReleaseContext.getActiveAllocations();
  ASSERT_LE(
      ac.getAllocsPerSlab() - 2 * AllocationClass::kFreeAllocsPruneLimit - 500,
      firstSlabActiveAllocs.size());
  ASSERT_LE(firstSlabAllocations.size(), firstSlabActiveAllocs.size());

  auto& allocState = ac.getSlabReleaseAllocMapLocked(firstSlab);
  int freedAllocs = 0;
  for (bool freed : allocState) {
    if (freed) {
      ++freedAllocs;
    }
  }
  ASSERT_EQ(2 * AllocationClass::kFreeAllocsPruneLimit + 500, freedAllocs);
}

// 1. allocate 10 slabs
// 2. allocate until full
// 3. free 99% of the allocations
// 4. free each slab, and ensure the active alloc list is correct
TEST_F(AllocationClassTest, ReleaseSlabWithManyFrees) {
  // allocate one slab. allocate some memory and release the slab.
  // we should get back a list of memory allocs that have been
  // previously allocated.
  const int numSlabs = 10;
  auto slabAlloc = createSlabAllocator(numSlabs);
  const PoolId pid = 2;
  const ClassId cid = 3;

  // use 64 bytes allocations.
  const auto allocSize = 1 << 6;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  std::vector<std::vector<void*>> activeAllocationsList;
  std::vector<void*> slabReleaseHints;
  for (unsigned int i = 0; i < numSlabs; ++i) {
    // add first slab and allocate until full
    auto slab = slabAlloc->makeNewSlab(pid);
    ASSERT_NE(slab, nullptr);

    auto slabHeader = slabAlloc->getSlabHeader(slab);
    slabHeader->classId = Slab::kInvalidClassId;

    ac.addSlab(slab);
    ASSERT_EQ(slabHeader->classId, ac.getId());

    std::vector<void*> slabAllocations;
    for (unsigned int j = 0; j < ac.getAllocsPerSlab(); j++) {
      auto alloc = ac.allocate();
      ASSERT_NE(alloc, nullptr);
      ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, slab));
      slabAllocations.push_back(alloc);
    }
    activeAllocationsList.push_back(slabAllocations);
    // use the first alloc as a hint for releasing this slab later
    slabReleaseHints.push_back(slabAllocations.at(0));
  }

  // free 99% of allocs in random order
  int numToFree = numSlabs * (ac.getAllocsPerSlab() * 99 / 100);
  while (numToFree-- > 0) {
    auto slabIdx = folly::Random::rand32(numSlabs);
    auto& activeAllocs = activeAllocationsList.at(slabIdx);
    if (activeAllocs.empty()) {
      ++numToFree;
      continue;
    }
    auto allocIdx = folly::Random::rand32(activeAllocs.size());
    auto alloc = activeAllocs.at(allocIdx);
    activeAllocs.erase(activeAllocs.begin() + allocIdx);
    ac.free(alloc);
  }

  // Free each slab one by one and verify the active allocs list is correct
  for (unsigned int i = 0; i < numSlabs; ++i) {
    auto& activeAllocs = activeAllocationsList.at(i);
    auto slabReleaseContext =
        ac.startSlabRelease(SlabReleaseMode::kResize, slabReleaseHints.at(i));

    // verify active allocs list is correct
    auto slabActiveAllocs = slabReleaseContext.getActiveAllocations();
    ASSERT_TRUE(std::is_permutation(
        activeAllocs.begin(), activeAllocs.end(), slabActiveAllocs.begin()));

    // free the remaining active allocs and finish releasing this slab
    while (!activeAllocs.empty()) {
      auto alloc = activeAllocs.back();
      activeAllocs.pop_back();
      ac.free(alloc);
    }
    ASSERT_NO_THROW(ac.completeSlabRelease(std::move(slabReleaseContext)));
  }
}

TEST_F(AllocationClassTest, Serialization) {
  // allocator with ~100 slabs.
  auto slabAlloc = createSlabAllocator(100);
  const unsigned int nUsable = slabAlloc->getNumUsableSlabs();
  std::vector<Slab*> freeSlabs;
  const PoolId pid = 0;
  const ClassId cid = 0;
  for (unsigned int i = 0; i < nUsable; i++) {
    auto slab = slabAlloc->makeNewSlab(pid);
    ASSERT_NE(slab, nullptr);
    freeSlabs.push_back(slab);
  }

  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);

  std::vector<Slab*> usedSlabs;
  auto grabFreeSlab = [&freeSlabs, &usedSlabs]() -> Slab* {
    assert(!freeSlabs.empty());
    Slab* s = freeSlabs.back();
    freeSlabs.pop_back();
    usedSlabs.push_back(s);
    return s;
  };

  // add a slab and ensure that we can make sufficient number of
  // allocations.
  auto slab = grabFreeSlab();
  ac.addSlab(slab);
  const unsigned int nPerSlab = ac.getAllocsPerSlab();
  std::vector<void*> allocations;
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }

  // no further allocations should be possible.
  ASSERT_EQ(ac.allocate(), nullptr);

  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serializer.serialize(ac.saveState());

  Deserializer deserializer(begin, end);
  AllocationClass ac2(
      deserializer.deserialize<serialization::AllocationClassObject>(),
      pid,
      *slabAlloc);
  ASSERT_TRUE(isSameAllocationClass(ac, ac2));

  // no further allocations should be possible.
  ASSERT_EQ(ac2.allocate(), nullptr);

  // free all allocations
  for (auto alloc : allocations) {
    ac2.free(alloc);
  }

  // should be able to make allocations again
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac2.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }
}

TEST_F(AllocationClassTest, InvalidDeSerialization) {
  // allocator with ~100 slabs.
  auto slabAlloc = createSlabAllocator(100);
  const unsigned int nUsable = slabAlloc->getNumUsableSlabs();
  std::vector<Slab*> freeSlabs;
  const PoolId pid = 0;
  const ClassId cid = 0;
  for (unsigned int i = 0; i < nUsable; i++) {
    auto slab = slabAlloc->makeNewSlab(pid);
    ASSERT_NE(slab, nullptr);
    freeSlabs.push_back(slab);
  }

  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);

  std::vector<Slab*> usedSlabs;
  auto grabFreeSlab = [&freeSlabs, &usedSlabs]() -> Slab* {
    assert(!freeSlabs.empty());
    Slab* s = freeSlabs.back();
    freeSlabs.pop_back();
    usedSlabs.push_back(s);
    return s;
  };

  // add a slab and ensure that we can make sufficient number of
  // allocations.
  auto slab = grabFreeSlab();
  ac.addSlab(slab);
  const unsigned int nPerSlab = ac.getAllocsPerSlab();
  std::vector<void*> allocations;

  // make half the allocations now.
  for (unsigned int i = 0; i < nPerSlab / 2; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }

  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serializer.serialize(ac.saveState());

  Deserializer deserializer(begin, end);
  auto correctState =
      deserializer.deserialize<serialization::AllocationClassObject>();
  AllocationClass ac2(correctState, pid, *slabAlloc);
  ASSERT_TRUE(isSameAllocationClass(ac, ac2));

  // should be able to make the other half of the allocations again
  for (unsigned int i = 0; i < nPerSlab / 2; i++) {
    auto allocation = ac2.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }

  // try to create with invalid serialized state and
  // first, invalid classid.
  auto incorrectClassIdState = correctState;
  *incorrectClassIdState.classId() = -20;

  ASSERT_THROW(AllocationClass(incorrectClassIdState, pid, *slabAlloc),
               std::invalid_argument);

  // test with invalid allocSizes in the serialized state
  auto incorrectAllocSizeState = correctState;
  *incorrectAllocSizeState.allocationSize() = 0;

  ASSERT_THROW(AllocationClass(incorrectAllocSizeState, pid, *slabAlloc),
               std::invalid_argument);

  *incorrectAllocSizeState.allocationSize() =
      Slab::kSize + folly::Random::rand32() % Slab::kSize + 1;

  ASSERT_THROW(AllocationClass(incorrectAllocSizeState, pid, *slabAlloc),
               std::invalid_argument);

  // invalid current slab
  auto incorrectCurrSlabState = correctState;

  // a 32 bit address is not going to be valid.
  *incorrectCurrSlabState.currSlabIdx() = folly::Random::rand32();
  ASSERT_THROW(AllocationClass(incorrectCurrSlabState, pid, *slabAlloc),
               std::invalid_argument);

  slab = grabFreeSlab();
  auto header = slabAlloc->getSlabHeader(slab);
  header->classId = cid + 1;
  *incorrectCurrSlabState.currSlabIdx() = slabAlloc->slabIdx(slab);
  ASSERT_THROW(AllocationClass(incorrectCurrSlabState, pid, *slabAlloc),
               std::invalid_argument);

  // even if the classId is correct, it must belong to the allocated slab.
  header->classId = cid;
  ASSERT_THROW(AllocationClass(incorrectClassIdState, pid, *slabAlloc),
               std::invalid_argument);
}

// Should not be able to save/restore during active releases
TEST_F(AllocationClassTest, SerializationWithActiveReleases) {
  // allocator with ~100 slabs.
  auto slabAlloc = createSlabAllocator(100);
  const unsigned int nUsable = slabAlloc->getNumUsableSlabs();
  std::vector<Slab*> freeSlabs;
  const PoolId pid = 0;
  const ClassId cid = 0;
  for (unsigned int i = 0; i < nUsable; i++) {
    auto slab = slabAlloc->makeNewSlab(pid);
    ASSERT_NE(slab, nullptr);
    freeSlabs.push_back(slab);
  }

  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);

  std::vector<Slab*> usedSlabs;
  auto grabFreeSlab = [&freeSlabs, &usedSlabs]() -> Slab* {
    assert(!freeSlabs.empty());
    Slab* s = freeSlabs.back();
    freeSlabs.pop_back();
    usedSlabs.push_back(s);
    return s;
  };

  // add a slab and ensure that we can make sufficient number of
  // allocations.
  auto slab = grabFreeSlab();
  ac.addSlab(slab);
  const unsigned int nPerSlab = ac.getAllocsPerSlab();
  std::vector<void*> allocations;

  // make half the allocations now.
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }

  auto releaseContext = ac.startSlabRelease(SlabReleaseMode::kResize,
                                            reinterpret_cast<void*>(slab));
  ASSERT_FALSE(releaseContext.isReleased());
  ASSERT_THROW(ac.saveState(), std::logic_error);
  for (auto alloc : releaseContext.getActiveAllocations()) {
    ac.free(alloc);
  }

  ac.completeSlabRelease(std::move(releaseContext));
  ASSERT_NO_THROW(ac.saveState());

  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serializer.serialize(ac.saveState());

  Deserializer deserializer(begin, end);
  auto correctState =
      deserializer.deserialize<serialization::AllocationClassObject>();
  AllocationClass ac2(correctState, pid, *slabAlloc);
  ASSERT_TRUE(isSameAllocationClass(ac, ac2));
}

TEST_F(AllocationClassTest, Stats) {
  auto slabAlloc = createSlabAllocator(100);
  const unsigned int nUsable = slabAlloc->getNumUsableSlabs();
  const PoolId pid = 0;
  const ClassId cid = 0;
  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);

  // no allocation yet.
  {
    auto stat = ac.getStats();
    ASSERT_EQ(0, stat.usedSlabs);
    ASSERT_EQ(0, stat.freeSlabs);
    ASSERT_EQ(0, stat.freeAllocs);
    ASSERT_EQ(ac.getAllocsPerSlab(), stat.allocsPerSlab);
  }

  unsigned int used = 0;
  std::vector<void*> allocs;
  used++;
  ac.addSlab(slabAlloc->makeNewSlab(pid));
  while (true) {
    auto alloc = ac.allocate();
    if (alloc == nullptr) {
      break;
    }
    allocs.push_back(alloc);
    ASSERT_EQ(allocs.size(), ac.getStats().activeAllocs);
  }

  {
    auto stat = ac.getStats();
    ASSERT_EQ(used, stat.usedSlabs);
    ASSERT_EQ(0, stat.freeSlabs);
    ASSERT_EQ(0, stat.freeAllocs);
  }

  while (used < 5 && used < nUsable) {
    ac.addSlab(slabAlloc->makeNewSlab(pid));
    used++;
    auto stat = ac.getStats();
    ASSERT_EQ(1, stat.usedSlabs);
    // all the slabs used are free except the one we used initially.
    ASSERT_EQ(used - 1, stat.freeSlabs);
    ASSERT_EQ((used - 1) * Slab::kSize, stat.getTotalFreeMemory());
    ASSERT_EQ(0, stat.freeAllocs);
  }

  int nFreed = 0;
  while (nFreed < 100 && !allocs.empty()) {
    auto alloc = allocs.back();
    allocs.pop_back();
    ac.free(alloc);
    nFreed++;

    auto stat = ac.getStats();
    auto memFree = (used - 1) * Slab::kSize + nFreed * stat.allocSize;
    ASSERT_EQ(memFree, stat.getTotalFreeMemory());
    ASSERT_EQ(1, stat.usedSlabs);
    // all the slabs used are free except the one we used initially.
    ASSERT_EQ(used - 1, stat.freeSlabs);
    ASSERT_EQ(nFreed, stat.freeAllocs);
  }
}

// Test alloc processing during slab release
TEST_F(AllocationClassTest, ProcessAllocForRelease) {
  auto slabAlloc = createSlabAllocator(1);
  const PoolId pid = 0;
  const ClassId cid = 0;
  auto slab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(slab, nullptr);
  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);
  ac.addSlab(slab);

  const unsigned int nPerSlab = ac.getAllocsPerSlab();

  std::vector<void*> allAllocs;
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac.allocate();
    allAllocs.push_back(allocation);
  }

  // Free a random set of allocs
  std::vector<void*> activeAllocs;
  for (unsigned int i = 0; i < nPerSlab; i++) {
    const auto& alloc = allAllocs[i];
    if (folly::Random::rand32() % 2) {
      ac.free(alloc);
    } else {
      activeAllocs.push_back(alloc);
    }
  }

  auto releaseContext = ac.startSlabRelease(SlabReleaseMode::kResize,
                                            reinterpret_cast<void*>(slab));

  std::vector<void*> processedActiveAllocs;
  for (const auto& alloc : releaseContext.getActiveAllocations()) {
    const auto fn = [&processedActiveAllocs](void* memory) {
      processedActiveAllocs.push_back(memory);
    };
    ac.processAllocForRelease(releaseContext, alloc, fn);
  }

  ASSERT_EQ(activeAllocs, processedActiveAllocs);

  // Complete the slab release process
  for (const auto& alloc : processedActiveAllocs) {
    ac.free(alloc);
  }
  ac.completeSlabRelease(std::move(releaseContext));
}

TEST_F(AllocationClassTest, AbortSlabReleaseTest) {
  // make  allocations from a slab, free some allocations back and call
  // abort slab and ensure that the allocation class is in the expected state
  auto slabAlloc = createSlabAllocator(10);
  const PoolId pid = 2;
  const ClassId cid = 3;

  auto slab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(slab, nullptr);

  auto header = slabAlloc->getSlabHeader(slab);
  header->classId = Slab::kInvalidClassId;

  // use 1K allocations.
  const auto allocSize = 1 << 10;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);

  // add the slab and check that the classId of the slab reflects that.
  ac.addSlab(slab);

  ASSERT_EQ(header->classId, ac.getId());

  std::vector<void*> slabAllocations;
  for (unsigned int i = 0; i < ac.getAllocsPerSlab(); i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    slabAllocations.push_back(alloc);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, slab));
  }

  // no more allocations.
  ASSERT_EQ(ac.allocate(), nullptr);

  ASSERT_EQ(slabAllocations.size(), ac.getAllocsPerSlab());
  auto stats = ac.getStats();
  ASSERT_EQ(stats.activeAllocs, ac.getAllocsPerSlab());
  ASSERT_EQ(stats.freeAllocs, 0);

  auto slabReleaseContext =
      ac.startSlabRelease(SlabReleaseMode::kRebalance, nullptr);

  // free half of the allocations
  for (unsigned int i = 0; i < ac.getAllocsPerSlab() / 2; i++) {
    auto alloc = slabAllocations.back();
    ac.free(alloc);
    slabAllocations.pop_back();
  }

  // Assert abortSlabRelease is successful
  ASSERT_NO_THROW(ac.abortSlabRelease(slabReleaseContext));
  stats = ac.getStats();

  // Make sure stats are correct
  ASSERT_EQ(stats.activeAllocs, ac.getAllocsPerSlab() / 2);
  ASSERT_EQ(stats.freeAllocs, ac.getAllocsPerSlab() / 2);

  // Check to see if we can allocate the half of them back
  for (unsigned int i = 0; i < ac.getAllocsPerSlab() / 2; i++) {
    auto alloc = ac.allocate();
    ASSERT_NE(alloc, nullptr);
    slabAllocations.push_back(alloc);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(alloc, slab));
  }

  // no more allocations.
  ASSERT_EQ(ac.allocate(), nullptr);
}

TEST_F(AllocationClassTest, RestoreFreedAllocations) {
  auto slabAlloc = createSlabAllocator(1);

  const PoolId pid = 0;
  const ClassId cid = 0;
  auto slab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(slab, nullptr);

  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);
  ac.addSlab(slab);
  const unsigned int nPerSlab = ac.getAllocsPerSlab();
  std::vector<void*> allocations;
  for (unsigned int i = 0; i < nPerSlab; i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    allocations.push_back(allocation);
  }

  // no further allocations should be possible.
  ASSERT_EQ(ac.allocate(), nullptr);

  std::vector<void*> freedAllocations;
  for (unsigned int i = 0; i < nPerSlab; i++) {
    if (folly::Random::rand32() % 2) {
      ac.free(allocations[i]);
      freedAllocations.push_back(allocations[i]);
    }
  }

  std::array<uint8_t, SerializationBufferSize> buffer;
  uint8_t* begin = buffer.begin();
  uint8_t* end = buffer.end();
  Serializer serializer(begin, end);
  serializer.serialize(ac.saveState());

  Deserializer deserializer(begin, end);
  AllocationClass ac2(
      deserializer.deserialize<serialization::AllocationClassObject>(),
      pid,
      *slabAlloc);
  ASSERT_TRUE(isSameAllocationClass(ac, ac2));

  std::vector<void*> newAllocations;
  for (unsigned int i = 0; i < freedAllocations.size(); i++) {
    auto allocation = ac.allocate();
    ASSERT_NE(allocation, nullptr);
    ASSERT_TRUE(slabAlloc->isMemoryInSlab(allocation, slab));
    newAllocations.push_back(allocation);
  }

  // no further allocations should be possible.
  ASSERT_EQ(ac.allocate(), nullptr);

  // The intrusive list contains freed allocations in the reverse order.
  reverse(newAllocations.begin(), newAllocations.end());
  ASSERT_EQ(newAllocations, freedAllocations);
}

TEST_F(AllocationClassTest, forEachAllocationBasicSmallAlloc) {
  // create a dummy allocator to instantiate the AllocationClass.
  const auto slabAlloc = createSlabAllocator(1);
  const ClassId cid = 5;
  const PoolId pid = 0;

  // test with a 64 byte allocaiton size.
  const size_t allocSize = 64;
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);
  ASSERT_EQ(ac.getId(), cid);
  ASSERT_EQ(ac.getAllocSize(), allocSize);

  // add a slab and then invoke the allocation traversal function.
  auto slab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(slab, nullptr);
  ac.addSlab(slab);
  ASSERT_EQ(ac.getAllocsPerSlab(), Slab::kSize / allocSize);
  forEachAllocationCount = 0;
  ac.forEachAllocation(slab, test_callback);
  ASSERT_EQ(forEachAllocationCount, ac.getAllocsPerSlab());
}
TEST_F(AllocationClassTest, forEachAllocationBasicRandomAlloc) {
  // create a dummy allocator to instantiate the AllocationClass.
  const auto slabAlloc = createSlabAllocator(1);
  const ClassId cid = 5;
  const PoolId pid = 0;

  // test with a size not more than the slab size.
  const size_t allocSize = getRandomAllocSize();
  AllocationClass ac(cid, pid, allocSize, *slabAlloc);
  ASSERT_EQ(ac.getId(), cid);
  ASSERT_EQ(ac.getAllocSize(), allocSize);

  // add a slab and then invoke the allocation traversal function.
  auto slab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(slab, nullptr);
  ac.addSlab(slab);
  ASSERT_GT(ac.getAllocsPerSlab(), 0);
  ASSERT_LE(ac.getAllocsPerSlab(), Slab::kSize);
  forEachAllocationCount = 0;
  ac.forEachAllocation(slab, test_callback);
  ASSERT_EQ(forEachAllocationCount, ac.getAllocsPerSlab());
}
TEST_F(AllocationClassTest, forEachAllocationSlabRelease) {
  // create a dummy allocator to instantiate the AllocationClass.
  auto slabAlloc = createSlabAllocator(1);
  const PoolId pid = 0;
  const ClassId cid = 0;

  // add a slab and then invoke the allocation traversal function.
  auto slab = slabAlloc->makeNewSlab(pid);
  ASSERT_NE(slab, nullptr);
  AllocationClass ac(cid, pid, getRandomAllocSize(), *slabAlloc);
  ac.addSlab(slab);

  // Start the slab release, when it is release state try to
  // traverse it.
  auto releaseContext = ac.startSlabRelease(SlabReleaseMode::kResize,
                                            reinterpret_cast<void*>(slab));
  forEachAllocationCount = 0;
  ASSERT_EQ(forEachAllocationCount, 0);
}
} // namespace cachelib
} // namespace facebook
