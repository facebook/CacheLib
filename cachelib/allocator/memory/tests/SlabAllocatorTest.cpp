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

#include <cachelib/common/Utils.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <random>
#include <vector>

#include "cachelib/allocator/memory/SlabAllocator.h"
#include "cachelib/allocator/memory/tests/TestBase.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/shm/ShmManager.h"

using namespace facebook::cachelib::tests;
using namespace facebook::cachelib;

using SlabAllocatorTest = AllocTestBase;

constexpr size_t SerializationBufferSize = 100 * 1024;

namespace {
SlabAllocator::Config getDefaultConfig() {
  return {true /* disableFullCoredump */, false /* lockMemory */};
}
} // namespace

TEST_F(SlabAllocatorTest, SmallMemory) {
  // creating with not enough memory should fail.
  size_t size = Slab::kSize;
  void* memory = allocate(size);
  ASSERT_THROW(SlabAllocator(memory, size, getDefaultConfig()),
               std::invalid_argument);

  size = 2 * Slab::kSize;
  memory = allocate(size);
  ASSERT_NO_THROW(SlabAllocator(memory, size, getDefaultConfig()));

  // unaligned memory region with just space for two slabs should fail.
  memory = reinterpret_cast<void*>(reinterpret_cast<char*>(memory) + 100);
  ASSERT_THROW(SlabAllocator(memory, size, getDefaultConfig()),
               std::invalid_argument);

  // with unaligned size, the same pattern should be true.
  size = Slab::kSize + 1000;
  memory = allocate(size);
  ASSERT_THROW(SlabAllocator(memory, size, getDefaultConfig()),
               std::invalid_argument);

  size = 2 * Slab::kSize + 1000;
  memory = allocate(size);
  ASSERT_NO_THROW(SlabAllocator(memory, size, getDefaultConfig()));

  size = 3 * Slab::kSize + 1000;
  memory = allocate(size);

  // unaligned memory and unaligned size.
  memory = reinterpret_cast<void*>(reinterpret_cast<char*>(memory) + 100);
  ASSERT_THROW(SlabAllocator s(memory, size, getDefaultConfig()),
               std::invalid_argument);
}

TEST_F(SlabAllocatorTest, MakeSlabs) {
  // allocator enough for 100 slabs
  const unsigned int numSlabs = 100;
  const size_t size = numSlabs * Slab::kSize;

  void* memory = allocate(size);
  SlabAllocator s(memory, size, getDefaultConfig());
  PoolId poolId = 0;

  const auto nUsable = s.getNumUsableSlabs();
  std::vector<Slab*> slabs;
  // should be able to allocate total number of usable slabs
  for (size_t i = 0; i < nUsable; i++) {
    Slab* slab = s.makeNewSlab(poolId);
    ASSERT_NE(slab, nullptr);
    slabs.push_back(slab);
  }

  ASSERT_EQ(slabs.size(), nUsable);

  ASSERT_TRUE(s.allSlabsAllocated());
  // should not be able to allocate any more slabs.
  for (size_t i = 0; i < 10; i++) {
    // TODO check for the type of error.
    ASSERT_EQ(s.makeNewSlab(poolId), nullptr);
  }

  // shuffle the slabs.
  std::mt19937 gen(folly::Random::rand32());
  std::shuffle(slabs.begin(), slabs.end(), gen);

  // free couple of slabs and this should let us allocate more ones
  const unsigned int nFreed = 20;
  for (size_t i = 0; i < nFreed; i++) {
    auto slab = slabs.back();
    s.freeSlab(slab);
    slabs.pop_back();
  }

  ASSERT_FALSE(s.allSlabsAllocated());

  for (size_t i = 0; i < nFreed; i++) {
    Slab* slab = s.makeNewSlab(poolId);
    ASSERT_NE(slab, nullptr);
    slabs.push_back(slab);
  }

  ASSERT_EQ(slabs.size(), nUsable);
  ASSERT_EQ(nUsable, s.getNumUsableSlabs());
}

TEST_F(SlabAllocatorTest, SlabHeader) {
  const size_t size = 20 * Slab::kSize;
  void* memory = allocate(size);
  SlabAllocator s(memory, size, getDefaultConfig());

  const auto nUsable = s.getNumUsableSlabs();
  CHECK_GT(nUsable, 0);
  CHECK_LE(nUsable, 20);
  const PoolId poolId = 5;

  // allocate slabs and get their headers.
  std::vector<Slab*> slabs;
  std::vector<SlabHeader*> slabHeaders;
  for (size_t i = 0; i < nUsable; i++) {
    auto* slab = s.makeNewSlab(poolId);
    ASSERT_NE(slab, nullptr);
    slabs.push_back(slab);
    auto slabHeader = s.getSlabHeader(slab);
    ASSERT_NE(slabHeader, nullptr);
    slabHeaders.push_back(slabHeader);
    ASSERT_EQ(slabHeader->poolId, poolId);
  }

  // ensure we have headers for everything.
  CHECK_EQ(slabs.size(), slabHeaders.size());

  auto checkSlabHeaderFn = [nUsable](const SlabAllocator& sa,
                                     const std::vector<Slab*>& sv,
                                     const std::vector<SlabHeader*>& sHeaders) {
    // ensure that calling getSlabHeader on a memory belonging to a given
    // slab gives the correct header back.
    for (size_t i = 0; i < nUsable; i++) {
      ASSERT_EQ(sa.getSlabHeader(sv[i]), sHeaders[i]);
      ASSERT_TRUE(sa.isValidSlab(sv[i]));
      for (unsigned int offset = 0; offset < Slab::kSize; offset++) {
        void* m = sv[i]->memoryAtOffset(offset);
        // should return the correct slab header.
        ASSERT_EQ(sa.getSlabHeader(m), sHeaders[i]);
        // ensure that the memory is in slab
        ASSERT_TRUE(sa.isMemoryInSlab(m, sv[i]));
        ASSERT_EQ(sa.getSlabForMemory(m), sv[i]);
      }
    }
  };

  checkSlabHeaderFn(s, slabs, slabHeaders);

  const PoolId poolIdNew = 7;
  const unsigned int nFreed = std::min(5, static_cast<int>(nUsable / 2));
  // free some slabs and allocate them to a new pool id.
  for (size_t i = 0; i < nFreed; i++) {
    auto slab = slabs.back();
    s.freeSlab(slab);
    slabs.pop_back();
    auto header = slabHeaders.back();
    // ensure that the pool id is reset.
    ASSERT_NE(poolId, header->poolId);
    slabHeaders.pop_back();
  }

  // allocate 20 new slabs.
  for (size_t i = 0; i < nFreed; i++) {
    auto slab = s.makeNewSlab(poolIdNew);
    ASSERT_NE(slab, nullptr);
    auto slabHeader = s.getSlabHeader(slab);
    ASSERT_NE(slabHeader, nullptr);
    slabs.push_back(slab);
    slabHeaders.push_back(slabHeader);
    ASSERT_EQ(slabHeader->poolId, poolIdNew);
  }

  checkSlabHeaderFn(s, slabs, slabHeaders);

  // first slab from the memory start is currently used as header. So that
  // should not have any headers.
  ASSERT_EQ(nullptr, s.getSlabHeader(memory));
  for (size_t i = 0; i < Slab::kSize; i++) {
    ASSERT_EQ(nullptr, s.getSlabHeader(reinterpret_cast<uint8_t*>(memory) + i));
  }
}

TEST_F(SlabAllocatorTest, SlabMemoryValidity) {
  // allocator enough for 100 slabs
  const unsigned int numSlabs = 10;
  const size_t size = numSlabs * Slab::kSize;

  void* memory = allocate(size);
  SlabAllocator s(memory, size, getDefaultConfig());
  PoolId poolId = 0;

  const auto firstSlab = s.makeNewSlab(poolId);
  const auto secondSlab = s.makeNewSlab(poolId);

  // the above two should be valid.
  ASSERT_TRUE(s.isValidSlab(firstSlab));
  ASSERT_TRUE(s.isValidSlab(secondSlab));

  auto checkBetweenValidSlab = [&s](const Slab* first, const Slab* second) {
    ASSERT_TRUE(s.isValidSlab(first));
    ASSERT_TRUE(s.isValidSlab(second));
    for (size_t i = 0; i < Slab::kSize; i++) {
      auto m = first->memoryAtOffset(i);
      ASSERT_FALSE(s.isMemoryInSlab(m, second));
      ASSERT_TRUE(s.isMemoryInSlab(m, first));
    }
  };
  checkBetweenValidSlab(firstSlab, secondSlab);

  auto checkInValidSlab = [&s](const Slab* invalidSlab) {
    ASSERT_FALSE(s.isValidSlab(invalidSlab));

    // using an invalid slab in isMemoryInSlab should return false since the
    // slab is invalid.
    for (size_t i = 0; i < Slab::kSize; i++) {
      auto m = invalidSlab->memoryAtOffset(i);
      ASSERT_FALSE(s.isMemoryInSlab(m, invalidSlab));
      // getSlabForMemory should work fine since it only does address masking.
      ASSERT_EQ(s.getSlabForMemory(m), invalidSlab);
    }
  };

  // allocate a random slab aligned to slab size.
  auto invSlab = (Slab*)allocate(Slab::kSize);
  checkInValidSlab(invSlab);

  // using unallocated slab should result in the same as well.
  for (size_t i = 1; i < 5; i++) {
    invSlab = secondSlab + i;
    checkInValidSlab(invSlab);
  }

  // first slab from the memory start is currently used as header. So that
  // should not be valid
  for (size_t i = 0; i < Slab::kSize; i++) {
    auto slab = s.getSlabForMemory(reinterpret_cast<uint8_t*>(memory) + i);
    ASSERT_NE(nullptr, slab);
    ASSERT_FALSE(s.isValidSlab(slab));
  }
}

TEST_F(SlabAllocatorTest, Serialization) {
  const unsigned int numSlabs = 10;
  const size_t size = numSlabs * Slab::kSize;

  void* memory = allocate(size);
  SlabAllocator s(memory, size, getDefaultConfig());
  PoolId poolId = 0;

  s.makeNewSlab(poolId);
  s.makeNewSlab(poolId);
  const auto slab3 = s.makeNewSlab(poolId);
  const auto slab4 = s.makeNewSlab(poolId);

  s.freeSlab(slab3);
  s.freeSlab(slab4);

  auto checkSlabsAndMemoryInSlab = [&](SlabAllocator& sAlloc) {
    const auto first = sAlloc.getSlabForIdx(0);
    const auto second = sAlloc.getSlabForIdx(1);

    ASSERT_TRUE(sAlloc.isValidSlab(first));
    ASSERT_TRUE(sAlloc.isValidSlab(second));
    ASSERT_TRUE(sAlloc.isValidSlab(sAlloc.getSlabForIdx(2)));
    ASSERT_TRUE(sAlloc.isValidSlab(sAlloc.getSlabForIdx(3)));

    for (size_t i = 0; i < Slab::kSize; i++) {
      auto m = first->memoryAtOffset(i);
      ASSERT_FALSE(sAlloc.isMemoryInSlab(m, second));
      ASSERT_TRUE(sAlloc.isMemoryInSlab(m, first));
    }
  };
  checkSlabsAndMemoryInSlab(s);

  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serializer.serialize(s.saveState());

  // Attach to a different address
  void* memory2 = allocate(size);
  ASSERT_NE(memory, memory2);
  memcpy(memory2, memory, size);

  Deserializer deserializer(begin, end);
  SlabAllocator s2(
      deserializer.deserialize<serialization::SlabAllocatorObject>(), memory2,
      size, getDefaultConfig());

  ASSERT_TRUE(isSameSlabAllocator(s, s2));
  checkSlabsAndMemoryInSlab(s2);
}

TEST_F(SlabAllocatorTest, InvalidDeSerialization) {
  const unsigned int numSlabs = 10;
  const size_t size = numSlabs * Slab::kSize;

  void* memory = allocate(size);
  SlabAllocator s(memory, size, getDefaultConfig());
  PoolId poolId = 0;

  s.makeNewSlab(poolId);
  s.makeNewSlab(poolId);
  const auto slab3 = s.makeNewSlab(poolId);
  const auto slab4 = s.makeNewSlab(poolId);

  s.freeSlab(slab3);
  s.freeSlab(slab4);

  auto checkSlabsAndMemoryInSlab = [&](SlabAllocator& sAlloc) {
    const auto first = sAlloc.getSlabForIdx(0);
    const auto second = sAlloc.getSlabForIdx(1);

    ASSERT_TRUE(sAlloc.isValidSlab(first));
    ASSERT_TRUE(sAlloc.isValidSlab(second));
    ASSERT_TRUE(sAlloc.isValidSlab(sAlloc.getSlabForIdx(2)));
    ASSERT_TRUE(sAlloc.isValidSlab(sAlloc.getSlabForIdx(3)));

    for (size_t i = 0; i < Slab::kSize; i++) {
      auto m = first->memoryAtOffset(i);
      ASSERT_FALSE(sAlloc.isMemoryInSlab(m, second));
      ASSERT_TRUE(sAlloc.isMemoryInSlab(m, first));
    }
  };
  checkSlabsAndMemoryInSlab(s);

  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serializer.serialize(s.saveState());

  // Attach to a different address
  void* memory2 = allocate(size);
  ASSERT_NE(memory, memory2);
  memcpy(memory2, memory, size);

  Deserializer deserializer(begin, end);
  const auto correctState =
      deserializer.deserialize<serialization::SlabAllocatorObject>();
  SlabAllocator s2(correctState, memory2, size, getDefaultConfig());
  ASSERT_TRUE(isSameSlabAllocator(s, s2));
  checkSlabsAndMemoryInSlab(s2);

  // now try to come up with some invalid serialized state and ensure that we
  // dont act normal.
  auto invalidSlabSizeState = correctState;
  *invalidSlabSizeState.slabSize() = Slab::kSize * 4;

  ASSERT_THROW(
      SlabAllocator(invalidSlabSizeState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  invalidSlabSizeState = correctState;
  *invalidSlabSizeState.slabSize() = Slab::kSize / 2;

  ASSERT_THROW(
      SlabAllocator(invalidSlabSizeState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  auto invalidMinAllocState = correctState;
  *invalidMinAllocState.minAllocSize() += 20;

  ASSERT_THROW(
      SlabAllocator(invalidMinAllocState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  invalidMinAllocState = correctState;
  *invalidMinAllocState.minAllocSize() -= 20;
  ASSERT_THROW(
      SlabAllocator(invalidMinAllocState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  auto invalidMemorySizeState = correctState;
  *invalidMemorySizeState.memorySize() = Slab::kSize;

  ASSERT_THROW(
      SlabAllocator(invalidMemorySizeState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  // correct memory, incorrect size.
  size_t randomSize = size + ((folly::Random::rand32() % 5) + 1) * Slab::kSize;
  ASSERT_THROW(
      SlabAllocator(correctState, memory, randomSize, getDefaultConfig()),
      std::invalid_argument);

  randomSize = size - ((folly::Random::rand32() % 5) + 1) * Slab::kSize;
  ASSERT_THROW(
      SlabAllocator(correctState, memory, randomSize, getDefaultConfig()),
      std::invalid_argument);

  auto invalidFreeSlabsState = correctState;
  invalidFreeSlabsState.freeSlabIdxs()->push_back(folly::Random::rand32());

  ASSERT_THROW(
      SlabAllocator(invalidFreeSlabsState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  // check the nextSlab allocation outside of the memory bounds.
  auto invalidNextSlabIdxState = correctState;
  *invalidNextSlabIdxState.nextSlabIdx() = 0;
  ASSERT_THROW(
      SlabAllocator(invalidNextSlabIdxState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  *invalidNextSlabIdxState.nextSlabIdx() = -1;
  ASSERT_THROW(
      SlabAllocator(invalidNextSlabIdxState, memory, size, getDefaultConfig()),
      std::invalid_argument);

  *invalidNextSlabIdxState.nextSlabIdx() =
      *correctState.memorySize() / Slab::kSize + 1;

  ASSERT_THROW(
      SlabAllocator(invalidNextSlabIdxState, memory, size, getDefaultConfig()),
      std::invalid_argument);
}

TEST_F(SlabAllocatorTest, Restorable) {
  // save state should work with this type of slab allocator
  const size_t size = 20 * Slab::kSize;
  {
    void* memory = allocate(size);
    SlabAllocator s(memory, size, getDefaultConfig());
    ASSERT_TRUE(s.isRestorable());
    ASSERT_NO_THROW(s.saveState());
  }

  // save state should not work with this type of slab allocator
  {
    SlabAllocator s(size, getDefaultConfig());
    ASSERT_FALSE(s.isRestorable());
    ASSERT_THROW(s.saveState(), std::logic_error);
  }

  {
    void* memory = allocate(size);
    SlabAllocator s(memory, size, getDefaultConfig());
    uint8_t buffer[SerializationBufferSize];
    uint8_t* begin = buffer;
    uint8_t* end = buffer + SerializationBufferSize;
    Serializer serializer(begin, end);
    serializer.serialize(s.saveState());

    // Attach to a different address
    void* memory2 = allocate(size);
    ASSERT_NE(memory, memory2);
    memcpy(memory2, memory, size);

    Deserializer deserializer(begin, end);
    const auto state =
        deserializer.deserialize<serialization::SlabAllocatorObject>();

    SlabAllocator s2(state, memory2, size, getDefaultConfig());
    ASSERT_TRUE(isSameSlabAllocator(s, s2));
    ASSERT_TRUE(s2.isRestorable());

    memset(buffer, 0, sizeof(buffer));
    Serializer serializer2(begin, end);
    serializer2.serialize(s.saveState());

    // Attach to a different address
    void* memory3 = allocate(size);
    ASSERT_NE(memory, memory3);
    memcpy(memory3, memory, size);

    Deserializer deserializer2(begin, end);
    const auto newState =
        deserializer2.deserialize<serialization::SlabAllocatorObject>();

    SlabAllocator s3(newState, memory3, size, getDefaultConfig());
    ASSERT_TRUE(s.isRestorable());
  }
}

TEST_F(SlabAllocatorTest, LockMemory) {
  const size_t size = 20 * Slab::kSize;
  size_t allocSize = size + sizeof(Slab);

  // important to have the mapping as shared to test for this without having
  // to set rlimits
  void* memory = mmap(nullptr, allocSize, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);

  memory = util::align(sizeof(Slab), size, memory, allocSize);

  ASSERT_TRUE(util::isPageAlignedAddr(memory));
  auto config = getDefaultConfig();
  {
    config.lockMemory = false;
    SlabAllocator s(memory, size, config);

    std::this_thread::sleep_for(std::chrono::seconds(3));
    ASSERT_EQ(0, util::getNumResidentPages(memory, size));
  }

  {
    config.lockMemory = true;
    SlabAllocator s(memory, size, config);

    std::this_thread::sleep_for(std::chrono::seconds(3));
    ASSERT_EQ(util::getNumResidentPages(memory, size), util::getNumPages(size));
  }
}

// ensure that we can call save state and have the memory locker thread
// be shut down appropriately.
TEST_F(SlabAllocatorTest, LockMemorySaveState) {
  const size_t size = 200 * Slab::kSize;
  size_t allocSize = size + sizeof(Slab);

  // important to have the mapping as shared to test for this without having
  // to set rlimits
  void* memory = mmap(nullptr, allocSize, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(memory, MAP_FAILED);

  void* alignedMem = util::align(sizeof(Slab), size, memory, allocSize);

  ASSERT_TRUE(util::isPageAlignedAddr(alignedMem));
  {
    auto config = getDefaultConfig();
    config.lockMemory = true;
    SlabAllocator s(alignedMem, size, config);

    // keep the allocator, but call save state, which should ensure that the
    // memory locker is stopped.
    auto state = s.saveState();

    // make sure the memory locker is actually stopped since it might be
    // sleeping, so we wait for some time
    /* sleep override */
    std::this_thread::sleep_for(
        std::chrono::milliseconds(10 * getSlabAllocatorLockSleepMs()));

    munmap(memory, allocSize);
  }
}

TEST_F(SlabAllocatorTest, AdviseRelease) {
  const size_t numSlabs = 50;
  const size_t numAdviseSlabs = 20;
  const size_t size = numSlabs * Slab::kSize;
  size_t allocSize = size + sizeof(Slab);

  // important to have the mapping as shared to test for this without having
  // to set rlimits
  void* memory = mmap(nullptr, allocSize, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);

  ShmManager shmManager("/tmp/test_advise" + std::to_string(::getpid()),
                        false /* use posix */);
  std::string shmName = "testShm_0_";
  shmName += std::to_string(::getpid());
  shmManager.createShm(shmName, allocSize, memory);

  SCOPE_EXIT { shmManager.removeShm(shmName); };

  memory = util::align(Slab::kSize, size, memory, allocSize);

  ASSERT_TRUE(util::isPageAlignedAddr(memory));
  auto config = getDefaultConfig();
  config.lockMemory = true;
  SlabAllocator s(memory, size, config);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(5));
  ASSERT_EQ(util::getNumResidentPages(memory, size), util::getNumPages(size));
  auto memRssBefore = facebook::cachelib::util::getRSSBytes();

  // Use up all but a few slabs so that we have a few free
  for (size_t i = 0; i < numSlabs; i++) {
    s.makeNewSlab(0);
  }
  // No free slabs available
  ASSERT_EQ(nullptr, s.makeNewSlab(0));

  // Advise away slabs
  for (size_t i = 0; i < numAdviseSlabs; i++) {
    s.adviseSlab(s.getSlabForIdx(i));
  }
  ASSERT_EQ(numAdviseSlabs, s.numSlabsReclaimable());
  ASSERT_EQ(util::getNumResidentPages(memory, size),
            util::getNumPages(size - numAdviseSlabs * Slab::kSize));

  auto memRssAfter = facebook::cachelib::util::getRSSBytes();
  ASSERT_TRUE(memRssBefore > memRssAfter);

  // Reclaim half of released memory
  for (size_t i = 0; i < numAdviseSlabs / 2; i++) {
    ASSERT_NE(nullptr, s.reclaimSlab(0));
  }

  ASSERT_EQ(numAdviseSlabs / 2, s.numSlabsReclaimable());
  ASSERT_EQ(util::getNumResidentPages(memory, size),
            util::getNumPages(size - numAdviseSlabs / 2 * Slab::kSize));

  auto memRssAfter1 = facebook::cachelib::util::getRSSBytes();
  ASSERT_TRUE(memRssAfter1 > memRssAfter);

  // Reclaim rest of released memory
  for (size_t i = numAdviseSlabs / 2; i < numAdviseSlabs; i++) {
    ASSERT_NE(nullptr, s.reclaimSlab(0));
  }
  ASSERT_EQ(0, s.numSlabsReclaimable());
  // Reclaiming doesn't change the number of resident pages returned by mincore.
  ASSERT_EQ(util::getNumResidentPages(memory, size), util::getNumPages(size));

  auto memRssAfter2 = facebook::cachelib::util::getRSSBytes();
  ASSERT_TRUE(memRssAfter2 > memRssAfter);
}

void testAdvise(SlabAllocator& s,
                const size_t numSlabs,
                const size_t numAdviseSlabs,
                void* memory,
                const size_t size) {
  ASSERT_EQ(util::getNumResidentPages(memory, size), util::getNumPages(size));
  auto memRssBefore = facebook::cachelib::util::getRSSBytes();

  // Use up all but a few slabs so that we have a few free
  for (size_t i = 0; i < numSlabs; i++) {
    s.makeNewSlab(0);
  }
  // No free slabs available
  ASSERT_EQ(nullptr, s.makeNewSlab(0));

  // Advise away slabs
  for (size_t i = 0; i < numAdviseSlabs; i++) {
    s.adviseSlab(s.getSlabForIdx(i));
  }
  ASSERT_EQ(numAdviseSlabs, s.numSlabsReclaimable());
  ASSERT_EQ(util::getNumResidentPages(memory, size),
            util::getNumPages(size - numAdviseSlabs * Slab::kSize));

  auto memRssAfter = facebook::cachelib::util::getRSSBytes();
  ASSERT_GT(memRssBefore, memRssAfter);
}

void testRestoreAndAdvise(SlabAllocator& s,
                          const size_t numAdviseSlabs,
                          void* memory,
                          const size_t size) {
  ASSERT_EQ(util::getNumResidentPages(memory, size),
            util::getNumPages(size - numAdviseSlabs * Slab::kSize));
  auto memRssBefore = facebook::cachelib::util::getRSSBytes();

  // No free slabs available
  ASSERT_EQ(nullptr, s.makeNewSlab(0));
  ASSERT_EQ(numAdviseSlabs, s.numSlabsReclaimable());

  // Reclaim slabs
  for (size_t i = 0; i < numAdviseSlabs; i++) {
    ASSERT_NE(nullptr, s.reclaimSlab(0));
  }
  ASSERT_EQ(util::getNumResidentPages(memory, size), util::getNumPages(size));

  auto memRssAfter = facebook::cachelib::util::getRSSBytes();
  ASSERT_LT(memRssBefore, memRssAfter);
}

TEST_F(SlabAllocatorTest, AdviseSaveRestore) {
  const size_t numSlabs = 50;
  const size_t numAdviseSlabs = 20;
  const size_t size = numSlabs * Slab::kSize;
  size_t allocSize = size + sizeof(Slab);

  auto cacheDir = "/tmp/test_advise" + std::to_string(::getpid());
  std::string shmName = "testShm_1_";
  shmName += std::to_string(::getpid());
  serialization::SlabAllocatorObject state;

  auto config = getDefaultConfig();
  config.lockMemory = true;

  // important to have the mapping as shared to test for this without having
  // to set rlimits
  void* memory = mmap(nullptr, allocSize, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);

  memory = util::align(Slab::kSize, size, memory, allocSize);
  ASSERT_TRUE(util::isPageAlignedAddr(memory));

  ShmManager shmManager(cacheDir, false /* posix */);
  shmManager.createShm(shmName, allocSize, memory);

  SCOPE_EXIT { shmManager.removeShm(shmName); };

  {
    SlabAllocator s(memory, size, config);
    // Wait until memory locking completes.
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(5));
    testAdvise(s, numSlabs, numAdviseSlabs, memory, size);
    state = s.saveState();
  }

  {
    SlabAllocator r(state, memory, size, config);
    // Wait until memory locking completes.
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(5));
    testRestoreAndAdvise(r, numAdviseSlabs, memory, size);
  }
}

TEST_F(SlabAllocatorTest, ReducedFragmentationUniqueChunksPerSlab) {
  auto runTest = [](double factor, uint32_t maxSize, uint32_t minSize) {
    auto allocSizes =
        MemoryAllocator::generateAllocSizes(factor, maxSize, minSize, true);
    ASSERT_FALSE(allocSizes.empty());
    EXPECT_EQ(minSize, *allocSizes.begin());
    // Verify each alloc size has a different number of chunks per slabs
    uint32_t lastPerSlab = 0;
    for (auto allocSize : allocSizes) {
      EXPECT_NE(lastPerSlab, Slab::kSize / allocSize);
      lastPerSlab = Slab::kSize / allocSize;
    }
  };
  constexpr auto kOneMB = 1024 * 1024;
  runTest(1.07, kOneMB, 64);
  runTest(1.07, kOneMB, 72);
  runTest(1.07, Slab::kSize, 64);
  runTest(1.07, Slab::kSize, 72);
  runTest(1.25, kOneMB, 64);
  runTest(1.25, kOneMB, 72);
  runTest(1.25, Slab::kSize, 64);
  runTest(1.25, Slab::kSize, 72);
}

TEST_F(SlabAllocatorTest, TestAlignedSize) {
  for (uint32_t i = 57; i <= 64; ++i) {
    EXPECT_EQ(64, util::getAlignedSize(i, 8));
  }
  for (uint32_t i = 65; i <= 72; ++i) {
    EXPECT_EQ(72, util::getAlignedSize(i, 8));
  }
}

TEST_F(SlabAllocatorTest, TestGenerateAllocSizesWithBadFactor) {
  uint32_t minSize = 64;
  uint32_t maxSize = 104;
  ASSERT_THROW(
      MemoryAllocator::generateAllocSizes(1.01, maxSize, minSize, false),
      std::invalid_argument);
  ASSERT_THROW(
      MemoryAllocator::generateAllocSizes(1.01, maxSize, minSize, true),
      std::invalid_argument);
  ASSERT_THROW(
      MemoryAllocator::generateAllocSizes(0.90, maxSize, minSize, true),
      std::invalid_argument);
}
