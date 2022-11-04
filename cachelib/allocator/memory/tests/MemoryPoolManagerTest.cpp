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

#include <memory>
#include <random>

#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/allocator/memory/tests/TestBase.h"
#include "cachelib/common/Serialization.h"

using namespace facebook::cachelib;
using namespace facebook::cachelib::tests;

using MemoryPoolManagerTest = SlabAllocatorTestBase;

constexpr size_t SerializationBufferSize = 100 * 1024;

TEST_F(MemoryPoolManagerTest, CreatePools) {
  auto slabAlloc = createSlabAllocator(50);
  auto nUsable = slabAlloc->getNumUsableSlabs();
  MemoryPoolManager m(*slabAlloc);

  // add some pools by name

  const std::string poolName1 = "foo";
  const std::string poolName2 = "bar";

  // create two pools with 20 slabs worth of memory.
  PoolId pool1Id = Slab::kInvalidPoolId;
  size_t poolSize1 = 20 * Slab::kSize;

  auto allocSizes1 = getRandomAllocSizes(5);

  PoolId pool2Id = Slab::kInvalidPoolId;
  size_t poolSize2 = 20 * Slab::kSize;

  auto allocSizes2 = getRandomAllocSizes(10);

  ASSERT_NO_THROW(pool1Id = m.createNewPool(poolName1, poolSize1, allocSizes1));
  ASSERT_NO_THROW(pool2Id = m.createNewPool(poolName2, poolSize2, allocSizes2));

  MemoryPool& pool1 = m.getPoolById(pool1Id);
  MemoryPool& pool2 = m.getPoolById(pool2Id);

  ASSERT_EQ(pool1.getPoolSize(), poolSize1);
  ASSERT_EQ(pool2.getPoolSize(), poolSize2);
  // pools should have unique id.
  ASSERT_NE(pool1.getId(), pool2.getId());

  // trying to create a new pool with not enough memory left should throw
  const size_t remaining = (nUsable * Slab::kSize) - poolSize1 - poolSize2;
  ASSERT_GT(remaining, 0);
  ASSERT_THROW(
      m.createNewPool("bazz", remaining + Slab::kSize, getRandomAllocSizes(4)),
      std::invalid_argument);

  // trying to create pools with duplicate names should fail.
  ASSERT_THROW(m.createNewPool(poolName1, Slab::kSize, getRandomAllocSizes(3)),
               std::invalid_argument);

  const std::string poolName3 = "bazz";
  PoolId pool3Id = Slab::kInvalidPoolId;
  ASSERT_NO_THROW(
      pool3Id = m.createNewPool(poolName3, remaining, getRandomAllocSizes(4)));
  auto& pool3 = m.getPoolById(pool3Id);
  ASSERT_NE(pool1.getId(), pool3.getId());
  ASSERT_NE(pool2.getId(), pool3.getId());

  // any more pool creations should fail since we are out of memory in slab
  // allocator.
  ASSERT_THROW(m.createNewPool("another", Slab::kSize, getRandomAllocSizes(4)),
               std::invalid_argument);
}

// ensure that we can not create more than the maximum number of pools.
TEST_F(MemoryPoolManagerTest, MaxPools) {
  const unsigned int maxPools = MemoryPoolManager::kMaxPools;
  auto slabAlloc = createSlabAllocator(5 * maxPools);
  auto nUsable = slabAlloc->getNumUsableSlabs();
  MemoryPoolManager m(*slabAlloc);
  const unsigned int numSlabsPerPool = 2;
  ASSERT_GT(nUsable, maxPools * numSlabsPerPool);

  for (unsigned int i = 0; i < maxPools; i++) {
    ASSERT_GT(nUsable, numSlabsPerPool);
    ASSERT_NO_THROW(m.createNewPool(
        getRandomStr(), numSlabsPerPool * Slab::kSize, getRandomAllocSizes(3)));
    nUsable -= numSlabsPerPool;
  }

  ASSERT_EQ(m.getPoolIds().size(), MemoryPoolManager::kMaxPools);
  ASSERT_GT(nUsable, 0);
  ASSERT_THROW(m.createNewPool(getRandomStr(), numSlabsPerPool * Slab::kSize,
                               getRandomAllocSizes(3)),
               std::logic_error);
}

// test getPoolById and getPoolByName.
TEST_F(MemoryPoolManagerTest, GetPoolBy) {
  const unsigned int nPools = 20;
  ASSERT_GE(MemoryPoolManager::kMaxPools, nPools);
  const unsigned int numSlabsPerPool = 2;

  auto slabAlloc = createSlabAllocator(5 * nPools);
  auto nUsable = slabAlloc->getNumUsableSlabs();
  MemoryPoolManager m(*slabAlloc);

  std::unordered_map<std::string, MemoryPool&> pools;
  for (unsigned int i = 0; i < nPools; i++) {
    // string of random length up to 10.
    ASSERT_GT(nUsable, numSlabsPerPool);
    auto name = getRandomStr();
    auto id = m.createNewPool(name, numSlabsPerPool * Slab::kSize,
                              getRandomAllocSizes(4));
    auto& pool = m.getPoolById(id);
    pools.insert({name, pool});
    nUsable -= numSlabsPerPool;
  }

  ASSERT_EQ(nPools, m.getPoolIds().size());

  for (auto kv : pools) {
    auto name = kv.first;
    auto& pool = kv.second;
    ASSERT_EQ(&m.getPoolByName(name), &pool);
    ASSERT_EQ(&m.getPoolById(pool.getId()), &pool);
  }

  // invalid pool id.
  ASSERT_THROW(m.getPoolById(-1), std::invalid_argument);
  ASSERT_THROW(m.getPoolById(MemoryPoolManager::kMaxPools),
               std::invalid_argument);
}

TEST_F(MemoryPoolManagerTest, Serialization) {
  const unsigned int nPools = 20;
  ASSERT_GE(MemoryPoolManager::kMaxPools, nPools);
  const unsigned int numSlabsPerPool = 2;

  auto slabAlloc = createSlabAllocator(5 * nPools);
  auto nUsable = slabAlloc->getNumUsableSlabs();
  MemoryPoolManager m(*slabAlloc);

  std::unordered_map<std::string, MemoryPool&> pools;
  for (unsigned int i = 0; i < nPools; i++) {
    // string of random length up to 10.
    ASSERT_GT(nUsable, numSlabsPerPool);
    auto name = getRandomStr();
    auto id = m.createNewPool(name, numSlabsPerPool * Slab::kSize,
                              getRandomAllocSizes(4));
    auto& pool = m.getPoolById(id);
    pools.insert({name, pool});
    nUsable -= numSlabsPerPool;
  }

  ASSERT_EQ(nPools, m.getPoolIds().size());

  for (auto kv : pools) {
    auto name = kv.first;
    auto& pool = kv.second;
    ASSERT_EQ(&m.getPoolByName(name), &pool);
    ASSERT_EQ(&m.getPoolById(pool.getId()), &pool);
  }

  // invalid pool id.
  ASSERT_THROW(m.getPoolById(-1), std::invalid_argument);
  ASSERT_THROW(m.getPoolById(MemoryPoolManager::kMaxPools),
               std::invalid_argument);

  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serializer.serialize(m.saveState());

  Deserializer deserializer(begin, end);
  MemoryPoolManager m2(
      deserializer.deserialize<serialization::MemoryPoolManagerObject>(),
      *slabAlloc);
  ASSERT_TRUE(isSameMemoryPoolManager(m, m2));

  for (auto kv : pools) {
    auto name = kv.first;
    auto& pool = kv.second;

    auto& poolByName = m2.getPoolByName(name);
    ASSERT_EQ(poolByName.getId(), pool.getId());
    ASSERT_EQ(poolByName.getPoolSize(), pool.getPoolSize());
    ASSERT_EQ(poolByName.getCurrentAllocSize(), pool.getCurrentAllocSize());
    ASSERT_EQ(poolByName.getNumClassId(), pool.getNumClassId());

    auto& poolById = m2.getPoolById(pool.getId());
    ASSERT_EQ(poolById.getId(), pool.getId());
    ASSERT_EQ(poolById.getPoolSize(), pool.getPoolSize());
    ASSERT_EQ(poolById.getCurrentAllocSize(), pool.getCurrentAllocSize());
    ASSERT_EQ(poolById.getNumClassId(), pool.getNumClassId());
  }
}

TEST_F(MemoryPoolManagerTest, ResizePools) {
  const unsigned int nPools = 5;
  ASSERT_GE(MemoryPoolManager::kMaxPools, nPools);
  const unsigned int numSlabsPerPool = 10;

  auto slabAlloc = createSlabAllocator(numSlabsPerPool * nPools + 10);
  auto nUsable = slabAlloc->getNumUsableSlabs();
  MemoryPoolManager m(*slabAlloc);

  std::vector<MemoryPool*> pools;
  for (unsigned int i = 0; i < nPools; i++) {
    // string of random length up to 10.
    ASSERT_GT(nUsable, numSlabsPerPool);
    auto name = getRandomStr();
    auto id = m.createNewPool(name, numSlabsPerPool * Slab::kSize,
                              getRandomAllocSizes(4));
    auto& pool = m.getPoolById(id);
    pools.push_back(&pool);
    nUsable -= numSlabsPerPool;
  }

  ASSERT_EQ(nPools, m.getPoolIds().size());

  int ntries = 10;
  while (ntries--) {
    for (unsigned int i = 0; i < nPools; i++) {
      unsigned int other = folly::Random::rand32() % nPools;
      if (i == other) {
        continue;
      }

      const auto iSize = pools[i]->getPoolSize();
      const auto otherSize = pools[other]->getPoolSize();

      if (iSize == 0) {
        continue;
      }

      const auto bytesToMove = iSize / 2;
      ASSERT_TRUE(
          m.resizePools(pools[i]->getId(), pools[other]->getId(), bytesToMove));
      ASSERT_EQ(iSize - bytesToMove, pools[i]->getPoolSize());
      ASSERT_EQ(otherSize + bytesToMove, pools[other]->getPoolSize());
    }
  }

  // nPool is an invalid pool id.
  ASSERT_THROW(m.getPoolById(nPools), std::invalid_argument);

  // src or dest pools invalid.
  ASSERT_THROW(m.resizePools(nPools, 0, 1024), std::invalid_argument);
  ASSERT_THROW(m.resizePools(0, nPools, 1024), std::invalid_argument);

  // moving more than what we own.
  ASSERT_FALSE(m.resizePools(pools[0]->getId(), pools[1]->getId(),
                             pools[0]->getPoolSize() + 100));
  ASSERT_TRUE(m.resizePools(pools[0]->getId(), pools[1]->getId(),
                            pools[0]->getPoolSize()));

  ASSERT_EQ(0, pools[0]->getPoolSize());
}

TEST_F(MemoryPoolManagerTest, PoolOverLimit) {
  const unsigned int nPools = 5;
  ASSERT_GE(MemoryPoolManager::kMaxPools, nPools);
  const unsigned int numSlabsPerPool = 10;

  auto slabAlloc = createSlabAllocator(numSlabsPerPool * nPools + 10);
  auto nUsable = slabAlloc->getNumUsableSlabs();
  MemoryPoolManager m(*slabAlloc);
  const auto& allocSizes = getRandomAllocSizes(4);

  // create some pools.
  std::vector<MemoryPool*> pools;
  for (unsigned int i = 0; i < nPools; i++) {
    // string of random length up to 10.
    ASSERT_GT(nUsable, numSlabsPerPool);
    auto name = getRandomStr();
    auto id = m.createNewPool(name, numSlabsPerPool * Slab::kSize, allocSizes);
    auto& pool = m.getPoolById(id);
    pools.push_back(&pool);
    nUsable -= numSlabsPerPool;
  }

  ASSERT_LT(Slab::kSize, m.getBytesUnReserved());
  unsigned int nAvailable = m.getBytesUnReserved() / Slab::kSize;
  ASSERT_LT(3, nAvailable);

  // pools are mostly empty. So nothing is over limit.
  ASSERT_EQ(std::set<PoolId>({}), m.getPoolsOverLimit());

  // shrink pools 0 and 1, 2
  m.shrinkPool(0, Slab::kSize);
  m.shrinkPool(1, Slab::kSize);
  m.shrinkPool(2, Slab::kSize);

  // pools are mostly empty. So nothing is over limit.
  ASSERT_EQ(std::set<PoolId>({}), m.getPoolsOverLimit());

  auto fillUpPool = [&](PoolId pid) {
    while (!pools[pid]->allSlabsAllocated()) {
      for (auto size : allocSizes) {
        pools[pid]->allocate(size);
      }
    }
  };

  fillUpPool(0);
  fillUpPool(1);
  fillUpPool(2);

  // pools are filled. but nothing is over limit.
  ASSERT_EQ(std::set<PoolId>({}), m.getPoolsOverLimit());

  // try to fill up some of these pools and observe that they are reflected as
  // being above the limit.
  m.shrinkPool(0, Slab::kSize);
  ASSERT_EQ(std::set<PoolId>({0}), m.getPoolsOverLimit());

  m.shrinkPool(2, Slab::kSize);
  ASSERT_EQ(std::set<PoolId>({0, 2}), m.getPoolsOverLimit());

  fillUpPool(3);
  ASSERT_EQ(std::set<PoolId>({0, 2}), m.getPoolsOverLimit());

  fillUpPool(2);
  ASSERT_EQ(std::set<PoolId>({0, 2}), m.getPoolsOverLimit());

  m.growPool(2, Slab::kSize);
  ASSERT_EQ(std::set<PoolId>({0}), m.getPoolsOverLimit());

  fillUpPool(4);
  m.resizePools(3, 4, 5 * Slab::kSize);
  ASSERT_EQ(std::set<PoolId>({0, 3}), m.getPoolsOverLimit());
}
