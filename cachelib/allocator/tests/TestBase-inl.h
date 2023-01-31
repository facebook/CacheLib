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

#include <fstream>
#include <iostream>
namespace facebook {
namespace cachelib {
namespace tests {

template <typename AllocatorT>
std::string AllocatorTest<AllocatorT>::getRandomNewKey(AllocatorT& alloc,
                                                       unsigned int keyLen) {
  auto key = facebook::cachelib::test_util::getRandomAsciiStr(keyLen);
  while (alloc.find(key) != nullptr) {
    key = facebook::cachelib::test_util::getRandomAsciiStr(keyLen);
  }
  return key;
}

template <typename AllocatorT>
std::string AllocatorTest<AllocatorT>::createFileForAllocator(size_t size) {
  const std::string fileName = this->cacheDir_ + "/" + "cache_mmap_file";
  std::ofstream file(fileName);
  std::fill_n(std::ostream_iterator<uint8_t>(file), size, 0);
  return fileName;
}

template <typename AllocatorT>
std::vector<uint32_t> AllocatorTest<AllocatorT>::getValidAllocSizes(
    AllocatorT& alloc,
    PoolId poolId,
    unsigned int nSizes,
    unsigned int keyLen) {
  std::vector<uint32_t> sizes;
  while (sizes.size() != nSizes) {
    const auto size = getRandomAllocSize();
    const auto key = getRandomNewKey(alloc, keyLen);
    // try to ensure that making allocations with fixed key len and our chosen
    // size is a valid allocation size. If not, try again.
    try {
      util::allocateAccessible(alloc, poolId, key, size);
    } catch (const std::invalid_argument& e) {
      continue;
    }
    sizes.push_back(size);
  }
  std::sort(sizes.begin(), sizes.end());
  assert(sizes.back() + keyLen + sizeof(typename AllocatorT::Item) <=
         facebook::cachelib::Slab::kSize);
  return sizes;
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::fillUpOneSlab(AllocatorT& alloc,
                                              PoolId poolId,
                                              const uint32_t acSize,
                                              unsigned int keyLen) {
  auto size = acSize - keyLen - sizeof(typename AllocatorT::Item);
  for (size_t i = 0; i < facebook::cachelib::Slab::kSize / acSize; i++) {
    const auto key = getRandomNewKey(alloc, keyLen);
    auto handle = util::allocateAccessible(alloc, poolId, key, size);
  }
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::fillUpPoolUntilEvictions(
    AllocatorT& alloc,
    PoolId poolId,
    const std::vector<uint32_t>& sizes,
    unsigned int keyLen) {
  unsigned int allocs = 0;
  do {
    allocs = 0;
    for (const auto size : sizes) {
      const auto key = getRandomNewKey(alloc, keyLen);
      ASSERT_EQ(alloc.find(key), nullptr);
      const size_t prev = alloc.getPool(poolId).getCurrentAllocSize();
      auto handle = util::allocateAccessible(alloc, poolId, key, size);
      if (handle && prev != alloc.getPool(poolId).getCurrentAllocSize()) {
        // this means we did not cause an eviction.
        ASSERT_GE(handle->getSize(), size);
        allocs++;
      }
    }
  } while (allocs != 0);
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::testAllocWithoutEviction(
    AllocatorT& alloc,
    PoolId poolId,
    const std::vector<uint32_t>& sizes,
    unsigned int keyLen) {
  for (const auto size : sizes) {
    const auto key = getRandomNewKey(alloc, keyLen);
    ASSERT_EQ(alloc.find(key), nullptr);

    const size_t prev = alloc.getPool(poolId).getCurrentAllocSize();
    auto handle = util::allocateAccessible(alloc, poolId, key, size);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(prev, alloc.getPool(poolId).getCurrentAllocSize());
  }
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::ensureAllocsOnlyFromEvictions(
    AllocatorT& alloc,
    PoolId poolId,
    const std::vector<uint32_t>& sizes,
    unsigned int keyLen,
    size_t totalAllocSize,
    bool check) {
  size_t currentPoolAllocatedSize = alloc.getPool(poolId).getCurrentAllocSize();
  size_t allocBytes = 0;
  while (allocBytes < totalAllocSize) {
    for (const auto size : sizes) {
      // get a key that does not exist already.
      const auto key = getRandomNewKey(alloc, keyLen);
      ASSERT_EQ(alloc.find(key), nullptr);
      if (check) {
        ASSERT_EQ(alloc.getPool(poolId).getCurrentAllocSize(),
                  currentPoolAllocatedSize);
      }

      auto handle = util::allocateAccessible(alloc, poolId, key, size);
      ASSERT_NE(handle, nullptr);
      // this means that we are not growing the pool any more and we are just
      // evicting.
      if (check) {
        ASSERT_EQ(alloc.getPool(poolId).getCurrentAllocSize(),
                  currentPoolAllocatedSize);
      }
      allocBytes += handle->getSize();
    }
  }
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::testLruLength(
    AllocatorT& alloc,
    PoolId poolId,
    const std::vector<uint32_t>& sizes,
    size_t keyLen,
    const std::set<std::string>& evictedKeys) {
  fillUpPoolUntilEvictions(alloc, poolId, sizes, keyLen);

  std::vector<size_t> lruLengths;
  for (size_t size : sizes) {
    size_t lruLength = 0;
    estimateLruSize(alloc, poolId, size, keyLen, evictedKeys, lruLength);
    ASSERT_GT(lruLength, 0);

    lruLengths.push_back(lruLength);
  }

  std::vector<size_t> newLruLengths;
  for (size_t size : sizes) {
    size_t lruLength = 0;
    estimateLruSize(alloc, poolId, size, keyLen, evictedKeys, lruLength);
    ASSERT_GT(lruLength, 0);

    newLruLengths.push_back(lruLength);
  }

  ASSERT_EQ(lruLengths, newLruLengths);
}

// given an allocator that is prepped for evictions, tries to estimate the
// size of the lru by watching the keys that are evicted from it.
template <typename AllocatorT>
void AllocatorTest<AllocatorT>::estimateLruSize(
    AllocatorT& alloc,
    PoolId poolId,
    size_t size,
    size_t keyLen,
    const std::set<std::string>& evictedKeys,
    size_t& outLruSize) {
  // try to determine the length of the lru for each alloc size. we do this by
  // trying to create a new item and insert it and see after how many
  // allocations, it gets evicted.
  unsigned int lruLength = 0;
  auto markerKey = getRandomNewKey(alloc, keyLen);
  {
    auto handle = util::allocateAccessible(alloc, poolId, markerKey, size);
    ASSERT_NE(handle, nullptr);
  }

  // this should bump it to the head of the lru.
  alloc.find(markerKey);

  ASSERT_EQ(evictedKeys.find(markerKey), evictedKeys.end());

  while (evictedKeys.find(markerKey) == evictedKeys.end()) {
    // potentially this can collide with the original key and screw up our
    // numbers, but its fine.
    util::allocateAccessible(alloc, poolId, getRandomNewKey(alloc, keyLen),
                             size);
    lruLength++;
  }
  ASSERT_NE(lruLength, 0);
  outLruSize = lruLength;
}

// This test basically makes sure allocations made during the first
// instance of the cache allocator can still be accessed when we
// shut it down and restore it (i.e. in the second instance)
template <typename AllocatorT>
void AllocatorTest<AllocatorT>::runSerializationTest(
    typename AllocatorT::Config config) {
  std::set<std::string> evictedKeys;
  auto evictCb = [&evictedKeys](const typename AllocatorT::RemoveCbData& data) {
    if (data.context == RemoveContext::kEviction) {
      const auto key = data.item.getKey();
      evictedKeys.insert({key.data(), key.size()});
    }
  };
  config.setRemoveCallback(evictCb);

  const size_t nSlabs = 20;
  const size_t size = nSlabs * Slab::kSize;
  const unsigned int nSizes = 5;
  const unsigned int keyLen = 100;

  config.setCacheSize(size);
  config.enableCachePersistence(cacheDir_);

  std::vector<uint32_t> sizes;
  uint8_t poolId;

  // Test allocations. These allocations should remain after save/restore.
  // Original lru allocator
  std::vector<std::string> keys;
  {
    AllocatorT alloc(AllocatorT::SharedMemNew, config);
    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    poolId = alloc.addPool("foobar", numBytes);
    sizes = getValidAllocSizes(alloc, poolId, nSlabs, keyLen);
    fillUpPoolUntilEvictions(alloc, poolId, sizes, keyLen);
    for (const auto& item : alloc) {
      auto key = item.getKey();
      keys.push_back(key.str());
    }

    // save
    alloc.shutDown();
  }

  testShmIsNotRemoved(config);

  // Restored lru allocator
  {
    AllocatorT alloc(AllocatorT::SharedMemAttach, config);
    for (auto& key : keys) {
      auto handle = alloc.find(typename AllocatorT::Key{key});
      ASSERT_NE(nullptr, handle.get());
    }
  }

  testShmIsRemoved(config);

  // Test LRU eviction and length before and after save/restore
  // Original lru allocator
  {
    AllocatorT alloc(AllocatorT::SharedMemNew, config);
    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    poolId = alloc.addPool("foobar", numBytes);

    sizes = getValidAllocSizes(alloc, poolId, nSizes, keyLen);

    testLruLength(alloc, poolId, sizes, keyLen, evictedKeys);

    // save
    alloc.shutDown();
  }
  evictedKeys.clear();

  testShmIsNotRemoved(config);

  // Restored lru allocator
  {
    AllocatorT alloc(AllocatorT::SharedMemAttach, config);
    testLruLength(alloc, poolId, sizes, keyLen, evictedKeys);
  }

  testShmIsRemoved(config);
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::testInfoShmIsRemoved(
    typename AllocatorT::Config config) {
  ASSERT_FALSE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmInfoName, config.usePosixShm));
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::testShmIsRemoved(
    typename AllocatorT::Config config) {
  testInfoShmIsRemoved(config);
  ASSERT_FALSE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmHashTableName, config.usePosixShm));
  ASSERT_FALSE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmCacheName, config.usePosixShm));
  ASSERT_FALSE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmChainedItemHashTableName,
      config.usePosixShm));
}

template <typename AllocatorT>
void AllocatorTest<AllocatorT>::testShmIsNotRemoved(
    typename AllocatorT::Config config) {
  ASSERT_TRUE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmInfoName, config.usePosixShm));
  ASSERT_TRUE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmHashTableName, config.usePosixShm));
  ASSERT_TRUE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmCacheName, config.usePosixShm));
  ASSERT_TRUE(AllocatorT::ShmManager::segmentExists(
      config.getCacheDir(), detail::kShmChainedItemHashTableName,
      config.usePosixShm));
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
