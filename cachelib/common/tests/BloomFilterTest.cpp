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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/common/BloomFilter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {

TEST(BloomFilter, OptimalParams) {
  {
    auto bf = BloomFilter::makeBloomFilter(1000, 25, 0.1);

    EXPECT_EQ(1000, bf.numFilters());
    // this disagrees with the observation in NavySetup.cpp that 4 hashes with
    // 16 bytes per filter is good for 25 elems and 0.1 fp rate
    EXPECT_EQ(120, bf.numBitsPerFilter());
    EXPECT_EQ(3, bf.numHashes());
  }

  {
    auto bf = BloomFilter::makeBloomFilter(6, 200'000'000, 0.02);
    EXPECT_EQ(6, bf.numFilters());
    EXPECT_EQ(1630310280, bf.numBitsPerFilter());
    EXPECT_EQ(6, bf.numHashes());
  }
}

TEST(BloomFilter, Default) {
  BloomFilter bf{};

  EXPECT_EQ(0, bf.numFilters());
  EXPECT_EQ(0, bf.numBitsPerFilter());
  EXPECT_EQ(0, bf.numHashes());
  EXPECT_EQ(0, bf.getByteSize());

  auto key = folly::Random::rand64();
  EXPECT_TRUE(bf.couldExist(1, key));
  EXPECT_TRUE(bf.couldExist(1, key + 1));

  bf.set(1, key);
  EXPECT_TRUE(bf.couldExist(1, key));
  EXPECT_TRUE(bf.couldExist(1, key + 1));
}

// add some keys, move and verify the move transfers the state
TEST(BloomFilter, Move) {
  uint32_t numFilters = 10;
  BloomFilter bf = BloomFilter::makeBloomFilter(numFilters, 1000, 0.01);
  auto numHashes = bf.numHashes();
  auto numBits = bf.numBitsPerFilter();
  std::vector<uint64_t> addedKeys;

  auto idx = [&](uint64_t key) { return key % numHashes; };
  for (int i = 0; i < 1000; i++) {
    auto key = folly::Random::rand64();
    bf.set(idx(key), key);
    EXPECT_TRUE(bf.couldExist(idx(key), key));
    addedKeys.push_back(key);
  }

  std::vector<uint64_t> nonExistentKeys;
  while (nonExistentKeys.size() < addedKeys.size()) {
    auto key = folly::Random::rand64();
    if (!bf.couldExist(idx(key), key)) {
      nonExistentKeys.push_back(key);
    }
  }

  auto newBf = std::move(bf);
  EXPECT_EQ(0, bf.numHashes());

  EXPECT_EQ(numFilters, newBf.numFilters());
  EXPECT_EQ(numBits, newBf.numBitsPerFilter());
  EXPECT_EQ(numHashes, newBf.numHashes());
  for (auto k : addedKeys) {
    EXPECT_TRUE(newBf.couldExist(idx(k), k));
  }

  for (auto k : nonExistentKeys) {
    EXPECT_FALSE(newBf.couldExist(idx(k), k));
  }
}

TEST(BloomFilter, Reset) {
  BloomFilter bf{4, 2, 4};
  EXPECT_EQ(4, bf.getByteSize());
  for (uint32_t i = 0; i < 4; i++) {
    for (uint64_t key = 0; key < 10; key++) {
      bf.set(i, key);
    }
  }

  for (uint32_t i = 0; i < 4; i++) {
    for (uint64_t key = 0; key < 10; key++) {
      EXPECT_TRUE(bf.couldExist(i, key));
    }
  }

  bf.reset();

  // reset should make the bloom filter return negative on all keys
  for (uint32_t i = 0; i < 4; i++) {
    for (uint64_t key = 0; key < 10; key++) {
      EXPECT_FALSE(bf.couldExist(i, key));
    }
  }
}

TEST(BloomFilter, SimpleCollision) {
  BloomFilter bf{4, 2, 4};
  EXPECT_EQ(4, bf.getByteSize());
  for (uint32_t i = 0; i < 4; i++) {
    bf.set(i, 1);
    {
      uint64_t key = 1;
      EXPECT_EQ(0, facebook::cachelib::combineHashes(key, hashInt(0)) % 4);
      EXPECT_EQ(3, facebook::cachelib::combineHashes(key, hashInt(1)) % 4);
      EXPECT_TRUE(bf.couldExist(i, key));
    }
    {
      uint64_t key = 3;
      EXPECT_EQ(2, facebook::cachelib::combineHashes(key, hashInt(0)) % 4);
      EXPECT_EQ(2, facebook::cachelib::combineHashes(key, hashInt(1)) % 4);
      EXPECT_FALSE(bf.couldExist(i, key));
    }
    {
      uint64_t key = 33;
      EXPECT_EQ(0, facebook::cachelib::combineHashes(key, hashInt(0)) % 4);
      EXPECT_EQ(3, facebook::cachelib::combineHashes(key, hashInt(1)) % 4);
      EXPECT_TRUE(bf.couldExist(i, key)); // Collision
    }
    // For index 1, check clearing others doesn't affect it
    if (i == 1) {
      bf.clear(0);
      bf.clear(2);
      bf.clear(3);
      EXPECT_TRUE(bf.couldExist(i, 1));
      EXPECT_FALSE(bf.couldExist(i, 2));
      EXPECT_FALSE(bf.couldExist(i, 3));
    }
    bf.clear(i);
    EXPECT_FALSE(bf.couldExist(i, 1));
  }
}

TEST(BloomFilter, SharedCollision) {
  BloomFilter bf{1, 2, 4};
  EXPECT_EQ(1, bf.getByteSize());
  EXPECT_EQ(0, facebook::cachelib::combineHashes(1, hashInt(0)) %
                   4); // Bit 0 in 1st hash table
  EXPECT_EQ(3, facebook::cachelib::combineHashes(1, hashInt(1)) %
                   4); // Bit 3 in 2nd hash table
  bf.set(0, 1);
  EXPECT_EQ(2, facebook::cachelib::combineHashes(3, hashInt(0)) %
                   4); // Bit 2 in 1st hash table
  EXPECT_EQ(2, facebook::cachelib::combineHashes(3, hashInt(1)) %
                   4); // Bit 2 in 2nd hash table
  bf.set(0, 3);

  // BloomFilter looks like:
  //   bit# 0 1 2 3
  // [HT 0] 1 0 1 0
  // [HT 1] 0 0 1 1

  // Try key = 18: 1st bit #2, 2nd bit #3.
  {
    uint64_t key = 18;
    EXPECT_EQ(2, facebook::cachelib::combineHashes(key, hashInt(0)) % 4);
    EXPECT_EQ(3, facebook::cachelib::combineHashes(key, hashInt(1)) % 4);
    EXPECT_TRUE(bf.couldExist(0, key));
  }
  // Try key = 15: 1st bit #0, 2nd bit #2.
  {
    uint64_t key = 15;
    EXPECT_EQ(0, facebook::cachelib::combineHashes(key, hashInt(0)) % 4);
    EXPECT_EQ(2, facebook::cachelib::combineHashes(key, hashInt(1)) % 4);
    EXPECT_TRUE(bf.couldExist(0, key));
  }
}

TEST(BloomFilter, InvalidArgs) {
  EXPECT_NO_THROW(BloomFilter(2, 2, 3));
  EXPECT_THROW(BloomFilter(0, 2, 2), std::invalid_argument);
  EXPECT_THROW(BloomFilter(2, 0, 2), std::invalid_argument);
  EXPECT_THROW(BloomFilter(2, 2, 0), std::invalid_argument);
}

TEST(BloomFilter, Clear) {
  BloomFilter bf{2, 2, 4};

  // By default every filter is assumed valid
  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_FALSE(bf.couldExist(1, 100 + i));
  }

  // Invalidate bucket at index 1, and it will always return "could exist"
  // until we mark it initialized
  bf.clear(1);

  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_FALSE(bf.couldExist(1, 100 + i));
  }

  bf.set(1, 100);

  EXPECT_TRUE(bf.couldExist(1, 100));
  EXPECT_TRUE(bf.couldExist(1, 103)); // False positive
  for (uint32_t i = 1; i < 16; i++) {
    if (i != 3) {
      EXPECT_FALSE(bf.couldExist(1, 100 + i));
    }
  }

  // Clear resets init bit. We can tell this because we set key after clear
  // and we don't see effect of it set.
  bf.clear(1);
  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_FALSE(bf.couldExist(1, 100 + i));
  }
}

TEST(BloomFilter, PersistRecoverWithInvalidParams) {
  const size_t numFilters = 10;
  const size_t bitsPerFilter = 16;
  const size_t numHash = 1;
  const int numKeys = 1024;

  auto makeBf = [=]() {
    BloomFilter bf{numFilters, 1, 8};
    for (int i = 0; i < numKeys; i++) {
      auto key = folly::Random::rand64();
      auto idx = folly::Random::rand64() % numFilters;
      bf.set(idx, key);
    }

    folly::IOBufQueue queue;
    auto rw = createMemoryRecordWriter(queue);
    bf.persist<apache::thrift::BinarySerializer>(*rw);
    return queue;
  };

  // recovery with incorrect params should fail
  {
    {
      auto queue = makeBf();
      auto rr = createMemoryRecordReader(queue);
      BloomFilter bf(numFilters + 1, numHash, bitsPerFilter);
      ASSERT_THROW(bf.recover<apache::thrift::BinarySerializer>(*rr),
                   std::invalid_argument);
    }
    {
      auto queue = makeBf();
      auto rr = createMemoryRecordReader(queue);
      BloomFilter bf(numFilters, numHash + 1, bitsPerFilter);
      ASSERT_THROW(bf.recover<apache::thrift::BinarySerializer>(*rr),
                   std::invalid_argument);
    }
    {
      auto queue = makeBf();
      auto rr = createMemoryRecordReader(queue);
      BloomFilter bf(numFilters, numHash, bitsPerFilter + 8);
      ASSERT_THROW(bf.recover<apache::thrift::BinarySerializer>(*rr),
                   std::invalid_argument);
    }
  }
}

void testPersistRecoveryWithParams(uint32_t numFilters,
                                   size_t bitsPerFilterHash,
                                   uint32_t numHash) {
  const int numKeys = 1024;

  std::vector<std::pair<size_t, uint64_t>> keysAdded;
  std::vector<std::pair<size_t, uint64_t>> keysNotPresent;
  folly::IOBufQueue queue;
  {
    BloomFilter bf{numFilters, numHash, bitsPerFilterHash};
    for (int i = 0; i < numKeys; i++) {
      auto key = folly::Random::rand64();
      auto idx = folly::Random::rand64() % numFilters;
      bf.set(idx, key);
      keysAdded.push_back(std::make_pair(idx, key));
    }

    while (keysNotPresent.size() < numKeys) {
      auto key = folly::Random::rand64();
      auto idx = folly::Random::rand64() % numFilters;
      if (!bf.couldExist(idx, key)) {
        keysNotPresent.push_back(std::make_pair(idx, key));
      }
    }

    auto rw = createMemoryRecordWriter(queue);
    bf.persist<apache::thrift::BinarySerializer>(*rw);
  }

  auto rr = createMemoryRecordReader(queue);
  BloomFilter bf(numFilters, numHash, bitsPerFilterHash);
  bf.recover<apache::thrift::BinarySerializer>(*rr);

  EXPECT_EQ(bf.numHashes(), numHash);
  EXPECT_EQ(bf.numBitsPerFilter(), bitsPerFilterHash * numHash);
  EXPECT_EQ(bf.numFilters(), numFilters);
  for (const auto& p : keysAdded) {
    EXPECT_TRUE(bf.couldExist(p.first, p.second));
  }

  for (const auto& p : keysNotPresent) {
    EXPECT_FALSE(bf.couldExist(p.first, p.second));
  }
}

TEST(BloomFilter, PersistRecoveryValidLarge) {
  const size_t numFilters = 1e7;
  const size_t bitsPerFilterHash = 1024;
  const size_t numHash = 1;
  testPersistRecoveryWithParams(numFilters, bitsPerFilterHash, numHash);
}

TEST(BloomFilter, PersistRecoveryValid) {
  const size_t numFilters = 10;
  const size_t bitsPerFilterHash = 1024;
  const size_t numHash = 1;
  testPersistRecoveryWithParams(numFilters, bitsPerFilterHash, numHash);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
