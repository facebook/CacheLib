#include "cachelib/navy/bighash/BloomFilter.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {

TEST(BloomFilter, Reset) {
  BloomFilter bf{4, 2, 4};
  EXPECT_EQ(4, bf.getByteSize());
  for (uint32_t i = 0; i < 4; i++) {
    bf.setInitBit(i);
    for (uint64_t key = 0; key < 10; key++) {
      bf.set(i, key);
    }
  }

  for (uint32_t i = 0; i < 4; i++) {
    EXPECT_TRUE(bf.getInitBit(i));
    for (uint64_t key = 0; key < 10; key++) {
      EXPECT_TRUE(bf.couldExist(i, key));
    }
  }

  bf.reset();

  // reset should make the bloom filter return negative on all keys
  for (uint32_t i = 0; i < 4; i++) {
    EXPECT_TRUE(bf.getInitBit(i));
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
    bf.setInitBit(i);
    {
      uint64_t key = 1;
      EXPECT_EQ(0, combineHashes(key, hashInt(0)) % 4);
      EXPECT_EQ(3, combineHashes(key, hashInt(1)) % 4);
      EXPECT_TRUE(bf.couldExist(i, key));
    }
    {
      uint64_t key = 3;
      EXPECT_EQ(2, combineHashes(key, hashInt(0)) % 4);
      EXPECT_EQ(2, combineHashes(key, hashInt(1)) % 4);
      EXPECT_FALSE(bf.couldExist(i, key));
    }
    {
      uint64_t key = 33;
      EXPECT_EQ(0, combineHashes(key, hashInt(0)) % 4);
      EXPECT_EQ(3, combineHashes(key, hashInt(1)) % 4);
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
    EXPECT_FALSE(bf.getInitBit(i));
    EXPECT_TRUE(bf.couldExist(i, 1));
  }
}

TEST(BloomFilter, SharedCollision) {
  BloomFilter bf{1, 2, 4};
  bf.setInitBit(0);
  EXPECT_EQ(1, bf.getByteSize());
  EXPECT_EQ(0, combineHashes(1, hashInt(0)) % 4); // Bit 0 in 1st hash table
  EXPECT_EQ(3, combineHashes(1, hashInt(1)) % 4); // Bit 3 in 2nd hash table
  bf.set(0, 1);
  EXPECT_EQ(2, combineHashes(3, hashInt(0)) % 4); // Bit 2 in 1st hash table
  EXPECT_EQ(2, combineHashes(3, hashInt(1)) % 4); // Bit 2 in 2nd hash table
  bf.set(0, 3);

  // BloomFilter looks like:
  //   bit# 0 1 2 3
  // [HT 0] 1 0 1 0
  // [HT 1] 0 0 1 1

  // Try key = 18: 1st bit #2, 2nd bit #3.
  {
    uint64_t key = 18;
    EXPECT_EQ(2, combineHashes(key, hashInt(0)) % 4);
    EXPECT_EQ(3, combineHashes(key, hashInt(1)) % 4);
    EXPECT_TRUE(bf.couldExist(0, key));
  }
  // Try key = 15: 1st bit #0, 2nd bit #2.
  {
    uint64_t key = 15;
    EXPECT_EQ(0, combineHashes(key, hashInt(0)) % 4);
    EXPECT_EQ(2, combineHashes(key, hashInt(1)) % 4);
    EXPECT_TRUE(bf.couldExist(0, key));
  }
}

TEST(BloomFilter, InvalidArgs) {
  auto makeFilter = [] { BloomFilter bf{2, 2, 3 /* not power of two */}; };
  EXPECT_THROW(makeFilter(), std::invalid_argument);
}

TEST(BloomFilter, InitBits) {
  BloomFilter bf{2, 2, 4};

  // By default every filter is assumed valid
  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_FALSE(bf.couldExist(1, 100 + i));
  }

  // Invalidate bucket at index 1, and it will always return "could exist"
  // until we mark it initialized
  bf.clear(1);

  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_TRUE(bf.couldExist(1, 100 + i));
  }
  bf.set(1, 100);
  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_TRUE(bf.couldExist(1, 100 + i));
  }

  bf.setInitBit(1);
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
  EXPECT_FALSE(bf.getInitBit(1));
  for (uint32_t i = 0; i < 16; i++) {
    EXPECT_TRUE(bf.couldExist(1, 100 + i));
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
