#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/kangaroo/RripBitVector.h"

namespace facebook {
namespace cachelib {
namespace navy {

TEST(RripBitVector, BasicOps) {
  RripBitVector bv{2};
  EXPECT_EQ(4, bv.getByteSize());
  EXPECT_EQ(2, bv.numVectors());
  for (uint32_t key = 0; key < 18; key += 2) {
    bv.set(0, key);
  }
  for (uint32_t key = 0; key < 18; key++) {
    EXPECT_EQ(bv.get(0,key), key < 16 && key % 2 == 0);
  }
  bv.clear(0);
  for (uint32_t key = 0; key < 18; key++) {
    EXPECT_FALSE(bv.get(0,key));
  }

}
} // namespace navy
} // namespace cachelib
} // namespace facebook
