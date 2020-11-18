#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/kangaroo/NruBitVector.h"

namespace facebook {
namespace cachelib {
namespace navy {

TEST(NruBitVector, BasicOps) {
  NruBitVector bv{1};
  EXPECT_EQ(4, bv.getByteSize());
  EXPECT_EQ(1, bv.numVectors());
  for (uint32_t key = 0; key < 36; key += 1) {
    bv.set(0, key);
  }
  for (uint32_t key = 0; key < 36; key++) {
    EXPECT_EQ(bv.get(0,key), key < 32);
  }
  bv.clear(0);
  for (uint32_t key = 0; key < 36; key++) {
    EXPECT_FALSE(bv.get(0,key));
  }

}
} // namespace navy
} // namespace cachelib
} // namespace facebook
