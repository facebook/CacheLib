#include "cachelib/navy/common/Hash.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Hash, HashedKeyCollision) {
  HashedKey hk1{makeView("key 1")};
  HashedKey hk2{makeView("key 2")};

  // Simulate a case where hash matches but key doesn't.
  HashedKey hk3 = HashedKey::precomputed(hk2.key(), hk1.keyHash());

  EXPECT_NE(hk1, hk2);
  EXPECT_NE(hk1, hk3);
  EXPECT_NE(hk2, hk3);
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
