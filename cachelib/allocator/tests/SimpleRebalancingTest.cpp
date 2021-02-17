#include "cachelib/allocator/tests/SimpleRebalancingTest.h"

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook {
namespace cachelib {
namespace tests {

using LruSimpleRebalanceTest = SimpleRebalanceTest<LruAllocator>;

TEST_F(LruSimpleRebalanceTest, PoolRebalancerCreation) {
  testPoolRebalancerCreation();
}

TEST_F(LruSimpleRebalanceTest, MultiplePools) { testMultiplePools(); }
} // namespace tests
} // namespace cachelib
} // namespace facebook
