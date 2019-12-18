#include "SimpleRebalancingTest.h"

#include "cachelib/allocator/CacheAllocator.h"

using namespace facebook::cachelib;

namespace facebook {
namespace cachelib {
namespace tests {

using LruSimpleRebalanceTest = SimpleRebalanceTest<LruAllocator>;

TEST_F(LruSimpleRebalanceTest, PoolRebalancerCreation) {
  testPoolRebalancerCreation();
}

TEST_F(LruSimpleRebalanceTest, MultiplePools) { testMultiplePools(); }
}
}
}
