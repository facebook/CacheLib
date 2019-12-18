#include "SimplePoolOptimizationTest.h"

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/TestBase.h"

using namespace facebook::cachelib;

namespace facebook {
namespace cachelib {
namespace tests {

TYPED_TEST_CASE(SimplePoolOptimizationTest, AllocatorTypes);

TYPED_TEST(SimplePoolOptimizationTest, PoolOptimizerBasic) {
  this->testPoolOptimizerBasic();
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
