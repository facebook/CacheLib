#include "cachelib/allocator/tests/SimplePoolOptimizationTest.h"

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/TestBase.h"

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
