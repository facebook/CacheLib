#include "AllocatorHitStatsTest.h"
#include "TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

TYPED_TEST_CASE(AllocatorHitStatsTest, AllocatorTypes);

TYPED_TEST(AllocatorHitStatsTest, CacheStats) { this->testCacheStats(); }

TYPED_TEST(AllocatorHitStatsTest, PoolName) { this->testPoolName(); }

TYPED_TEST(AllocatorHitStatsTest, FragmentationSizeStats) {
  this->testFragmentationStats();
}
} // end of namespace tests
} // end of namespace cachelib
} // end of namespace facebook
