#include "MultiAllocatorTest.h"
#include "TestBase.h"

#include <algorithm>
#include <future>
#include <mutex>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"

using namespace facebook::cachelib::tests;
namespace facebook {
namespace cachelib {
namespace tests {

using LruTo2QTest = MultiAllocatorTest<LruAllocator, Lru2QAllocator>;
TEST_F(LruTo2QTest, InvalidAttach) { testInCompatibility(); }

using TwoQToLruTest = MultiAllocatorTest<Lru2QAllocator, LruAllocator>;
TEST_F(TwoQToLruTest, InvalidAttach) { testInCompatibility(); }
}
}
} // namespace facebook::cachelib
