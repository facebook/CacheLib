#include "cachelib/allocator/tests/MultiAllocatorTest.h"

#include <algorithm>
#include <future>
#include <mutex>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

using LruTo2QTest = MultiAllocatorTest<LruAllocator, Lru2QAllocator>;
TEST_F(LruTo2QTest, InvalidAttach) { testInCompatibility(); }

using TwoQToLruTest = MultiAllocatorTest<Lru2QAllocator, LruAllocator>;
TEST_F(TwoQToLruTest, InvalidAttach) { testInCompatibility(); }
} // namespace tests
} // namespace cachelib
} // namespace facebook
