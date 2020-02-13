#pragma once

#include "cachelib/allocator/tests/TestBase.h"

#include <algorithm>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <folly/Random.h>

#include "cachelib/allocator/FreeMemStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/PoolRebalancer.h"

namespace facebook {
namespace cachelib {
namespace tests {

template <typename AllocatorA, typename AllocatorB>
class MultiAllocatorTest : public AllocatorTest<AllocatorA> {
 public:
  // create Allocator A and try to attach with AllocatorB type and it should
  // fail or be handled gracefully.
  void testInCompatibility() {
    const size_t nSlabs = 20;
    // allocate using AllocatorA.
    {
      const unsigned int keyLen = 100;
      typename AllocatorA::Config config;
      config.setCacheSize(nSlabs * Slab::kSize);
      config.enableCachePersistence(this->cacheDir_);
      AllocatorA alloc(AllocatorA::SharedMemNew, config);
      const size_t numBytes = alloc.getCacheMemoryStats().cacheSize;
      const auto poolId = alloc.addPool("foobar", numBytes);
      auto sizes = this->getValidAllocSizes(alloc, poolId, nSlabs, keyLen);
      this->fillUpPoolUntilEvictions(alloc, poolId, sizes, keyLen);
      alloc.shutDown();
    }

    {
      typename AllocatorB::Config config;
      config.setCacheSize(nSlabs * Slab::kSize);
      config.enableCachePersistence(this->cacheDir_);
      ASSERT_THROW(AllocatorB(AllocatorB::SharedMemAttach, config),
                   std::invalid_argument);
    }
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
