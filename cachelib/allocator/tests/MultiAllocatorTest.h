/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/Random.h>

#include <algorithm>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "cachelib/allocator/FreeMemStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/PoolRebalancer.h"
#include "cachelib/allocator/tests/TestBase.h"

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
      const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
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
