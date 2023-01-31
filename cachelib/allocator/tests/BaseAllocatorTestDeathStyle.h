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
#include <chrono>
#include <ctime>
#include <future>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {
template <typename AllocatorT>
class BaseAllocatorTestDeathStyle : public AllocatorTest<AllocatorT> {
 public:
  // create a cache and access it through the offsets from the read only cache
  // view. Writing to the read only view should fail even if we try to
  // const_cast
  void testReadOnlyCacheView() {
    typename AllocatorT::Config config;

    uint8_t poolId;
    const size_t nSlabs = 20;
    config.setCacheSize(nSlabs * Slab::kSize);
    config.enableCachePersistence(this->cacheDir_);

    size_t allocSize = 1024;
    AllocatorT alloc(AllocatorT::SharedMemNew, config);
    const size_t numBytes = alloc.getCacheMemoryStats().ramCacheSize;
    poolId = alloc.addPool("foobar", numBytes);
    auto hdl = util::allocateAccessible(alloc, poolId, "mykey", allocSize);
    ASSERT_NE(hdl, nullptr);
    unsigned char magicVal = 'f';
    std::vector<char> v(hdl->getSize(), magicVal);
    auto data = folly::StringPiece{v.data(), v.size()};
    std::memcpy(hdl->getMemory(), data.data(), data.size());
    auto hdlSp = folly::StringPiece{
        reinterpret_cast<const char*>(hdl->getMemory()), hdl->getSize()};
    ASSERT_EQ(hdlSp, data);

    auto offset = alloc.getItemPtrAsOffset(hdl->getMemory());

    auto roCache = ReadOnlySharedCacheView(config.cacheDir, config.usePosixShm);

    auto ptr = roCache.getItemPtrFromOffset(offset);
    ASSERT_NE(reinterpret_cast<uintptr_t>(ptr),
              reinterpret_cast<uintptr_t>(hdl->getMemory()));
    auto roSp =
        folly::StringPiece{reinterpret_cast<const char*>(ptr), allocSize};
    ASSERT_EQ(data, roSp);

    ASSERT_DEATH(std::memset(reinterpret_cast<char*>(const_cast<void*>(ptr)), 0,
                             allocSize),
                 ".*");
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
