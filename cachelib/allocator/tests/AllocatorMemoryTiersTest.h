/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/MemoryTierCacheConfig.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

template <typename AllocatorT>
class AllocatorMemoryTiersTest : public AllocatorTest<AllocatorT> {
 public:
  void testMultiTiersInvalid() {
    typename AllocatorT::Config config;
    config.setCacheSize(100 * Slab::kSize);
    ASSERT_NO_THROW(config.configureMemoryTiers(
        {MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
             std::string("0")),
         MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
             std::string("0"))}));
  }

  void testMultiTiersValid() {
    typename AllocatorT::Config config;
    config.setCacheSize(100 * Slab::kSize);
    config.enableCachePersistence("/tmp");
    ASSERT_NO_THROW(config.configureMemoryTiers(
        {MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
             std::string("0")),
         MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
             std::string("0"))}));

    auto alloc = std::make_unique<AllocatorT>(AllocatorT::SharedMemNew, config);
    ASSERT_NE(alloc, nullptr);

    auto pool = alloc->addPool("default", alloc->getCacheMemoryStats().ramCacheSize);
    auto handle = alloc->allocate(pool, "key", std::string("value").size());
    ASSERT_NE(handle, nullptr);
    ASSERT_NO_THROW(alloc->insertOrReplace(handle));
  }

  void testMultiTiersValidMixed() {
    typename AllocatorT::Config config;
    config.setCacheSize(100 * Slab::kSize);
    config.enableCachePersistence("/tmp");
    ASSERT_NO_THROW(config.configureMemoryTiers(
        {MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
             std::string("0")),
         MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
             std::string("0"))}));

    auto alloc = std::make_unique<AllocatorT>(AllocatorT::SharedMemNew, config);
    ASSERT_NE(alloc, nullptr);

    auto pool = alloc->addPool("default", alloc->getCacheMemoryStats().ramCacheSize);
    auto handle = alloc->allocate(pool, "key", std::string("value").size());
    ASSERT_NE(handle, nullptr);
    ASSERT_NO_THROW(alloc->insertOrReplace(handle));
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
