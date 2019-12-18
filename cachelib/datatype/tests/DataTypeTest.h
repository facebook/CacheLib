#pragma once

#include <memory>

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook {
namespace cachelib {
namespace tests {
struct DataTypeTest {
  static const std::string kDefaultPool;

  // Creates a bare minimum Cache for test cases
  // Default Pool ID is 0
  template <typename AllocatorT>
  static std::unique_ptr<AllocatorT> createCache() {
    typename AllocatorT::Config config;
    config.configureChainedItems();
    config.setCacheSize(100 * Slab::kSize);
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().cacheSize;
    cache->addPool(kDefaultPool, numBytes);
    return cache;
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
