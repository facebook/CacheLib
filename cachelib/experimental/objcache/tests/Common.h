#include <folly/logging/xlog.h>

#include <memory>
#include <scoped_allocator>
#include <string>

#pragma once

#include "cachelib/experimental/objcache/Allocator.h"

namespace facebook {
namespace cachelib {
namespace objcache {
namespace test {
// A simple custom allocator that uses global allocator internally
// This is just to simulate allocator behavior in unit tests
class TestAllocatorResource {
 public:
  TestAllocatorResource() = default;
  explicit TestAllocatorResource(std::string name) : name_{std::move(name)} {}

  void* allocate(size_t bytes, size_t alignment) {
    (void)alignment;
    XLOG(INFO) << "requesting bytes of " << bytes;
    (*numAllocs_)++;
    if (*throw_) {
      throw exception::ObjectCacheAllocationError(
          "Alloc failed because test set the allocator to throw");
    }
    return std::allocator<uint8_t>{}.allocate(bytes);
  }

  void deallocate(void* alloc, size_t bytes, size_t alignment) {
    (void)alignment;
    if (*throw_) {
      throw exception::ObjectCacheDeallocationBadArgs(
          "Dealloc failed because test set the allocator to throw");
    }
    std::allocator<uint8_t>{}.deallocate(reinterpret_cast<uint8_t*>(alloc),
                                         bytes);
  }

  bool isEqual(const TestAllocatorResource& other) const noexcept {
    return name_ == other.name_;
  }

  uint64_t getNumAllocs() const { return *numAllocs_; }

  void setThrow(bool shouldThrow) { *throw_ = shouldThrow; }

 private:
  std::string name_{"default"};
  std::shared_ptr<uint64_t> numAllocs_{std::make_shared<uint64_t>()};
  std::shared_ptr<bool> throw_{std::make_shared<bool>(false)};
};

using ScopedTestAllocator =
    std::scoped_allocator_adaptor<Allocator<char, TestAllocatorResource>>;
using TestString =
    std::basic_string<char, std::char_traits<char>, ScopedTestAllocator>;
template <typename K, typename V>
using TestMap = std::map<K, V, std::less<K>, ScopedTestAllocator>;

template <typename T>
using TestF14TemplateAllocator = Allocator<T, TestAllocatorResource>;
template <typename K, typename V>
using TestFollyF14FastMap =
    folly::F14FastMap<K,
                      V,
                      std::hash<K>,
                      std::equal_to<K>,
                      TestF14TemplateAllocator<std::pair<const K, V>>>;

inline std::unique_ptr<LruAllocator> createCache() {
  LruAllocator::Config config;
  config.setCacheSize(100 * 1024 * 1024);
  auto cache = std::make_unique<LruAllocator>(config);
  cache->addPool("default", cache->getCacheMemoryStats().cacheSize);
  return cache;
}
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook
