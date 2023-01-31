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
#include <folly/logging/xlog.h>

#include <memory>
#include <scoped_allocator>
#include <string>

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
  // @param name          name must be the same for allocator to compare equal
  // @param privateName   private name can be anything. it's just for us to
  //                      identify the allocator
  explicit TestAllocatorResource(std::string name,
                                 std::string privateName = "private")
      : name_{std::move(name)}, privateName_{std::move(privateName)} {}

  TestAllocatorResource(const TestAllocatorResource& other) = default;
  TestAllocatorResource& operator=(const TestAllocatorResource& other) =
      default;

  TestAllocatorResource(TestAllocatorResource&& other) {
    // Instead of moving. Simply copy over the name and the shared_ptr
    name_ = other.name_;
    privateName_ = other.privateName_;
    numAllocs_ = other.numAllocs_;
    throw_ = other.throw_;
  }

  TestAllocatorResource& operator=(TestAllocatorResource&& other) {
    if (this != &other) {
      new (this) TestAllocatorResource(std::move(other));
    }
    return *this;
  }

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

  void setThrow(bool shouldThrow) { *throw_ = shouldThrow; }

  uint64_t getNumAllocs() const { return *numAllocs_; }

  const std::string& getName() const { return name_; }
  const std::string& getPrivateName() const { return privateName_; }

 private:
  std::string name_{folly::sformat("name_{}", folly::Random::rand32())};
  std::string privateName_{"private"};
  std::shared_ptr<uint64_t> numAllocs_{std::make_shared<uint64_t>()};
  std::shared_ptr<bool> throw_{std::make_shared<bool>(false)};
};

using ScopedTestAllocator =
    std::scoped_allocator_adaptor<Allocator<char, TestAllocatorResource>>;
using TestString =
    std::basic_string<char, std::char_traits<char>, ScopedTestAllocator>;

template <typename K, typename V>
using TestMap =
    std::map<K,
             V,
             std::less<K>,
             std::scoped_allocator_adaptor<
                 Allocator<std::pair<const K, V>, TestAllocatorResource>>>;

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
  cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);
  return cache;
}
} // namespace test
} // namespace objcache
} // namespace cachelib
} // namespace facebook
