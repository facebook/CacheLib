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

#include <folly/init/Init.h>

#include "cachelib/allocator/CCacheAllocator.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/compact_cache/CCacheCreator.h"

namespace facebook {
namespace cachelib_examples {

using Cache = cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
using CacheConfig = typename Cache::Config;
using cachelib::CCacheAllocator;
using cachelib::CCacheCreator;
using cachelib::CCacheReturn;

// Global cache object
std::unique_ptr<Cache> gCache_;

// We instantiate a compact cache with uint64_t keys and uint64_t values
struct IntegerKey {
  uint64_t id;

  // This method is required by compact cache to determine whether
  // two keys are the same
  bool operator==(const IntegerKey& other) const { return id == other.id; }

  // This is the method compact cache used to determine whether a key is empty
  // Here we assume that 0 key is an invalid key.
  bool isEmpty() const { return id == 0; }
};

// compact cache from our IntegerKey type to uint64_t value
using MyCompactCache =
    CCacheCreator<CCacheAllocator, IntegerKey, uint64_t>::type;

MyCompactCache* myCompactCache{nullptr};

void initializeCache() {
  CacheConfig config;
  config.enableCompactCache(); // required to use compact cache

  config
      .setCacheSize(1 * 1024 * 1024 * 1024) // 1GB
      .setCacheName("my_use_case")
      .setAccessConfig(
          {25 /* bucket power */, 10 /* lock power */}) // assuming caching 20
                                                        // million items
      .validate(); // will throw if bad config

  gCache_ = std::make_unique<Cache>(config);

  // Create a compact cache.  interface detail can be found in
  // cachelib/allocator/CacheAllocator.h
  // We create a compact cache with our type and "ccache name" as the name
  // We set the size to the full cache size. If needed, the compact cache can
  // be sized smaller or you can add more compact caches.
  myCompactCache = gCache_->addCompactCache<MyCompactCache>(
      "ccache name",
      gCache_->getCacheMemoryStats()
          .cacheSize /* compact cache size in bytes */);
}

void destroyCache() { gCache_.reset(); }

// returns true if found and value is copied. false if not found or error.
bool get(uint64_t key, uint64_t& value) {
  auto res = myCompactCache->get(IntegerKey{key}, &value);
  return res == CCacheReturn::FOUND;
}

// set a value to compact cache.
void put(uint64_t key, uint64_t value) {
  myCompactCache->set(IntegerKey{key}, &value);
}

// return true if the key was found. false if error or key was not found.
bool del(uint64_t key) {
  return myCompactCache->del(IntegerKey{key}) == CCacheReturn::FOUND;
}

} // namespace cachelib_examples
} // namespace facebook

using namespace facebook::cachelib_examples;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  initializeCache();

  // Use cache
  {
    put(50000, 1111111);

    uint64_t val;
    bool found = get(50000, val);
    if (found) {
      std::cout << "Value " << val << std::endl;
    } else {
      std::cout << "Not found" << std::endl;
    }

    // we should not find this key
    found = get(99999999, val);
    assert(!found);

    // delete a key
    found = del(50000);
    assert(found);

    found = del(9999999);
    assert(!found);
  }

  destroyCache();
}
