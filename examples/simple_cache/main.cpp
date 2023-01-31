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

#include "cachelib/allocator/CacheAllocator.h"
#include "folly/init/Init.h"

namespace facebook {
namespace cachelib_examples {
using Cache = cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
using CacheConfig = typename Cache::Config;
using CacheKey = typename Cache::Key;
using CacheReadHandle = typename Cache::ReadHandle;

// Global cache object and a default cache pool
std::unique_ptr<Cache> gCache_;
cachelib::PoolId defaultPool_;

void initializeCache() {
  CacheConfig config;
  config
      .setCacheSize(1 * 1024 * 1024 * 1024) // 1GB
      .setCacheName("My Use Case")
      .setAccessConfig(
          {25 /* bucket power */, 10 /* lock power */}) // assuming caching 20
                                                        // million items
      .validate(); // will throw if bad config
  gCache_ = std::make_unique<Cache>(config);
  defaultPool_ =
      gCache_->addPool("default", gCache_->getCacheMemoryStats().ramCacheSize);
}

void destroyCache() { gCache_.reset(); }

CacheReadHandle get(CacheKey key) { return gCache_->find(key); }

bool put(CacheKey key, const std::string& value) {
  auto handle = gCache_->allocate(defaultPool_, key, value.size());
  if (!handle) {
    return false; // cache may fail to evict due to too many pending writes
  }
  std::memcpy(handle->getMemory(), value.data(), value.size());
  gCache_->insertOrReplace(handle);
  return true;
}
} // namespace cachelib_examples
} // namespace facebook

using namespace facebook::cachelib_examples;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  initializeCache();

  // Use cache
  {
    auto res = put("key", "value");
    std::ignore = res;
    assert(res);

    auto item = get("key");
    folly::StringPiece sp{reinterpret_cast<const char*>(item->getMemory()),
                          item->getSize()};
    std::ignore = sp;
    assert(sp == "value");
  }

  destroyCache();
}
