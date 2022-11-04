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

#include "cachelib/cachebench/runner/FastShutdown.h"

#include <folly/logging/xlog.h>

#include <iostream>

namespace facebook {
namespace cachelib {
namespace cachebench {

FastShutdownStressor::FastShutdownStressor(const CacheConfig& cacheConfig,
                                           uint64_t numOps)
    : numOps_(numOps),
      cacheDir_{folly::sformat("/tmp/cache_bench_fss_{}", getpid())},
      cache_(std::make_unique<Cache<LruAllocator>>(
          cacheConfig, nullptr, cacheDir_)) {}

void FastShutdownStressor::start() {
  startTime_ = std::chrono::system_clock::now();
  constexpr uint32_t kSlabSize = 4 * 1024 * 1024;

  uint32_t nslabs = cache_->getCacheSize() / kSlabSize;
  uint32_t numSmallAllocs = kSlabSize / 64;
  using CacheType = Cache<LruAllocator>;
  uint64_t expectedAbortCount = 0;

  // Test with wait time 6 seconds first time and wait time 30 seconds second
  // time to interrupt slab release at different places.
  uint32_t waitTime = 6;
  for (; waitTime <= numOps_ * 3; waitTime += 3) {
    std::vector<CacheType::WriteHandle> v;
    std::cout << "allocating....\n";
    for (uint32_t i = 0; i < nslabs; i++) {
      for (uint32_t j = 0; j < numSmallAllocs; j++) {
        auto it = cache_->allocate(
            static_cast<uint8_t>(0),
            folly::sformat("key_{}", i * numSmallAllocs + j), 5);
        if (it) {
          cache_->insertOrReplace(it);
          v.push_back(std::move(it));
        }
      }
      ops_.fetch_add(numSmallAllocs, std::memory_order_relaxed);
    }

    std::cout << "removing most of the allocated items....\n";
    // Free up items one per slab so that items from same slab are not together
    // in the free allocs list. Skip some while doing this.
    for (uint32_t j = 0; j < numSmallAllocs; j++) {
      // skip some items
      if (j % 10000 == 0) {
        continue;
      }
      for (uint32_t i = 0; i < nslabs; i++) {
        // Adding the check to make the linter happy
        if (!v.empty()) {
          v[i * numSmallAllocs + j].reset();
          cache_->remove(folly::sformat("key_{}", i * numSmallAllocs + j));
        }
      }
    }
    // create a thread that tries to allocate from a different slab class
    // resulting in pool rebalancer to release one slab from class id 0 to
    // class id 1.

    std::cout << "creating thread...\n";
    testThread_ = std::thread([this] {
      int count = 0;
      // This should trigger rebalancer to release a slab from class id 0.
      while (count < 20) {
        auto it1 = cache_->allocate(static_cast<uint8_t>(0), "nkey1", 50);
        if (it1) {
          cache_->insertOrReplace(it1);
          break;
        }
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
        count++;
      }
    });

    std::cout << "sleeping for " << waitTime << " seconds\n";
    // It could take up to 3 seconds for the rebalancer to kick in.
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds{waitTime});

    std::cout << "releasing the last few items...\n";
    // release the items that were not removed, so that the items can be
    // moved.
    for (uint32_t j = 0; j < numSmallAllocs; j += 10000) {
      for (uint32_t i = 0; i < nslabs; i++) {
        // Adding the check to make the linter happy
        if (!v.empty()) {
          v[i * numSmallAllocs + j].reset();
        }
      }
    }
    auto shutDownStartTime = std::chrono::system_clock::now();
    std::cout << "Shutting Down...\n";
    cache_->shutDown();
    testThread_.join();
    endTime_ = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
        endTime_ - shutDownStartTime);

    std::cout << "Shut down durtaion " << duration.count() << "\n";
    if (duration.count() > 10) {
      throw std::runtime_error(
          folly::sformat("Failed. Took {} seconds for shutdown to complete",
                         duration.count()));
    }
    // reattach the cache, so that stats can be collected or the test can be
    // repeated.
    std::cout << "Reattaching to cache...\n";
    cache_->reAttach();
    expectedAbortCount++;
    const auto stats = cache_->getStats();
    if (stats.numAbortedSlabReleases != expectedAbortCount) {
      throw std::runtime_error(
          folly::sformat("Failed. Expected abort count did not match {} {}",
                         stats.numAbortedSlabReleases, expectedAbortCount));
    }
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
