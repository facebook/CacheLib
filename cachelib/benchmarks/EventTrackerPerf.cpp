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

/*
============================================================================
cachelib/benchmarks/EventTrackerPerf.cpp        relative  time/iter  iters/s
============================================================================
Find                                                       263.03ms     3.80
FindWithWSA                                      100.55%   273.41ms     3.66
Allocate                                                   783.73ms     1.28
AllocateWithWSA                                   99.16%   790.35ms     1.27
============================================================================
*/

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "cachelib/allocator/CacheAllocator.h"

using namespace facebook::cachelib;

namespace {
class FakeWsaTracker : public EventInterface<LruAllocator::Key> {
 public:
  using Key = LruAllocator::Key;

  void record(AllocatorApiEvent /* event */,
              Key /* key */,
              AllocatorApiResult /* result */,
              EventInterfaceTypes::SizeT /* valueSize */ = folly::none,
              EventInterfaceTypes::TtlT /* ttlSecs */ = 0) override {}

  // Method that extracts stats from the event logger
  // @param statsMap A map of string to a stat value.
  void getStats(std::unordered_map<std::string, uint64_t>&) const override {}
};

std::unique_ptr<LruAllocator> cache;

void runFindOps(std::shared_ptr<FakeWsaTracker> tracker) {
  BENCHMARK_SUSPEND {
    LruAllocator::Config config;

    config.setCacheSize(500ul * 1024ul * 1024ul); // 500 MB

    // 16 million buckets, 1 million locks
    LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                            20 /* locks power */};
    config.setAccessConfig(accessConfig);
    config.configureChainedItems(accessConfig);

    if (tracker) {
      config.setEventTracker(std::move(tracker));
    }

    cache = std::make_unique<LruAllocator>(config);
    cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);

    auto hdl = cache->allocate(0, "my key", 100);
    for (int i = 0; i < 10; i++) {
      auto c = cache->allocateChainedItem(hdl, 100);
      cache->addChainedItem(hdl, std::move(c));
    }
    cache->insert(hdl);
  }

  for (int i = 0; i < 1'000'000; i++) {
    auto hdl = cache->find("my key");
    if (!hdl) {
      throw std::runtime_error("cannot find my key");
    }
    folly::doNotOptimizeAway(hdl);
  }

  BENCHMARK_SUSPEND { cache.reset(); }
}

void runAllocateOps(std::shared_ptr<FakeWsaTracker> tracker) {
  BENCHMARK_SUSPEND {
    LruAllocator::Config config;

    config.setCacheSize(500ul * 1024ul * 1024ul); // 500 MB

    // 16 million buckets, 1 million locks
    LruAllocator::AccessConfig accessConfig{24 /* buckets power */,
                                            20 /* locks power */};
    config.setAccessConfig(accessConfig);
    config.configureChainedItems(accessConfig);

    if (tracker) {
      config.setEventTracker(std::move(tracker));
    }

    cache = std::make_unique<LruAllocator>(config);
    cache->addPool("default", cache->getCacheMemoryStats().ramCacheSize);
  }

  for (int i = 0; i < 100'000; i++) {
    std::string key{folly::sformat("{}", i)};
    auto hdl = cache->allocate(0, key, 100);
    for (int j = 0; j < 10; j++) {
      auto c = cache->allocateChainedItem(hdl, 100);
      cache->addChainedItem(hdl, std::move(c));
    }
    cache->insert(hdl);
  }

  BENCHMARK_SUSPEND { cache.reset(); }
}
} // namespace

BENCHMARK(Find) { runFindOps(nullptr); }
BENCHMARK_RELATIVE(FindWithWSA) {
  runFindOps(std::make_shared<FakeWsaTracker>());
}

BENCHMARK(Allocate) { runAllocateOps(nullptr); }
BENCHMARK_RELATIVE(AllocateWithWSA) {
  runAllocateOps(std::make_shared<FakeWsaTracker>());
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
