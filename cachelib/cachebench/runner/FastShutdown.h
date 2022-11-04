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

#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// tests the functionality of cache shutdown while slab releases are in
// progress. the test is run repeatedly to ensure that cache shutdown is not
// blocked when slab releases happen and aborting the slab release has no
// side-effects. Currently the test fails if the shutdown takes more than 10
// seconds once initiated.
class FastShutdownStressor : public Stressor {
 public:
  // @param cacheConfig  configuration for the cache
  // @param ops          number of times the test is performed in sequence.
  FastShutdownStressor(const CacheConfig& cacheConfig, uint64_t numOps);

  // report the cache statistics
  Stats getCacheStats() const override { return cache_->getStats(); }

  ThroughputStats aggregateThroughputStats() const override {
    ThroughputStats stats;
    stats.ops = ops_;
    return stats;
  }

  uint64_t getTestDurationNs() const override {
    return std::chrono::nanoseconds{endTime_ - startTime_}.count();
  }

  void start() override;
  void finish() override { cache_->cleanupSharedMem(); }

 private:
  // number of times the test operates
  const uint64_t numOps_{};

  std::string cacheDir_{};

  // instance of the cache
  std::unique_ptr<Cache<LruAllocator>> cache_;

  // progress so far.
  std::atomic<uint64_t> ops_{0};

  // start and end time for the test. end time is set when the test completes.
  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;

  // thread that creates allocations that trigggers slab release.
  std::thread testThread_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
