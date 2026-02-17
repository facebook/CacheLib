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

#include <folly/coro/Task.h>

#include <memory>

#include "cachelib/cachebench/cache/CacheStats.h"
#include "cachelib/cachebench/runner/CacheStressorBase.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"
#include "cachelib/interface/CacheComponent.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// Stressor implementation that uses CacheComponent as the underlying cache.
// This stressor exercises the generic CacheComponent interface with coroutines
// and async operations.
class CacheComponentStressor : public CacheStressorBase {
 public:
  // @param cacheConfig   the config to instantiate the cache instance
  // @param config        stress test configuration
  // @param generator     workload generator
  CacheComponentStressor(const CacheConfig& cacheConfig,
                         StressorConfig config,
                         std::unique_ptr<GeneratorBase>&& generator);

  // Start the stress test by spawning worker threads
  void start() override;

  // Obtain stats from the cache instance
  Stats getCacheStats() const override;

 private:
  // Validate the test config (implicitly validates the StressorConfig in
  // CacheStressorBase::config_)
  void validate(const CacheConfig& config) const;

  // Main stress coroutine that runs operations
  folly::coro::Task<void> stressCoroutine(ThroughputStats& stats);

  // Execute a single cache operation
  folly::coro::Task<OpResultType> executeOperation(ThroughputStats& stats,
                                                   const Request& req,
                                                   std::mt19937_64& gen);

  // Set operation implementation
  folly::coro::Task<OpResultType> setKey(
      ThroughputStats& stats,
      const std::string_view key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap,
      const std::string& itemValue);

  // Update operation implementation
  folly::coro::Task<OpResultType> updateKey(
      ThroughputStats& stats,
      const std::string_view key,
      size_t size,
      uint32_t ttlSecs,
      const std::unordered_map<std::string, std::string>& featureMap,
      const std::string& itemValue);

  // Get operation implementation
  folly::coro::Task<OpResultType> getKey(ThroughputStats& stats,
                                         const std::string_view key,
                                         const Request& req);

  // Delete operation implementation
  folly::coro::Task<OpResultType> deleteKey(ThroughputStats& stats,
                                            const std::string_view key);

  // Populate an allocated item handle with data
  void populateItem(interface::AllocatedHandle& handle,
                    const std::string& itemValue = "");

  std::unique_ptr<interface::CacheComponent> cache_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
