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

#include <folly/Benchmark.h>

#include <atomic>

#include "cachelib/cachebench/util/Request.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class GeneratorBase {
 public:
  virtual ~GeneratorBase() {}

  // Grab the next request given the last request in the sequence or using the
  // random number generator.
  // @param poolId cache pool id for the generated request
  // @param gen random number generator
  // @param lastRequestId generator may generate next request based on last
  // request, e.g., piecewise caching in bigcache.
  virtual const Request& getReq(uint8_t /*poolId*/,
                                std::mt19937_64& /*gen*/,
                                std::optional<uint64_t> /*lastRequestId*/) = 0;

  // Notify the workload generator about the result of the request.
  // Note the workload generator may release the memory for the request.
  virtual void notifyResult(uint64_t /*requestId*/, OpResultType /*result*/) {}

  virtual const std::vector<std::string>& getAllKeys() const = 0;

  // Notify the workload generator that the nvm cache has already warmed up.
  virtual void setNvmCacheWarmedUp(uint64_t /*timestamp*/) {
    // not implemented by default
  }

  virtual void renderStats(uint64_t /*elapsedTimeNs*/,
                           std::ostream& /*out*/) const {
    // not implemented by default
  }

  virtual void renderStats(uint64_t /*elapsedTimeNs*/,
                           folly::UserCounters& /*counters*/) const {
    // not implemented by default
  }

  // Render the stats based on the data since last time we rendered.
  virtual void renderWindowStats(double /*elapsedSecs*/,
                                 std::ostream& /*out*/) const {
    // not implemented by default
  }

  // Should be called when all working threads are finished, or aborted
  void markShutdown() { isShutdown_.store(true, std::memory_order_relaxed); }

  // Should be called when working thread finish its operations
  virtual void markFinish() {}

 protected:
  bool shouldShutdown() const {
    return isShutdown_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<bool> isShutdown_{false};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
