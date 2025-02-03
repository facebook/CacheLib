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

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/ThreadLocal.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"
#include "cachelib/cachebench/workload/WorkloadDistribution.h"

// The SimpleFlashBenchmarkGenerator class is designed to generate requests with
// a fixed size. This is essential for benchmarking scenarios where the cache's
// performance with fixed-size objects is being assessed. The generator also
// aims to maintain the flash read/write ratio specified by the user. This is
// achieved by filling the RAM cache with enough data and then generating
// get/set requests to sustain the ratio. For example, to maintain 75% hit rate,
// we simply issue 25% of SET requests with new keys and 75% of GET requests
// with existing keys. To ensure GET requests read from flash, we always read
// the oldest key.

namespace facebook {
namespace cachelib {
namespace cachebench {

class SimpleFlashBenchmarkGenerator : public GeneratorBase {
 public:
  explicit SimpleFlashBenchmarkGenerator(const StressorConfig& config);

  const Request& getReq(
      uint8_t poolId,
      std::mt19937_64& gen,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  const std::vector<std::string>& getAllKeys() const override {
    throw std::logic_error(
        "SimpleFlashBenchmarkGenerator has no keys precomputed!");
  }

 private:
  // if there is only one workloadDistribution, use it for everything.
  size_t workloadIdx(size_t i) { return workloadDist_.size() > 1 ? i : 0; }

  const StressorConfig config_;

  // overall workload distribution per pool
  std::vector<WorkloadDistribution> workloadDist_;

  std::vector<size_t> sizes_;

  // thread local copy of last ID used for set/get
  folly::ThreadLocal<std::string> key_;
  folly::ThreadLocal<Request> req_;
  folly::ThreadLocal<uint64_t> lastSetId_;
  folly::ThreadLocal<uint64_t> lastGetId_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
