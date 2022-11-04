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
#include <folly/logging/xlog.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"
#include "cachelib/cachebench/workload/WorkloadDistribution.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class WorkloadGenerator : public GeneratorBase {
 public:
  explicit WorkloadGenerator(const StressorConfig& config);
  virtual ~WorkloadGenerator() {}

  const Request& getReq(
      uint8_t poolId,
      std::mt19937_64& gen,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  const std::vector<std::string>& getAllKeys() const override { return keys_; }

 private:
  void generateFirstKeyIndexForPool();
  void generateKeys();
  void generateReqs();
  void generateKeyDistributions();
  // if there is only one workloadDistribution, use it for everything.
  size_t workloadIdx(size_t i) { return workloadDist_.size() > 1 ? i : 0; }

  const StressorConfig config_;
  std::vector<std::string> keys_;
  std::vector<std::vector<size_t>> sizes_;
  std::vector<Request> reqs_;
  // @firstKeyIndexForPool_ contains the first key in each pool (As represented
  // by key pool distribution). It's a convenient method for us to populate
  // @keyIndicesForPoo_ which contains all the key indices that each operation
  // in a pool. @keyGenForPool_ contains uniform distributions to select indices
  // contained in @keyIndicesForPool_.
  std::vector<uint32_t> firstKeyIndexForPool_;
  std::vector<std::vector<uint32_t>> keyIndicesForPool_;
  std::vector<std::uniform_int_distribution<uint32_t>> keyGenForPool_;

  std::vector<WorkloadDistribution> workloadDist_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
