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

#include "cachelib/cachebench/workload/SimpleFlashBenchmarkGenerator.h"

namespace {
// TODO: make these configurable
constexpr uint64_t kNumToFillRamCache = 4096; // 256MB
constexpr uint64_t kDataSize = 64 * 1024;
constexpr uint64_t kMinDistance = 2148; // a little more than 128MB
} // namespace

namespace facebook {
namespace cachelib {
namespace cachebench {

SimpleFlashBenchmarkGenerator::SimpleFlashBenchmarkGenerator(
    const StressorConfig& config)
    : config_{config},
      sizes_{kDataSize},
      req_([&]() { return Request(*key_, sizes_.begin(), sizes_.end()); }),
      lastSetId_([&]() { return 0; }),
      lastGetId_([&]() { return 0; }) {
  // For op distribution.
  for (const auto& c : config_.poolDistributions) {
    workloadDist_.emplace_back(c);
  }
  // TODO: get size distribution from config
}

const Request& SimpleFlashBenchmarkGenerator::getReq(uint8_t poolId,
                                                     std::mt19937_64& gen,
                                                     std::optional<uint64_t>) {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  std::string threadIdStr = ss.str();
  auto op = OpType::kSet;
  if (*lastSetId_ > kNumToFillRamCache) {
    op = static_cast<OpType>(
        workloadDist_[workloadIdx(poolId)].sampleOpDist(gen));
  }
  if (op == OpType::kSet) {
    ++(*lastSetId_);
    *key_ = folly::to<std::string>(
        "key", threadIdStr, "_", poolId, "_", *lastSetId_);
  } else if (op == OpType::kGet) {
    CHECK_GT(*lastSetId_, *lastGetId_) << "Get id should be less than set id";
    ++(*lastGetId_);
    if (*lastSetId_ - *lastGetId_ < kMinDistance) {
      *lastGetId_ = 1;
    }
    *key_ = folly::to<std::string>(
        "key", threadIdStr, "_", poolId, "_", *lastGetId_);
  } else {
    // Do not care about other ops
    *key_ = folly::to<std::string>("any_key", threadIdStr, "_", poolId);
  }
  req_->key = *key_;
  req_->setOp(op);

  return *req_;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
