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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "cachelib/cachebench/runner/ObjectCacheStressor.h"
#include "cachelib/cachebench/runner/Stressor.h"

namespace facebook::cachelib::cachebench {
namespace {

class DeterministicGenerator : public GeneratorBase {
 public:
  DeterministicGenerator() {
    requests_.reserve(6);
    sizes_.reserve(6);

    addRequest(OpType::kSet, 16);
    addRequest(OpType::kGet, 16);
    addRequest(OpType::kUpdate, 16);
    addRequest(OpType::kGet, 16);
    addRequest(OpType::kDel, 16);
    addRequest(OpType::kGet, 16);
  }

  const Request& getReq(uint8_t,
                        std::mt19937_64&,
                        std::optional<uint64_t>) override {
    return requests_.at(nextReq_++);
  }

  const std::vector<std::string>& getAllKeys() const override { return keys_; }

 private:
  void addRequest(OpType op, size_t size) {
    sizes_.push_back({size});
    auto& valueSizes = sizes_.back();
    requests_.emplace_back(keys_.front(), valueSizes.begin(), valueSizes.end(),
                           op);
  }

  std::vector<std::string> keys_{"alpha"};
  std::vector<std::vector<size_t>> sizes_;
  std::vector<Request> requests_;
  size_t nextReq_{0};
};

CacheConfig makeCacheConfig() {
  CacheConfig config;
  config.allocator = "LRU";
  config.cacheSizeMB = 1;
  config.customConfigJson = folly::dynamic::object("l1EntriesLimit", 32);
  return config;
}

StressorConfig makeWorkloadStressorConfig() {
  StressorConfig config;
  config.name = "object_cache";
  config.numOps = 1;
  config.numThreads = 1;
  config.numKeys = 4;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.keySizeRange = std::vector<double>{8, 9};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRange = std::vector<double>{16};
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.popularityBuckets = std::vector<size_t>{1};
  workloadConfig.popularityWeights = std::vector<double>{1.0};
  workloadConfig.setRatio = 1.0;
  return config;
}

StressorConfig makeDeterministicStressorConfig() {
  StressorConfig config;
  config.numOps = 6;
  config.numThreads = 1;
  config.name = "object_cache";
  return config;
}

TEST(ObjectCacheStressorTest, DispatchesFromFactory) {
  auto stressor =
      Stressor::makeStressor(makeCacheConfig(), makeWorkloadStressorConfig());
  ASSERT_NE(nullptr, stressor);
  EXPECT_NE(nullptr,
            dynamic_cast<ObjectCacheStressor<LruAllocator>*>(stressor.get()));
}

TEST(ObjectCacheStressorTest, RunsDeterministicObjectCacheWorkload) {
  ObjectCacheStressor<LruAllocator> stressor{
      makeCacheConfig(), makeDeterministicStressorConfig(),
      std::make_unique<DeterministicGenerator>()};

  stressor.start();
  stressor.finish();

  const auto throughput = stressor.aggregateThroughputStats();
  EXPECT_EQ(6, throughput.ops);
  EXPECT_EQ(1, throughput.set);
  EXPECT_EQ(4, throughput.get);
  EXPECT_EQ(1, throughput.getMiss);
  EXPECT_EQ(1, throughput.update);
  EXPECT_EQ(0, throughput.updateMiss);
  EXPECT_EQ(1, throughput.del);
  EXPECT_EQ(0, throughput.delNotFound);

  auto stats = stressor.getCacheStats();
  auto* cacheStats = stats->asPtr<const Stats>();
  ASSERT_NE(nullptr, cacheStats);
  EXPECT_EQ(0, cacheStats->numItems);
}

} // namespace
} // namespace facebook::cachelib::cachebench
