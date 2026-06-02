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

#include <random>
#include <unordered_set>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/WorkloadDistribution.h"
#include "cachelib/cachebench/workload/WorkloadGenerator.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
TEST(WorkloadGeneratorTest, SimplePiecewiseValueSizes) {
  StressorConfig config;
  config.numKeys = 1000;
  config.numOps = 10000;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  // piecewise probability for continuous values
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0, 0.0};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1, 10.3};

  WorkloadGenerator keygen{config};
  std::mt19937_64 gen;
  for (int i = 0; i < 1500; ++i) {
    const Request& r(keygen.getReq(0, gen));
    EXPECT_EQ(10, *(r.sizeBegin));
  }
}

TEST(WorkloadGeneratorTest, SimpleDiscreteValueSizes) {
  StressorConfig config;
  config.numKeys = 1000;
  config.numOps = 10000;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  // discrete value
  workloadConfig.valSizeRangeProbability = std::vector<double>{0.5, 0.4, 0.1};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1, 10.2};

  WorkloadGenerator keygen{config};
  std::mt19937_64 gen;
  for (int i = 0; i < 1500; ++i) {
    const Request& r(keygen.getReq(0, gen));
    EXPECT_EQ(10, *(r.sizeBegin));
  }
}

// If no TTL is configured, requests should carry ttlSecs=0 (no expiry).
TEST(WorkloadGeneratorTest, NoTtlByDefault) {
  StressorConfig config;
  config.numKeys = 100;
  config.numOps = 100;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  WorkloadGenerator keygen{config};
  std::mt19937_64 gen;
  for (int i = 0; i < 200; ++i) {
    const Request& r(keygen.getReq(0, gen));
    EXPECT_EQ(0u, r.ttlSecs);
  }
}

// Discrete TTL form (range.size() == probability.size()): every emitted ttl
// must be exactly one of the configured bucket values.
TEST(WorkloadGeneratorTest, DiscreteTtl) {
  StressorConfig config;
  config.numKeys = 1000;
  config.numOps = 1000;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  // Three discrete TTL classes; every emitted ttl must be one of them.
  const std::unordered_set<uint32_t> expectedTtls{60, 3600, 86400};
  workloadConfig.ttlSecsRange = std::vector<double>{60, 3600, 86400};
  workloadConfig.ttlSecsRangeProbability = std::vector<double>{0.5, 0.4, 0.1};

  WorkloadGenerator keygen{config};
  std::mt19937_64 gen;
  for (int i = 0; i < 2000; ++i) {
    const Request& r(keygen.getReq(0, gen));
    EXPECT_NE(expectedTtls.find(r.ttlSecs), expectedTtls.end())
        << "unexpected ttlSecs=" << r.ttlSecs;
  }
}

// Piecewise TTL form (range.size() == probability.size() + 1): every emitted
// ttl must fall within the configured [low, high] interval.
TEST(WorkloadGeneratorTest, PiecewiseTtl) {
  StressorConfig config;
  config.numKeys = 1000;
  config.numOps = 1000;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  // Single piecewise bucket [10, 30]; every ttl must fall in [10, 30].
  workloadConfig.ttlSecsRange = std::vector<double>{10, 30};
  workloadConfig.ttlSecsRangeProbability = std::vector<double>{1.0};

  WorkloadGenerator keygen{config};
  std::mt19937_64 gen;
  for (int i = 0; i < 2000; ++i) {
    const Request& r(keygen.getReq(0, gen));
    EXPECT_GE(r.ttlSecs, 10u);
    EXPECT_LE(r.ttlSecs, 30u);
  }
}

// TTL range/probability lengths that match neither form should be rejected.
TEST(WorkloadGeneratorTest, InvalidTtl) {
  StressorConfig config;
  config.numKeys = 100;
  config.numOps = 100;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  // ttl range and probability lengths do not align with either form.
  workloadConfig.ttlSecsRange = std::vector<double>{10, 20, 30};
  workloadConfig.ttlSecsRangeProbability = std::vector<double>{0.5};
  ASSERT_THROW(WorkloadGenerator keygen{config}, std::invalid_argument);

  // Probability without a range is a malformed TTL config, not default TTL.
  workloadConfig.ttlSecsRange = std::vector<double>{};
  workloadConfig.ttlSecsRangeProbability = std::vector<double>{1.0};
  ASSERT_THROW(WorkloadGenerator keygen{config}, std::invalid_argument);

  // A single range value with no probabilities defines no valid TTL bucket.
  workloadConfig.ttlSecsRange = std::vector<double>{10};
  workloadConfig.ttlSecsRangeProbability = std::vector<double>{};
  ASSERT_THROW(WorkloadGenerator keygen{config}, std::invalid_argument);
}

TEST(WorkloadGeneratorTest, InvalidValueSizes) {
  StressorConfig config;
  config.numKeys = 1000;
  config.numOps = 10000;
  config.numThreads = 1;
  config.poolDistributions.emplace_back();
  auto& workloadConfig = config.poolDistributions.back();
  workloadConfig.getRatio = 1.0;
  workloadConfig.keySizeRange = std::vector<double>{10, 11};
  workloadConfig.keySizeRangeProbability = std::vector<double>{1.0};
  config.opPoolDistribution = std::vector<double>{1.0};
  config.keyPoolDistribution = std::vector<double>{1.0};

  // more probability than value size intervals
  workloadConfig.valSizeRangeProbability = std::vector<double>{0.5, 0.4, 0.1};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1};

  ASSERT_THROW(WorkloadGenerator keygen{config}, std::invalid_argument);

  // less values than probabilties
  workloadConfig.valSizeRangeProbability = std::vector<double>{0.5};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1, 100.1};
  ASSERT_THROW(WorkloadGenerator keygen{config}, std::invalid_argument);
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
