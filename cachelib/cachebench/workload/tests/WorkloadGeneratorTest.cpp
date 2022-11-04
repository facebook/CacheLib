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
  config.poolDistributions.push_back(DistributionConfig{});
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
  config.poolDistributions.push_back(DistributionConfig{});
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

TEST(WorkloadGeneratorTest, InvalidValueSizes) {
  StressorConfig config;
  config.numKeys = 1000;
  config.numOps = 10000;
  config.numThreads = 1;
  config.poolDistributions.push_back(DistributionConfig{});
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
