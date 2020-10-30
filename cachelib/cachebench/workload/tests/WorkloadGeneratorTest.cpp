#include <gtest/gtest.h>
#include <random>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/WorkloadGenerator.h"
#include "cachelib/cachebench/workload/distributions/RangeDistribution.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
TEST(WorkloadGeneratorTest, Simple) {
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
  workloadConfig.valSizeRangeProbability = std::vector<double>{1.0};
  workloadConfig.valSizeRange = std::vector<double>{10.0, 10.1};

  WorkloadGenerator<RangeDistribution> keygen{config};
  std::mt19937_64 gen;
  for (int i = 0; i < 1500; ++i) {
    const Request& r(keygen.getReq(0, gen));
    EXPECT_EQ(10, *(r.sizeBegin));
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
