/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Random.h>

#include <numeric>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

using LruAllocatorConfig = CacheAllocatorConfig<LruAllocator>;
using LruMemoryTierConfigs = LruAllocatorConfig::MemoryTierConfigs;
using Strings = std::vector<std::string>;
using Ratios = std::vector<size_t>;

constexpr size_t MB = 1024ULL * 1024ULL;
constexpr size_t GB = MB * 1024ULL;

const size_t defaultTotalCacheSize{1 * GB};
const std::string defaultCacheDir{"/tmp/metadataDir"};

template <typename Allocator>
class MemoryTiersTest : public AllocatorTest<Allocator> {
 public:
  void basicCheck(LruAllocatorConfig& actualConfig,
                  size_t expectedTotalCacheSize = defaultTotalCacheSize,
                  const std::string& expectedCacheDir = defaultCacheDir) {
    EXPECT_EQ(actualConfig.getCacheSize(), expectedTotalCacheSize);
    auto configs = actualConfig.getMemoryTierConfigs();

    size_t sum_ratios = std::accumulate(
        configs.begin(), configs.end(), 0UL,
        [](const size_t i, const MemoryTierCacheConfig& config) {
          return i + config.getRatio();
        });
    size_t sum_sizes = std::accumulate(
        configs.begin(), configs.end(), 0UL,
        [&](const size_t i, const MemoryTierCacheConfig& config) {
          return i + config.calculateTierSize(actualConfig.getCacheSize(),
                                              sum_ratios);
        });

    EXPECT_GE(expectedTotalCacheSize, sum_ratios * Slab::kSize);
    EXPECT_LE(sum_sizes, expectedTotalCacheSize);
    EXPECT_GE(sum_sizes, expectedTotalCacheSize - configs.size() * Slab::kSize);
  }

  LruAllocatorConfig createTestCacheConfig(
      const Ratios& tierRatios = {1},
      bool setPosixForShm = true,
      size_t cacheSize = defaultTotalCacheSize,
      const std::string& cacheDir = defaultCacheDir) {
    LruAllocatorConfig cfg;
    cfg.setCacheSize(cacheSize).enableCachePersistence(cacheDir);

    if (setPosixForShm)
      cfg.usePosixForShm();
    LruMemoryTierConfigs tierConfigs;
    tierConfigs.reserve(tierRatios.size());
    for (auto i = 0; i < tierRatios.size(); ++i) {
      tierConfigs.push_back(MemoryTierCacheConfig::fromShm()
                                .setRatio(tierRatios[i])
                                .setMemBind(std::string("0")));
    }

    cfg.configureMemoryTiers(tierConfigs);
    return cfg;
  }

  LruAllocatorConfig createTieredCacheConfig(size_t totalCacheSize,
                                             size_t numTiers = 2) {
    LruAllocatorConfig tieredCacheConfig{};
    std::vector<MemoryTierCacheConfig> configs;
    for (auto i = 1; i <= numTiers; ++i) {
      configs.push_back(MemoryTierCacheConfig::fromShm().setRatio(1).setMemBind(
          std::string("0")));
    }
    tieredCacheConfig.setCacheSize(totalCacheSize)
        .enableCachePersistence(
            folly::sformat("/tmp/multi-tier-test/{}", ::getpid()))
        .usePosixForShm()
        .configureMemoryTiers(configs);
    return tieredCacheConfig;
  }

  LruAllocatorConfig createDramCacheConfig(size_t totalCacheSize) {
    LruAllocatorConfig dramConfig{};
    dramConfig.setCacheSize(totalCacheSize);
    return dramConfig;
  }

  void validatePoolSize(PoolId poolId,
                        std::unique_ptr<LruAllocator>& allocator,
                        size_t expectedSize) {
    size_t actualSize = allocator->getPoolSize(poolId);
    EXPECT_EQ(actualSize, expectedSize);
  }

  void testAddPool(std::unique_ptr<LruAllocator>& alloc,
                   size_t poolSize,
                   bool isSizeValid = true,
                   size_t numTiers = 2) {
    if (isSizeValid) {
      auto pool = alloc->addPool("validPoolSize", poolSize);
      EXPECT_LE(alloc->getPoolSize(pool), poolSize);
      if (poolSize >= numTiers * Slab::kSize)
        EXPECT_GE(alloc->getPoolSize(pool),
                  poolSize - numTiers * Slab::kSize);
    } else {
      EXPECT_THROW(alloc->addPool("invalidPoolSize", poolSize),
                   std::invalid_argument);
      // TODO: test this for all tiers
      EXPECT_EQ(alloc->getPoolIds().size(), 0);
    }
  }
};

using LruMemoryTiersTest = MemoryTiersTest<LruAllocator>;

TEST_F(LruMemoryTiersTest, TestValid1TierConfig) {
  LruAllocatorConfig cfg = createTestCacheConfig().validate();
  basicCheck(cfg);
}

TEST_F(LruMemoryTiersTest, TestValid2TierConfig) {
  LruAllocatorConfig cfg = createTestCacheConfig({1, 1});
  basicCheck(cfg);
}

TEST_F(LruMemoryTiersTest, TestValid2TierRatioConfig) {
  LruAllocatorConfig cfg = createTestCacheConfig({5, 2});
  basicCheck(cfg);
}

TEST_F(LruMemoryTiersTest, TestInvalid2TierConfigNumberOfPartitionsTooLarge) {
  EXPECT_THROW(createTestCacheConfig({defaultTotalCacheSize, 1}).validate(),
               std::invalid_argument);
}

TEST_F(LruMemoryTiersTest, TestInvalid2TierConfigSizesAndRatioNotSet) {
  EXPECT_THROW(createTestCacheConfig({1, 0}), std::invalid_argument);
}

TEST_F(LruMemoryTiersTest, TestInvalid2TierConfigRatiosCacheSizeNotSet) {
  EXPECT_THROW(createTestCacheConfig({1, 1}, true,
                                     /* cacheSize */ 0)
                   .validate(),
               std::invalid_argument);
}

TEST_F(LruMemoryTiersTest, TestInvalid2TierConfigRatioNotSet) {
  EXPECT_THROW(createTestCacheConfig({1, 0}), std::invalid_argument);
}

TEST_F(LruMemoryTiersTest, TestInvalid2TierConfigSizesNeCacheSize) {
  EXPECT_THROW(createTestCacheConfig({0, 0}), std::invalid_argument);
}

TEST_F(LruMemoryTiersTest, TestPoolAllocations) {
  std::vector<size_t> totalCacheSizes = {8 * GB, 2 * GB};

  static const size_t numExtraSizes = 4;
  static const size_t numExtraSlabs = 20;

  for (size_t i = 0; i < numExtraSizes; i++) {
    totalCacheSizes.push_back(totalCacheSizes.back() +
                              (folly::Random::rand64() % numExtraSlabs) *
                                  Slab::kSize);
  }

  size_t min_ratio = 1;
  size_t max_ratio = 111;

  static const size_t numCombinations = 10;

  for (auto totalCacheSize : totalCacheSizes) {
    for (size_t k = 0; k < numCombinations; k++) {
      const size_t i = folly::Random::rand32() % max_ratio + min_ratio;
      const size_t j = folly::Random::rand32() % max_ratio + min_ratio;
      LruAllocatorConfig cfg =
          createTestCacheConfig({i, j},
                                /* usePoisx */ true, totalCacheSize);
      basicCheck(cfg, totalCacheSize);

      std::unique_ptr<LruAllocator> alloc = std::unique_ptr<LruAllocator>(
          new LruAllocator(LruAllocator::SharedMemNew, cfg));

      size_t size = (folly::Random::rand64() %
                      (alloc->getCacheMemoryStats().ramCacheSize - Slab::kSize)) +
                    Slab::kSize;
      testAddPool(alloc, size, true);
    }
  }
}

TEST_F(LruMemoryTiersTest, TestPoolInvalidAllocations) {
  std::vector<size_t> totalCacheSizes = {48 * MB, 51 * MB, 256 * MB,
                                         1 * GB,  5 * GB,  8 * GB};
  size_t min_ratio = 1;
  size_t max_ratio = 111;

  static const size_t numCombinations = 10;

  for (auto totalCacheSize : totalCacheSizes) {
    for (size_t k = 0; k < numCombinations; k++) {
      const size_t i = folly::Random::rand32() % max_ratio + min_ratio;
      const size_t j = folly::Random::rand32() % max_ratio + min_ratio;
      LruAllocatorConfig cfg =
          createTestCacheConfig({i, j},
                                /* usePoisx */ true, totalCacheSize);

      std::unique_ptr<LruAllocator> alloc = nullptr;
      try {
         alloc = std::unique_ptr<LruAllocator>(
            new LruAllocator(LruAllocator::SharedMemNew, cfg));
      } catch(...) {
        // expection only if cache too small
        size_t sum_ratios = std::accumulate(
          cfg.getMemoryTierConfigs().begin(), cfg.getMemoryTierConfigs().end(), 0UL,
          [](const size_t i, const MemoryTierCacheConfig& config) {
            return i + config.getRatio();
        });
        auto tier1slabs = cfg.getMemoryTierConfigs()[0].calculateTierSize(cfg.getCacheSize(), sum_ratios) / Slab::kSize;
        auto tier2slabs = cfg.getMemoryTierConfigs()[1].calculateTierSize(cfg.getCacheSize(), sum_ratios) / Slab::kSize;
        EXPECT_TRUE(tier1slabs <= 2 || tier2slabs <= 2);

        continue;
      }

      size_t size = (folly::Random::rand64() % (100 * GB)) +
                    alloc->getCacheMemoryStats().ramCacheSize;
      testAddPool(alloc, size, false);
    }
  }
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
