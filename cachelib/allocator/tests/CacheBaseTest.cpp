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

#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

class CacheBaseTest : public CacheBase, public SlabAllocatorTestBase {
 public:
  CacheBaseTest()
      : slabAllocator_(createSlabAllocator(10)),
        memoryPool_(0, 1024, *slabAllocator_, {64}) {}
  const std::string getCacheName() const override { return cacheName; }
  bool isObjectCache() const override { return false; }
  const MemoryPool& getPool(PoolId) const override { return memoryPool_; }
  PoolStats getPoolStats(PoolId) const override { return PoolStats(); }
  AllSlabReleaseEvents getAllSlabReleaseEvents(PoolId) const override {
    return AllSlabReleaseEvents{};
  }
  PoolEvictionAgeStats getPoolEvictionAgeStats(PoolId,
                                               unsigned int) const override {
    return PoolEvictionAgeStats();
  }
  std::unordered_map<std::string, uint64_t> getEventTrackerStatsMap()
      const override {
    return {};
  }
  CacheMetadata getCacheMetadata() const noexcept override { return {}; }
  GlobalCacheStats getGlobalCacheStats() const override { return {}; }
  SlabReleaseStats getSlabReleaseStats() const override { return {}; }
  CacheMemoryStats getCacheMemoryStats() const override { return {}; }
  std::set<PoolId> getRegularPoolIdsForResize() const override { return {}; }
  std::set<PoolId> getRegularPoolIds() const override { return {}; }
  std::set<PoolId> getCCachePoolIds() const override { return {}; }
  std::set<PoolId> getPoolIds() const override { return {}; }
  std::string getPoolName(PoolId /* unused */) const override { return ""; }
  bool resizePools(PoolId, PoolId, size_t) override { return false; }
  std::map<std::string, std::string> serializeConfigParams() const override {
    return {};
  }
  void resizeCompactCaches() override {}
  void releaseSlab(PoolId, ClassId, SlabReleaseMode, const void*) override {}
  void releaseSlab(
      PoolId, ClassId, ClassId, SlabReleaseMode, const void*) override {}
  unsigned int reclaimSlabs(PoolId, size_t) override { return 0; }
  bool autoResizeEnabledForPool(PoolId) const override { return false; }

  const ICompactCache& getCompactCache(PoolId) const override {
    throw std::invalid_argument("");
  }

  util::StatsMap getNvmCacheStatsMap() const override { return {}; }
  void updateNumSlabsToAdvise(int32_t /* unused */) override final {}

  PoolAdviseReclaimData calcNumSlabsToAdviseReclaim() override final {
    return {};
  }

 protected:
  std::unique_ptr<SlabAllocator> slabAllocator_;
  MemoryPool memoryPool_;

 private:
  std::string cacheName{"CacheBaseTestCache"};
};

TEST_F(CacheBaseTest, RebalanceStrategyTest) {
  auto strategy1 = std::make_shared<RebalanceStrategy>();
  auto strategy2 = std::make_shared<RebalanceStrategy>();
  PoolId pid1(1);
  PoolId pid2(2);
  EXPECT_EQ(nullptr, getRebalanceStrategy(pid1));
  EXPECT_EQ(nullptr, getRebalanceStrategy(pid2));

  setRebalanceStrategy(pid1, strategy1);
  EXPECT_EQ(strategy1, getRebalanceStrategy(pid1));
  EXPECT_EQ(nullptr, getRebalanceStrategy(pid2));

  setRebalanceStrategy(pid2, strategy2);
  EXPECT_EQ(strategy1, getRebalanceStrategy(pid1));
  EXPECT_EQ(strategy2, getRebalanceStrategy(pid2));

  setRebalanceStrategy(pid2, strategy1);
  EXPECT_EQ(strategy1, getRebalanceStrategy(pid1));
  EXPECT_EQ(strategy1, getRebalanceStrategy(pid2));
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
