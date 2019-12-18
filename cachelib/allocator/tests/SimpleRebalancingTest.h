#pragma once

#include <future>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/PoolRebalancer.h"

namespace facebook {
namespace cachelib {
namespace tests {

struct SimpleRebalanceStrategy : public RebalanceStrategy {
  // Figure out which allocation class has the highest number of allocations
  // and release
 public:
  SimpleRebalanceStrategy() : RebalanceStrategy(PickNothingOrTest) {}

 private:
  ClassId pickVictim(const CacheBase& allocator, PoolId pid) {
    auto poolStats = allocator.getPoolStats(pid);
    ClassId cid = Slab::kInvalidClassId;
    uint64_t maxActiveAllocs = 0;
    for (size_t i = 0; i < poolStats.mpStats.acStats.size(); ++i) {
      const auto& acStats = poolStats.mpStats.acStats[i];
      if (maxActiveAllocs < acStats.activeAllocs) {
        maxActiveAllocs = acStats.activeAllocs;
        cid = i;
      }
    }
    return cid;
  }

  ClassId pickVictimImpl(const CacheBase& allocator, PoolId pid) override {
    return pickVictim(allocator, pid);
  }

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& allocator,
                                             PoolId pid) override {
    return {pickVictim(allocator, pid), Slab::kInvalidClassId};
  }
};

template <typename AllocatorT>
class SimpleRebalanceTest : public testing::Test {
 public:
  void testPoolRebalancerCreation() {
    std::set<std::string> evictedKeys;
    std::mutex lock;
    auto evictCb = [&](const typename AllocatorT::RemoveCbData& data) {
      std::unique_lock<std::mutex> l{lock};
      const auto key = data.item.getKey();
      evictedKeys.insert({key.data(), key.size()});
    };

    typename AllocatorT::Config config;
    config.enablePoolRebalancing(std::make_shared<SimpleRebalanceStrategy>(),
                                 std::chrono::seconds{1});
    config.setRemoveCallback(evictCb);
    config.setCacheSize(10 * Slab::kSize);

    AllocatorT alloc(config);
    const size_t numBytes = alloc.getCacheMemoryStats().cacheSize;
    auto poolId = alloc.addPool("foobar", numBytes);

    std::vector<typename AllocatorT::ItemHandle> handles;
    const std::vector<uint32_t> sizes{64,   128,  256,  512,
                                      1024, 2048, 4096, 8192};

    int i = 0;
    while (!alloc.getPool(poolId).allSlabsAllocated()) {
      const uint32_t size = sizes[folly::Random::oneIn(sizes.size())];
      auto handle = util::allocateAccessible(alloc, poolId,
                                             folly::to<std::string>(i), size);
      if (!handle) {
        break;
      }
      ++i;
      handles.push_back(std::move(handle));
    }

    ASSERT_FALSE(handles.empty());
    handles.clear();

    // Sleep for 2 seconds to let the rebalancing work
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Evicted keys shouldn't be in the allocator anymore
    ASSERT_FALSE(evictedKeys.empty());
    for (auto key : evictedKeys) {
      ASSERT_EQ(nullptr, alloc.find(key));
    }
  }

  void testMultiplePools() {
    std::set<std::string> evictedKeys;
    std::set<PoolId> evictedKeysPid;
    std::mutex lock;
    auto evictCb = [&](const typename AllocatorT::RemoveCbData& data) {
      std::unique_lock<std::mutex> l{lock};
      const auto key = data.item.getKey();
      evictedKeys.insert({key.data(), key.size()});

      auto pid = reinterpret_cast<const PoolId*>(data.item.getMemory());
      evictedKeysPid.insert(*pid);
    };

    typename AllocatorT::Config config;
    config.enablePoolRebalancing(std::make_shared<SimpleRebalanceStrategy>(),
                                 std::chrono::seconds{1});
    config.setRemoveCallback(evictCb);
    config.setCacheSize(20 * Slab::kSize);

    AllocatorT alloc(config);
    const size_t numBytes = alloc.getCacheMemoryStats().cacheSize;
    const unsigned int numPools = 5;
    std::vector<PoolId> pidList;
    for (unsigned int i = 0; i < numPools; ++i) {
      pidList.push_back(
          alloc.addPool(folly::sformat("foobar{}", i), numBytes / numPools));
    }

    const std::vector<uint32_t> sizes{64,   128,  256,  512,
                                      1024, 2048, 4096, 8192};
    int i = 0;
    for (auto pid : pidList) {
      while (!alloc.getPool(pid).allSlabsAllocated()) {
        // we should not be rebalancing until the pool is filled up.
        ASSERT_EQ(evictedKeysPid.end(), evictedKeysPid.find(pid));
        const uint32_t size = sizes[folly::Random::oneIn(sizes.size())];
        auto handle = util::allocateAccessible(alloc, pid,
                                               folly::to<std::string>(i), size);
        if (!handle) {
          break;
        }
        PoolId* mem = reinterpret_cast<PoolId*>(handle->getMemory());
        *mem = pid;
        ++i;
      }
    }

    // Sleep to let the rebalancing work
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(2 * numPools));

    // Evicted keys shouldn't be in the allocator anymore
    ASSERT_FALSE(evictedKeys.empty());
    for (auto key : evictedKeys) {
      ASSERT_EQ(nullptr, alloc.find(key));
    }
    // We have rebalanced all the pools
    ASSERT_EQ(numPools, evictedKeysPid.size());
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook
