#include "cachelib/navy/block_cache/Allocator.h"

#include "cachelib/navy/block_cache/tests/MockPolicy.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"

#include <algorithm>
#include <memory>
#include <random>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using testing::Return;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Allocator, RegionSync) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{1}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  std::vector<uint32_t> sizeClasses{1024, 2048};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(kNumRegions, kRegionSize, 0,
                                            *device, 1, ex, std::move(evictCb),
                                            sizeClasses, std::move(policy), 0);

  Allocator allocator{*rm};
  EXPECT_EQ(4, rm->numFree());
  EXPECT_EQ(0, ex.getQueueSize());

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_EQ(1, ex.getQueueSize());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_EQ(0, ex.getQueueSize());
    }
    EXPECT_EQ(3 - i, rm->numFree());
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(0, addr.offset());
      rm->close(std::move(desc));
    }
    EXPECT_EQ(1, ex.getQueueSize());
    // 15 allocs exhaust region's space. No reclaims scheduled.
    for (uint32_t j = 0; j < 15; j++) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(1024 * (j + 1), addr.offset());
      rm->close(std::move(desc));
    }
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_EQ(0, ex.getQueueSize());
  }
  EXPECT_EQ(0, ex.getQueueSize());

  // regions 0, 1 should be tracked. last region is still being written to.
  for (uint32_t i = 0; i < 2; i++) {
    EXPECT_EQ(16, rm->getRegion(RegionId{i}).getNumItems());
    EXPECT_EQ(1024 * 16, rm->getRegion(RegionId{i}).getLastEntryEndOffset());
  }
  EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());
  EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());

  // Write to region 3
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }

  // @numItems and @lastEntryEndOffset are updated synchronously, validate them
  EXPECT_EQ(1, rm->getRegion(RegionId{3}).getNumItems());
  EXPECT_EQ(1024, rm->getRegion(RegionId{3}).getLastEntryEndOffset());
}

TEST(Allocator, RegionSyncInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{1}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  std::vector<uint32_t> sizeClasses{1024, 2048};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1);
  Allocator allocator{*rm};
  EXPECT_EQ(4, rm->numFree());
  EXPECT_EQ(0, ex.getQueueSize());

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_EQ(1, ex.getQueueSize());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_EQ(0, ex.getQueueSize());
    }
    EXPECT_EQ(3 - i, rm->numFree());
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(desc.isReady());
      if (i > 0) {
        EXPECT_TRUE(ex.runFirstIf("reclaim"));
        EXPECT_TRUE(ex.runFirstIf("flush"));
      }
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(0, addr.offset());
      rm->close(std::move(desc));
    }
    if (i > 0) {
      EXPECT_EQ(0, ex.getQueueSize());
    } else {
      EXPECT_EQ(1, ex.getQueueSize());
    }
    // 15 allocs exhaust region's space. No reclaims scheduled.
    for (uint32_t j = 0; j < 15; j++) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(1024 * (j + 1), addr.offset());
      rm->close(std::move(desc));
    }
    if (i > 0) {
      EXPECT_EQ(0, ex.getQueueSize());
    } else {
      EXPECT_EQ(1, ex.getQueueSize());
    }
    if (i == 0) {
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
    }
    EXPECT_EQ(0, ex.getQueueSize());
  }
  EXPECT_EQ(0, ex.getQueueSize());

  // regions 0, 1 should be tracked. last region is still being written to.
  for (uint32_t i = 0; i < 2; i++) {
    EXPECT_EQ(16, rm->getRegion(RegionId{i}).getNumItems());
    EXPECT_EQ(1024 * 16, rm->getRegion(RegionId{i}).getLastEntryEndOffset());
  }
  EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());
  EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());

  // Write to region 3
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_EQ(2, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_EQ(2, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("flush"));
    EXPECT_EQ(0, ex.getQueueSize());
    rm->close(std::move(desc));
  }

  // @numItems and @lastEntryEndOffset are updated synchronously, validate them
  EXPECT_EQ(1, rm->getRegion(RegionId{3}).getNumItems());
  EXPECT_EQ(1024, rm->getRegion(RegionId{3}).getLastEntryEndOffset());
}

TEST(Allocator, PermanentAlloc) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  EXPECT_CALL(*policy, track(RegionId{2}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(kNumRegions, kRegionSize, 0,
                                            *device, 1, ex, std::move(evictCb),
                                            sizeClasses, std::move(policy), 0);
  Allocator allocator{*rm};
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  // Allocate a permanent item. Region 0 is permanent region and never tracked.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
    EXPECT_EQ(1, ex.getQueueSize());
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    rm->close(std::move(desc));
  }

  EXPECT_EQ(0, addr.offset());
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  // Check stack allocation strategy for permanent items
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(8 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    EXPECT_EQ(1024, addr.offset());
    rm->close(std::move(desc));
  }
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    EXPECT_EQ(9 * 1024, addr.offset());
    rm->close(std::move(desc));
  }
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    EXPECT_EQ(15 * 1024, addr.offset());
    // No reclamation scheduled
    EXPECT_EQ(0, ex.getQueueSize());
    rm->close(std::move(desc));
  }

  // This will trigger reclamation and region 0 would be put
  // for tracking.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{1}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_EQ(0, ex.getQueueSize());
    EXPECT_EQ(1, rm->numFree());
    rm->close(std::move(desc));
  }

  // Now allocate region full of regular items and check that region 0 will be
  // put for tracking.
  for (uint32_t i = 0; i < 4; i++) {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  EXPECT_EQ(0, rm->numFree());

  // This allocation will request a clean region and put region 2 for tracking.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    rm->close(std::move(desc));
  }
}

TEST(Allocator, PermanentAllocInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  EXPECT_CALL(*policy, track(RegionId{2}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1);
  Allocator allocator{*rm};
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  // Allocate a permanent item. Region 0 is permanent region and never tracked.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
  }

  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_EQ(1, ex.getQueueSize());
    rm->close(std::move(desc));
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  // Check stack allocation strategy for permanent items
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(8 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(1024, addr.offset());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(9 * 1024, addr.offset());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(15 * 1024, addr.offset());
  // No reclamation scheduled
  EXPECT_EQ(0, ex.getQueueSize());

  // This will trigger reclamation and region 0 would be put
  // for tracking.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{1}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_EQ(2, ex.getQueueSize());
    rm->close(std::move(desc));
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("flush"));
  EXPECT_EQ(0, ex.getQueueSize());
  EXPECT_EQ(1, rm->numFree());

  // Now allocate region full of regular items and check that region 0 will be
  // put for tracking.
  for (uint32_t i = 0; i < 4; i++) {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{2}, addr.rid());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  EXPECT_EQ(0, rm->numFree());

  // This allocation will request a clean region and put region 2 for tracking.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    rm->close(std::move(desc));
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("flush"));
}

TEST(Allocator, OutOfRegions) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto evictCb1 = evictCb;
  auto rm = std::make_unique<RegionManager>(kNumRegions, kRegionSize, 0,
                                            *device, 1, ex, std::move(evictCb),
                                            sizeClasses, std::move(policy), 0);
  Allocator allocator{*rm};
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  // Allocate only permanent items. Observe that nothing put for tracking.
  // Evict invalid region will cause failure to allocate. New reclamations
  // are not scheduled.
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_EQ(1, ex.getQueueSize());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_EQ(0, ex.getQueueSize());
    }
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(0, addr.offset());
      rm->close(std::move(desc));
    }
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(6 * 1024, addr.offset());
      rm->close(std::move(desc));
    }
  }
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  // Reclamation is not scheduled, raises "out of region" flag internally.
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(6 * 1024, addr.offset());
    rm->close(std::move(desc));
  }

  // Next allocation will be failed after retry. New reclamation job isn't
  // scheduled because "out of regions" flag.
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_EQ(OpenStatus::Error, desc.status());
  }
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_EQ(OpenStatus::Error, desc.status());
  }
  EXPECT_EQ(0, ex.getQueueSize());

  // Fail regular item too
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_EQ(OpenStatus::Error, desc.status());
  }
  EXPECT_EQ(0, ex.getQueueSize());
}

TEST(Allocator, OutOfRegionsInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto evictCb1 = evictCb;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1);
  Allocator allocator{*rm};
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  // Allocate only permanent items. Observe that nothing put for tracking.
  // Evict invalid region will cause failure to allocate. New reclamations
  // are not scheduled.
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_EQ(1, ex.getQueueSize());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_EQ(0, ex.getQueueSize());
    }
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(0, addr.offset());
      rm->close(std::move(desc));
    }
    if (i == 0) {
      EXPECT_EQ(1, ex.getQueueSize());
    } else {
      EXPECT_EQ(2, ex.getQueueSize());
    }
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    if (i > 0) {
      EXPECT_TRUE(ex.runFirstIf("flush"));
    }
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(6 * 1024, addr.offset());
      rm->close(std::move(desc));
    }
  }
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    rm->close(std::move(desc));
  }
  EXPECT_EQ(2, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("flush"));
  // Reclamation is not scheduled, raises "out of region" flag internally.
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(6 * 1024, addr.offset());
    rm->close(std::move(desc));
  }

  // Next allocation will be failed after retry. New reclamation job isn't
  // scheduled because "out of regions" flag.
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_EQ(OpenStatus::Error, desc.status());
  }
  EXPECT_TRUE(ex.runFirstIf("flush"));
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(6 * 1024, true);
    EXPECT_EQ(OpenStatus::Error, desc.status());
  }
  EXPECT_EQ(0, ex.getQueueSize());

  // Fail regular item too
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_EQ(OpenStatus::Error, desc.status());
  }
  EXPECT_EQ(0, ex.getQueueSize());
}

TEST(Allocator, TestInMemBufferStates) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  // EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{}));

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  std::vector<uint32_t> sizeClasses{1024, 2048};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1);
  Allocator allocator{*rm};
  EXPECT_EQ(4, rm->numFree());
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
  }
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_EQ(0, ex.getQueueSize());
  {
    {
      RegionDescriptor rdesc{OpenStatus::Error};
      {
        RegionDescriptor wdesc{OpenStatus::Retry};
        std::tie(wdesc, slotSize, addr) = allocator.allocate(1024, false);
        EXPECT_TRUE(wdesc.isReady());
        rdesc = rm->openForRead(RegionId{0}, 0);
        EXPECT_TRUE(rdesc.isReady());
        for (uint32_t j = 0; j < 15; j++) {
          RegionDescriptor desc{OpenStatus::Retry};
          std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
          EXPECT_TRUE(desc.isReady());
          rm->close(std::move(desc));
        }
        EXPECT_EQ(1, ex.getQueueSize());
        {
          RegionDescriptor desc{OpenStatus::Retry};
          std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
          EXPECT_EQ(OpenStatus::Retry, desc.status());
        }
        EXPECT_EQ(2, ex.getQueueSize());
        EXPECT_FALSE(rm->getRegion(RegionId{0}).isFlushedLocked());
        rm->close(std::move(wdesc));
      }
      EXPECT_FALSE(ex.runFirstIf("flush"));
      EXPECT_TRUE(rm->getRegion(RegionId{0}).isFlushedLocked());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      rm->close(std::move(rdesc));
    }
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("flush"));
  }
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
