#include "cachelib/navy/block_cache/Allocator.h"

#include "cachelib/navy/block_cache/tests/TestHelpers.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"

#include <algorithm>
#include <memory>
#include <random>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Allocator, RegionSync) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
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
  EXPECT_EQ(0, ex.getQueueSize());

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    }
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(0, addr.offset());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
      rm->close(std::move(desc));
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
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
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
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  std::vector<uint32_t> sizeClasses{1024};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1);
  Allocator allocator{*rm};
  EXPECT_EQ(0, ex.getQueueSize());

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    }
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(desc.isReady());
      if (i > 0) {
        EXPECT_TRUE(ex.runFirstIf("reclaim"));
        EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
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
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
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

  // Write to region 3. This will flush region 2, and will also trigger eviction
  // of region 0 again
  EXPECT_EQ(0, ex.getQueueSize());
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_EQ(2, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
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
  expectRegionsTracked(*policy, {0, 1, 2, 3, 2});

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
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
  }
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }

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

  // This will trigger reclamation of region 2. Region 1 will be left partially
  // written.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{1}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }

  // Now allocate region full of regular items and check that region 2 will be
  // put for tracking.
  for (uint32_t i = 0; i < 4; i++) {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{2}, addr.rid());
    rm->close(std::move(desc));
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

  // This allocation will request a clean region (rid: 3) and put region 2 for
  // tracking. After we make one allocation, we will again reclaim region 2 as
  // it is the only region avaiable for reclaim (others are pinned).
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }
}

TEST(Allocator, PermanentAllocInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  auto& mp = *policy;
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

  // Only 2 and 3 will be tracked as 0 and 1 will be pinned regions
  expectRegionsTracked(mp, {2, 3});
  RelAddress addr;
  uint32_t slotSize = 0;
  // Allocate a permanent item. Region 0 is permanent region and never tracked.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
  }

  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{0}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }
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

  // This will trigger reclamation
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, true);
    EXPECT_TRUE(desc.isReady());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    EXPECT_TRUE(ex.runFirstIf("flush"));
    EXPECT_EQ(RegionId{1}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    rm->close(std::move(desc));
  }

  // Now allocate region full of regular items and check that region 2 will be
  // put for tracking.
  for (uint32_t i = 0; i < 4; i++) {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{2}, addr.rid());
    rm->close(std::move(desc));
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

  // This allocation will request a clean region (rid: 3), and then we will
  // enqueue to reclaim a region but will fail as (rid: 2) hasn't been flushed
  // yet. After the first reclaim job fails, we will flush and then the reclaim
  // of (rid: 2) will succeed.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    // This reclaim will fail since the eviction will return an invalid rid
    EXPECT_FALSE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("flush"));
    // Reclaim is retried and succeeds this time around
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }

  // Finish allocating region 3
  for (uint32_t i = 0; i < 3; i++) {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    rm->close(std::move(desc));
  }

  // Allocate an item from region 2. Now we will try to evict (rid: 3),
  // which will fail since it has yet to be flushed. Then we'll run
  // the flush job and then run the reclaim again.
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{2}, addr.rid());
    EXPECT_FALSE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("flush"));
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    rm->close(std::move(desc));
  }
}

TEST(Allocator, TestInMemBufferStates) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  std::vector<uint32_t> sizeClasses{1024};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1);
  Allocator allocator{*rm};
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

  {
    RegionDescriptor rdesc{OpenStatus::Error};
    {
      RegionDescriptor wdesc{OpenStatus::Retry};
      std::tie(wdesc, slotSize, addr) = allocator.allocate(1024, false);
      EXPECT_TRUE(wdesc.isReady());
      EXPECT_EQ(0, wdesc.id().index());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

      rdesc = rm->openForRead(RegionId{0}, 2 /* seqNumber_ */);
      EXPECT_TRUE(rdesc.isReady());
      for (uint32_t j = 0; j < 15; j++) {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
        EXPECT_TRUE(desc.isReady());
        EXPECT_EQ(0, desc.id().index());
        rm->close(std::move(desc));
      }
      EXPECT_EQ(0, ex.getQueueSize());
      {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slotSize, addr) = allocator.allocate(1024, false);
        EXPECT_EQ(OpenStatus::Ready, desc.status());
        EXPECT_EQ(1, desc.id().index());
        rm->close(std::move(desc));
      }
      EXPECT_FALSE(rm->getRegion(RegionId{0}).isFlushedLocked());
      rm->close(std::move(wdesc));
    }
    EXPECT_EQ(2, ex.getQueueSize());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

    // we still have one flush job remaining
    EXPECT_EQ(1, ex.getQueueSize());
    EXPECT_FALSE(ex.runFirstIf("flush"));
    // flush didn't finish as there's still one reader
    EXPECT_EQ(1, ex.getQueueSize());

    EXPECT_TRUE(rm->getRegion(RegionId{0}).isFlushedLocked());
    rm->close(std::move(rdesc));
  }
  EXPECT_EQ(1, ex.getQueueSize());
  EXPECT_TRUE(ex.runFirstIf("flush"));
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
