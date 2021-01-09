#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "cachelib/navy/block_cache/Allocator.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
constexpr uint16_t kNoPriority = 0;
constexpr uint16_t kNumPriorities = 1;
} // namespace

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
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      sizeClasses, std::move(policy), 0, 0);

  Allocator allocator{*rm, kNumPriorities};
  EXPECT_EQ(0, ex.getQueueSize());

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    }
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1, 0);
  Allocator allocator{*rm, kNumPriorities};
  EXPECT_EQ(0, ex.getQueueSize());

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;
  for (uint32_t i = 0; i < 3; i++) {
    if (i == 0) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
      EXPECT_EQ(OpenStatus::Retry, desc.status());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
    }
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    EXPECT_EQ(0, ex.getQueueSize());
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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
      std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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
      sizeClasses, std::move(policy), 2 * sizeClasses.size() + 1, 0);
  Allocator allocator{*rm, kNumPriorities};
  EXPECT_EQ(0, ex.getQueueSize());

  RelAddress addr;
  uint32_t slotSize = 0;
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
  }
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

  {
    RegionDescriptor rdesc{OpenStatus::Error};
    {
      RegionDescriptor wdesc{OpenStatus::Retry};
      std::tie(wdesc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
      EXPECT_TRUE(wdesc.isReady());
      EXPECT_EQ(0, wdesc.id().index());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));
      EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

      rdesc = rm->openForRead(RegionId{0}, 2 /* seqNumber_ */);
      EXPECT_TRUE(rdesc.isReady());
      for (uint32_t j = 0; j < 15; j++) {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
        EXPECT_TRUE(desc.isReady());
        EXPECT_EQ(0, desc.id().index());
        rm->close(std::move(desc));
      }
      EXPECT_EQ(0, ex.getQueueSize());
      {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
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

TEST(Allocator, UsePriorities) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      std::vector<uint32_t>{} /* no size class */, std::move(policy), 0,
      3 /* numPriorities */);

  Allocator allocator{*rm, 3 /* numPriorities */};
  EXPECT_EQ(0, ex.getQueueSize());

  // Allocate one item from each priortiy, we should see each allocation
  // results in a new region being allocated for its priority
  for (uint16_t pri = 0; pri < 3; pri++) {
    auto [desc, slotSize, addr] = allocator.allocate(1024, pri);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

    std::tie(desc, slotSize, addr) = allocator.allocate(1024, pri);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{pri}, addr.rid());
    EXPECT_EQ(0, rm->getRegion(addr.rid()).getClassId());
    EXPECT_EQ(pri, rm->getRegion(addr.rid()).getPriority());
    EXPECT_EQ(0, addr.offset());
  }

  // Reclaim the very last region in the eviction policy
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
}

TEST(Allocator, UsePrioritiesSizeClass) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  constexpr uint32_t kNumRegions = 7;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      std::vector<uint32_t>{1024, 4096} /* size class */, std::move(policy), 0,
      3 /* numPriorities */);

  Allocator allocator{*rm, 3 /* numPriorities */};
  EXPECT_EQ(0, ex.getQueueSize());

  // Allocate one item from each priortiy, we should see each allocation
  // results in a new region being allocated for its priority
  for (uint16_t pri = 0; pri < 3; pri++) {
    auto [desc, slotSize, addr] = allocator.allocate(1024, pri);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

    std::tie(desc, slotSize, addr) = allocator.allocate(1024, pri);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{pri}, addr.rid());
    EXPECT_EQ(0, rm->getRegion(addr.rid()).getClassId());
    EXPECT_EQ(pri, rm->getRegion(addr.rid()).getPriority());
    EXPECT_EQ(0, addr.offset());
  }

  // Allocate
  for (uint16_t pri = 0; pri < 3; pri++) {
    auto [desc, slotSize, addr] = allocator.allocate(2048, pri);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));
    EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));

    std::tie(desc, slotSize, addr) = allocator.allocate(2048, pri);
    EXPECT_TRUE(desc.isReady());
    // +3 to the pri for rid since we allocated 3 regions in the for-loop above
    EXPECT_EQ(RegionId{static_cast<uint32_t>(pri + 3)}, addr.rid());
    EXPECT_EQ(1, rm->getRegion(addr.rid()).getClassId());
    EXPECT_EQ(pri, rm->getRegion(addr.rid()).getPriority());
    EXPECT_EQ(0, addr.offset());
  }

  // Reclaim the very last region in the eviction policy
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
  EXPECT_TRUE(ex.runFirstIf("reclaim.evict"));
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
