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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "cachelib/common/inject_pause.h"
#include "cachelib/navy/block_cache/Allocator.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"

namespace facebook::cachelib::navy::tests {
namespace {
constexpr uint16_t kNoPriority = 0;
constexpr uint16_t kNumPriorities = 1;
constexpr uint16_t kFlushRetryLimit = 10;
} // namespace

TEST(Allocator, RegionSyncInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy), 3, 0, kFlushRetryLimit);
  Allocator allocator{*rm, kNumPriorities};

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_done");
  injectPauseSet("pause_flush_begin");

  // Write to 3 regions
  RelAddress addr;
  uint32_t slotSize = 0;

  // First allocation will fail with kicking a reclaim
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) =
        allocator.allocate(1024, kNoPriority, false);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
    // Reclaim should have been started; complete the reclaim
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
  }

  for (uint32_t i = 0; i < 3; i++) {
    // First allocation take a clean region and schedules a reclaim job and
    // tracks the current region that is full region if present.
    {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) =
          allocator.allocate(1024, kNoPriority, false);
      EXPECT_TRUE(desc.isReady());

      if (i > 0) {
        // The first allocation should schedule flush for old region
        EXPECT_TRUE(injectPauseWait("pause_flush_begin"));
      }

      // Reclaim should have been started due to the new region allocation
      EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(0, addr.offset());
      rm->close(std::move(desc));
    }

    // 15 allocs exhaust region's space,but no reclaims scheduled.
    for (uint32_t j = 0; j < 15; j++) {
      RegionDescriptor desc{OpenStatus::Retry};
      std::tie(desc, slotSize, addr) =
          allocator.allocate(1024, kNoPriority, false);
      EXPECT_TRUE(desc.isReady());
      EXPECT_EQ(RegionId{i}, addr.rid());
      EXPECT_EQ(1024 * (j + 1), addr.offset());
      rm->close(std::move(desc));
    }
  }

  // regions 0, 1 should be tracked. last region is still being written to.
  for (uint32_t i = 0; i < 2; i++) {
    EXPECT_EQ(16, rm->getRegion(RegionId{i}).getNumItems());
    EXPECT_EQ(1024 * 16, rm->getRegion(RegionId{i}).getLastEntryEndOffset());
  }
  EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());
  EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());

  // Write to region 3. This will flush region 2, and will also trigger eviction
  // of region 0 again
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) =
        allocator.allocate(1024, kNoPriority, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{3}, addr.rid());
    EXPECT_EQ(0, addr.offset());
    // Run flush for old region and reclaim
    EXPECT_TRUE(injectPauseWait("pause_flush_begin"));
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
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
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy), 3, 0, kFlushRetryLimit);
  Allocator allocator{*rm, kNumPriorities};

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_done");
  injectPauseSet("pause_flush_begin");
  injectPauseSet("pause_flush_detach_buffer");
  injectPauseSet("pause_flush_done");

  RelAddress addr;
  uint32_t slotSize = 0;
  {
    RegionDescriptor desc{OpenStatus::Retry};
    std::tie(desc, slotSize, addr) =
        allocator.allocate(1024, kNoPriority, false);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
  }

  // Allocate should trigger the reclaim job
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

  {
    RegionDescriptor rdesc{OpenStatus::Error};
    {
      RegionDescriptor wdesc{OpenStatus::Retry};
      // There should be clean region available
      std::tie(wdesc, slotSize, addr) =
          allocator.allocate(1024, kNoPriority, false);
      EXPECT_TRUE(wdesc.isReady());
      EXPECT_EQ(0, wdesc.id().index());
      // Clean region is allocated, so another reclaim should
      // have been triggered
      EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

      rdesc = rm->openForRead(RegionId{0}, 2 /* seqNumber_ */);
      EXPECT_TRUE(rdesc.isReady());
      // Fill the remaining space in the first region
      for (uint32_t j = 0; j < 15; j++) {
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slotSize, addr) =
            allocator.allocate(1024, kNoPriority, false);
        EXPECT_TRUE(desc.isReady());
        EXPECT_EQ(0, desc.id().index());
        rm->close(std::move(desc));
      }
      // The reclaim should not be kicked yet
      EXPECT_FALSE(injectPauseWait("pause_reclaim_done", 1, true, 1000));
      {
        // Do another allocation to flush/close the first region.
        // A reclaim will also be triggered
        RegionDescriptor desc{OpenStatus::Retry};
        std::tie(desc, slotSize, addr) =
            allocator.allocate(1024, kNoPriority, false);
        EXPECT_EQ(OpenStatus::Ready, desc.status());
        EXPECT_EQ(1, desc.id().index());
        rm->close(std::move(desc));
      }
      // Another reclaim should be triggered and completed
      EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
      EXPECT_TRUE(injectPauseWait("pause_flush_begin"));
      // Flush buffer cannot be done because wdesc is open
      EXPECT_FALSE(injectPauseWait("pause_flush_detach_buffer", 1, true, 1000));
      EXPECT_FALSE(rm->getRegion(RegionId{0}).isFlushedLocked());
      rm->close(std::move(wdesc));
    }
    // wdesc is closed, so the buffer can be flushed now
    EXPECT_TRUE(injectPauseWait("pause_flush_detach_buffer"));
    // flush didn't finish as there's still one reader and thus the buffer
    // cannot be detached
    EXPECT_FALSE(injectPauseWait("pause_flush_done", 1 /* numThreads */,
                                 true /* wakeup */, 1000 /* timeoutMs */));
    EXPECT_TRUE(rm->getRegion(RegionId{0}).isFlushedLocked());
    // Close the reader, so the buffer can be detached and flush finish
    rm->close(std::move(rdesc));
  }
  EXPECT_TRUE(injectPauseWait("pause_flush_done"));
}

TEST(Allocator, UsePriorities) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<MockPolicy>(&hits);
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 16 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy),
      kNumRegions /* numInMemBuffers */, 3 /* numPriorities */,
      kFlushRetryLimit);

  Allocator allocator{*rm, 3 /* numPriorities */};

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_done");

  // Allocate to make sure a reclaim is triggered
  auto [desc, slotSize, addr] = allocator.allocate(1024, 0, false);
  EXPECT_EQ(OpenStatus::Retry, desc.status());
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

  // Allocate one item from each priortiy, we should see each allocation
  // results in a new region being allocated for its priority
  for (uint16_t pri = 0; pri < 3; pri++) {
    std::tie(desc, slotSize, addr) = allocator.allocate(1024, pri, false);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{pri}, addr.rid());
    EXPECT_EQ(pri, rm->getRegion(addr.rid()).getPriority());
    // The allocation should be from a fresh region
    EXPECT_EQ(0, addr.offset());

    // Reclaim should have been triggered
    EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
  }
}

} // namespace facebook::cachelib::navy::tests
