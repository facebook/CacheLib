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
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      std::move(cleanupCb), std::move(policy), 3, 0, kFlushRetryLimit);
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
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      std::move(cleanupCb), std::move(policy), 3, 0, kFlushRetryLimit);
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

  {
    RegionDescriptor rdesc{OpenStatus::Error};
    {
      RegionDescriptor wdesc{OpenStatus::Retry};
      std::tie(wdesc, slotSize, addr) = allocator.allocate(1024, kNoPriority);
      EXPECT_TRUE(wdesc.isReady());
      EXPECT_EQ(0, wdesc.id().index());
      EXPECT_TRUE(ex.runFirstIf("reclaim"));

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
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, ex, std::move(evictCb),
      std::move(cleanupCb), std::move(policy),
      kNumRegions /* numInMemBuffers */, 3 /* numPriorities */,
      kFlushRetryLimit);

  Allocator allocator{*rm, 3 /* numPriorities */};
  EXPECT_EQ(0, ex.getQueueSize());

  // Allocate one item from each priortiy, we should see each allocation
  // results in a new region being allocated for its priority
  for (uint16_t pri = 0; pri < 3; pri++) {
    auto [desc, slotSize, addr] = allocator.allocate(1024, pri);
    EXPECT_EQ(OpenStatus::Retry, desc.status());
    EXPECT_TRUE(ex.runFirstIf("reclaim"));

    std::tie(desc, slotSize, addr) = allocator.allocate(1024, pri);
    EXPECT_TRUE(desc.isReady());
    EXPECT_EQ(RegionId{pri}, addr.rid());
    EXPECT_EQ(pri, rm->getRegion(addr.rid()).getPriority());
    EXPECT_EQ(0, addr.offset());
  }

  // Reclaim the very last region in the eviction policy
  EXPECT_TRUE(ex.runFirstIf("reclaim"));
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
