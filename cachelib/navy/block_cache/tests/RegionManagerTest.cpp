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
#include <vector>

#include "cachelib/common/inject_pause.h"
#include "cachelib/navy/block_cache/Allocator.h"
#include "cachelib/navy/block_cache/LruPolicy.h"
#include "cachelib/navy/block_cache/RegionManager.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"
#include "cachelib/navy/testing/SeqPoints.h"

namespace facebook::cachelib::navy::tests {
namespace {
const Region kRegion0{RegionId{0}, 100};
const Region kRegion1{RegionId{1}, 100};
const Region kRegion2{RegionId{2}, 100};
const Region kRegion3{RegionId{3}, 100};
constexpr uint16_t kFlushRetryLimit = 10;
} // namespace

TEST(RegionManager, ReclaimLruAsFifo) {
  auto policy = std::make_unique<LruPolicy>(4);
  auto& ep = *policy;
  ep.track(kRegion0);
  ep.track(kRegion1);
  ep.track(kRegion2);
  ep.track(kRegion3);

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy),
      kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

  // without touch, the first region inserted is reclaimed
  EXPECT_EQ(kRegion0.id(), rm->evict());
  EXPECT_EQ(kRegion1.id(), rm->evict());
  EXPECT_EQ(kRegion2.id(), rm->evict());
  EXPECT_EQ(kRegion3.id(), rm->evict());
}

TEST(RegionManager, ReclaimLru) {
  auto policy = std::make_unique<LruPolicy>(4);
  auto& ep = *policy;
  ep.track(kRegion0);
  ep.track(kRegion1);
  ep.track(kRegion2);
  ep.track(kRegion3);

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy),
      kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

  rm->touch(kRegion0.id());
  rm->touch(kRegion1.id());

  EXPECT_EQ(kRegion2.id(), rm->evict());
  EXPECT_EQ(kRegion3.id(), rm->evict());
  EXPECT_EQ(kRegion0.id(), rm->evict());
  EXPECT_EQ(kRegion1.id(), rm->evict());
}

TEST(RegionManager, Recovery) {
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  folly::IOBufQueue ioq;
  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    expectRegionsTracked(*policy, {0, 1, 2, 3});
    RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
        std::move(cleanupCb), std::move(policy),
        kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

    // Empty region, like it was evicted and reclaimed
    for (int i = 0; i < 20; i++) {
      auto [desc, addr] = rm->getRegion(RegionId{1}).openAndAllocate(101);
      rm->getRegion(RegionId{1}).close(std::move(desc));
    }
    for (int i = 0; i < 30; i++) {
      auto [desc, addr] = rm->getRegion(RegionId{2}).openAndAllocate(101);
      rm->getRegion(RegionId{2}).close(std::move(desc));
    }

    auto rw = createMemoryRecordWriter(ioq);
    rm->persist(*rw);
  }

  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    // Region 0 - 3 will be tracked at least once since the first time
    // is when RegionManager is initialized. When the RM is recovered,
    {
      testing::InSequence s;
      EXPECT_CALL(*policy, reset());
      // First all regions are tracked when region manager is created
      expectRegionsTracked(*policy, {0, 1, 2, 3});
      EXPECT_CALL(*policy, reset());
      // Non-empty regions are tracked at last.
      expectRegionsTracked(*policy, {0, 3, 1, 2});
    }

    RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
        std::move(cleanupCb), std::move(policy),
        kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

    auto rr = createMemoryRecordReader(ioq);
    rm->recover(*rr);

    EXPECT_EQ(0, rm->getRegion(RegionId{0}).getLastEntryEndOffset());
    EXPECT_EQ(0, rm->getRegion(RegionId{0}).getNumItems());

    EXPECT_EQ(2020, rm->getRegion(RegionId{1}).getLastEntryEndOffset());
    EXPECT_EQ(20, rm->getRegion(RegionId{1}).getNumItems());

    EXPECT_EQ(3030, rm->getRegion(RegionId{2}).getLastEntryEndOffset());
    EXPECT_EQ(30, rm->getRegion(RegionId{2}).getNumItems());

    // this is a region that was not assigned to anything.
    EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());
    EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());
  }
}

TEST(RegionManager, ReadWrite) {
  constexpr uint64_t kBaseOffset = 1024;
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;

  auto device = createMemoryDevice(kBaseOffset + kNumRegions * kRegionSize,
                                   nullptr /* encryption */);
  auto devicePtr = device.get();
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, kBaseOffset, *device, 1, 1, 0,
      std::move(evictCb), std::move(cleanupCb), std::make_unique<LruPolicy>(4),
      kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_done");

  constexpr uint32_t kLocalOffset = 3 * 1024;
  constexpr uint32_t kSize = 1024;
  BufferGen bg;
  RegionId rid;
  // do reclaim couple of times to get RegionId of 1
  rm->startReclaim();
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
  ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
  ASSERT_EQ(0, rid.index());
  rm->startReclaim();
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
  ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
  ASSERT_EQ(1, rid.index());

  auto& region = rm->getRegion(rid);
  auto [wDesc, addr] = region.openAndAllocate(4 * kSize);
  EXPECT_EQ(OpenStatus::Ready, wDesc.status());
  auto buf = bg.gen(kSize);
  auto wAddr = RelAddress{rid, kLocalOffset};
  rm->write(wAddr, buf.copy());
  auto rDesc = rm->openForRead(rid, 1);
  auto bufRead = rm->read(rDesc, wAddr, kSize);
  EXPECT_TRUE(bufRead.size() == kSize);
  EXPECT_EQ(buf.view(), bufRead.view());

  // flush buffer
  region.close(std::move(wDesc));
  EXPECT_EQ(Region::FlushRes::kSuccess, rm->flushBuffer(rid));
  // Check device directly at the offset we expect data to be written
  auto expectedOfs = kBaseOffset + kRegionSize + kLocalOffset;
  Buffer bufReadDirect{kSize};
  EXPECT_TRUE(devicePtr->read(expectedOfs, kSize, bufReadDirect.data()));
  EXPECT_EQ(buf.view(), bufReadDirect.view());
}

TEST(RegionManager, RecoveryLRUOrder) {
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  folly::IOBufQueue ioq;
  {
    auto policy = std::make_unique<LruPolicy>(kNumRegions);
    RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
        std::move(cleanupCb), std::move(policy),
        kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

    // Mark 1 and 2 clean (num entries == 0), 0 and 3 used. After recovery, LRU
    // should return clean before used, in order of index.
    for (int i = 0; i < 10; i++) {
      auto [desc, addr] = rm->getRegion(RegionId{0}).openAndAllocate(200);
      rm->getRegion(RegionId{0}).close(std::move(desc));
    }
    for (int i = 0; i < 20; i++) {
      auto [desc, addr] = rm->getRegion(RegionId{3}).openAndAllocate(150);
      rm->getRegion(RegionId{3}).close(std::move(desc));
    }

    auto rw = createMemoryRecordWriter(ioq);
    rm->persist(*rw);
  }

  {
    auto policy = std::make_unique<LruPolicy>(kNumRegions);
    RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
        std::move(cleanupCb), std::move(policy),
        kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

    auto rr = createMemoryRecordReader(ioq);
    rm->recover(*rr);

    EXPECT_EQ(RegionId{1}, rm->evict());
    EXPECT_EQ(RegionId{2}, rm->evict());
    EXPECT_EQ(RegionId{0}, rm->evict());
    EXPECT_EQ(RegionId{3}, rm->evict());
    EXPECT_EQ(RegionId{}, rm->evict()); // Invalid
  }
}

TEST(RegionManager, Fragmentation) {
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device =
      createMemoryDevice(kNumRegions * kRegionSize, nullptr /* encryption */);

  folly::IOBufQueue ioq;
  uint32_t fragmentationSize = 2 * kRegionSize;
  {
    auto policy = std::make_unique<LruPolicy>(kNumRegions);
    RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
        std::move(cleanupCb), std::move(policy),
        kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

    // Mark 1 and 2 clean (num entries == 0), 0 and 3 used. After recovery, LRU
    // should return clean before used, in order of index.
    for (int i = 0; i < 10; i++) {
      auto [desc, addr] = rm->getRegion(RegionId{0}).openAndAllocate(200);
      rm->getRegion(RegionId{0}).close(std::move(desc));
      fragmentationSize -= 200;
    }
    for (int i = 0; i < 20; i++) {
      auto [desc, addr] = rm->getRegion(RegionId{3}).openAndAllocate(150);
      rm->getRegion(RegionId{3}).close(std::move(desc));
      fragmentationSize -= 150;
    }

    // Even though we allocated, but we haven't tracked any. So fragmentation
    // stats are still zero.
    rm->getCounters({[](folly::StringPiece name, double count) {
      if (name == "navy_bc_external_fragmentation") {
        EXPECT_EQ(0, count);
      }
    }});

    auto rw = createMemoryRecordWriter(ioq);
    rm->persist(*rw);
  }

  {
    auto policy = std::make_unique<LruPolicy>(kNumRegions);
    RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
    RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
        std::move(cleanupCb), std::move(policy),
        kNumRegions /* numInMemBuffers */, 0, kFlushRetryLimit);

    rm->getCounters({[](folly::StringPiece name, double count) {
      if (name == "navy_bc_external_fragmentation") {
        EXPECT_EQ(0, count);
      }
    }});
    auto rr = createMemoryRecordReader(ioq);
    rm->recover(*rr);
    rm->getCounters(
        {[fragmentationSize](folly::StringPiece name, double count) {
          if (name == "navy_bc_external_fragmentation") {
            EXPECT_EQ(fragmentationSize, count);
          }
        }});

    EXPECT_EQ(RegionId{1}, rm->evict());
    EXPECT_EQ(RegionId{2}, rm->evict());
    EXPECT_EQ(RegionId{0}, rm->evict());
    EXPECT_EQ(RegionId{3}, rm->evict());
    EXPECT_EQ(RegionId{}, rm->evict()); // Invalid
  }
}

using testing::_;
using testing::Return;
TEST(RegionManager, cleanupRegionFailureSync) {
  // This test case tests if the flush is blocked on cleaning up and
  // detaching buffer while there are outstanding readers when
  // async flush is failed with an error due to the injected write failure
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  constexpr uint16_t kNumInMemBuffer = 2;
  auto device = std::make_unique<MockDevice>(kNumRegions * kRegionSize, 1024);
  auto policy = std::make_unique<LruPolicy>(kNumRegions);
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy), kNumInMemBuffer, 0,
      kFlushRetryLimit);

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_done");
  injectPauseSet("pause_flush_failure");

  BufferGen bg;
  RegionId rid;
  // Reclaim to get Region 0
  rm->startReclaim();
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

  ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
  ASSERT_EQ(0, rid.index());

  // Write to Region 0
  auto& region = rm->getRegion(rid);
  auto [wDesc, addr] = region.openAndAllocate(kRegionSize);
  EXPECT_EQ(OpenStatus::Ready, wDesc.status());
  auto buf = bg.gen(1024);
  auto wAddr = RelAddress{rid, 0};
  rm->write(wAddr, buf.copy());
  region.close(std::move(wDesc));

  SeqPoints sp;
  std::thread readThread{[&sp, &region] {
    auto rDesc = region.openForRead();
    EXPECT_EQ(OpenStatus::Ready, rDesc.status());
    sp.reached(0); // unblock flush

    sp.wait(1); // block here
    region.close(std::move(rDesc));
  }};

  std::thread flushThread{[&sp, &device, &rm, &rid] {
    // Make sure flush will fail
    EXPECT_CALL(*device, writeImpl(_, _, _, _)).WillRepeatedly(Return(false));
    sp.wait(0); // Flush after active reader
    rm->doFlush(rid, false /* async */);
  }};

  std::thread countThread{[&sp, &rm] {
    bool retried = false;
    // Wait for a cleanup retry up to 10s
    for (int i = 0; i < 100; i++) {
      rm->getCounters({[&retried](folly::StringPiece name, double count,
                                  CounterVisitor::CounterType type) {
        if (name == "navy_bc_inmem_flush_retries" &&
            type == CounterVisitor::CounterType::RATE) {
          if (count >= kFlushRetryLimit) {
            retried = true;
          }
        }
      }});
      if (retried) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    // Verify there is a cleanup retry
    EXPECT_TRUE(retried);

    // Flush cannot be completed until the region is open for read
    EXPECT_FALSE(injectPauseWait("pause_flush_failure", 1, true, 1000));

    // Unblock readThread to close the region
    sp.reached(1);
  }};

  // Flush can complete now
  EXPECT_TRUE(injectPauseWait("pause_flush_failure"));

  readThread.join();
  flushThread.join();
  countThread.join();

  // Verify other counters
  rm->getCounters({[](folly::StringPiece name, double count,
                      CounterVisitor::CounterType type) {
    if (name == "navy_bc_inmem_flush_retries" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(kFlushRetryLimit, count);
    }
    if (name == "navy_bc_inmem_flush_failures" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_inmem_waiting_flush") {
      EXPECT_EQ(0, count);
    }
  }});
}

TEST(RegionManager, cleanupRegionFailureAsync) {
  // This test case tests if the flush is blocked on cleaning up and
  // detaching buffer while there are outstanding readers when
  // sync flush is failed with an error due to the injected write failure
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  constexpr uint16_t kNumInMemBuffer = 2;
  auto device = std::make_unique<MockDevice>(kNumRegions * kRegionSize, 1024);
  auto policy = std::make_unique<LruPolicy>(kNumRegions);
  RegionEvictCallback evictCb{[](RegionId, BufferView) { return 0; }};
  RegionCleanupCallback cleanupCb{[](RegionId, BufferView) {}};
  auto rm = std::make_unique<RegionManager>(
      kNumRegions, kRegionSize, 0, *device, 1, 1, 0, std::move(evictCb),
      std::move(cleanupCb), std::move(policy), kNumInMemBuffer, 0,
      kFlushRetryLimit);

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_done");
  injectPauseSet("pause_flush_begin");
  injectPauseSet("pause_flush_failure");

  BufferGen bg;
  RegionId rid;
  // Reclaim to get Region 0
  rm->startReclaim();
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));

  ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
  ASSERT_EQ(0, rid.index());

  // Write to Region 0
  auto& region = rm->getRegion(rid);
  auto [wDesc, addr] = region.openAndAllocate(kRegionSize);
  EXPECT_EQ(OpenStatus::Ready, wDesc.status());
  auto buf = bg.gen(1024);
  auto wAddr = RelAddress{rid, 0};
  rm->write(wAddr, buf.copy());
  region.close(std::move(wDesc));

  SeqPoints sp;
  std::thread readThread{[&sp, &region] {
    auto rDesc = region.openForRead();
    EXPECT_EQ(OpenStatus::Ready, rDesc.status());
    sp.reached(0); // unblock flush

    sp.wait(1); // block here
    region.close(std::move(rDesc));
  }};

  std::thread flushThread{[&sp, &device, &rm, &rid] {
    // Make sure flush will fail
    EXPECT_CALL(*device, writeImpl(_, _, _, _)).WillRepeatedly(Return(false));
    sp.wait(0); // Flush after active reader
    rm->doFlush(rid, true /* async */);
  }};

  std::thread countThread{[&sp, &rm] {
    bool retried = false;
    EXPECT_TRUE(injectPauseWait("pause_flush_begin"));
    // Wait for a cleanup retry upto 10s
    for (int i = 0; i < 100; i++) {
      rm->getCounters({[&retried](folly::StringPiece name, double count,
                                  CounterVisitor::CounterType type) {
        if (name == "navy_bc_inmem_flush_retries" &&
            type == CounterVisitor::CounterType::RATE) {
          if (count >= kFlushRetryLimit) {
            retried = true;
          }
        }
      }});
      if (retried) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    // Verify there is a cleanup retry
    EXPECT_TRUE(retried);

    // Flush cannot be completed while the region is open for read
    EXPECT_FALSE(injectPauseWait("pause_flush_failure", 1, true, 1000));

    // Unblock readThread to close the region
    sp.reached(1);
  }};

  // Flush can complete now
  EXPECT_TRUE(injectPauseWait("pause_flush_failure"));

  readThread.join();
  flushThread.join();
  countThread.join();

  // Verify other counters
  rm->getCounters({[](folly::StringPiece name, double count,
                      CounterVisitor::CounterType type) {
    if (name == "navy_bc_inmem_flush_retries" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(kFlushRetryLimit, count);
    }
    if (name == "navy_bc_inmem_flush_failures" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_inmem_waiting_flush") {
      EXPECT_EQ(0, count);
    }
  }});
}
} // namespace facebook::cachelib::navy::tests
