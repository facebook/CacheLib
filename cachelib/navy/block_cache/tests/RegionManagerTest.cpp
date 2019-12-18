#include "cachelib/navy/block_cache/RegionManager.h"

#include "cachelib/navy/block_cache/LruPolicy.h"
#include "cachelib/navy/block_cache/tests/MockPolicy.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"

#include <vector>

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Region, BlockAndLock) {
  Region r{RegionId(0), 1024};
  // Can't lock if access isn't blocked
  EXPECT_FALSE(r.tryLock());

  EXPECT_TRUE(r.blockAccess());
  EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
  EXPECT_TRUE(r.tryLock());
  EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
  r.unlock();
  r.unblockAccess();

  {
    auto desc1 = r.open(OpenMode::Write);
    EXPECT_EQ(desc1.status(), OpenStatus::Ready);
    {
      auto desc2 = r.open(OpenMode::Write);
      EXPECT_EQ(desc2.status(), OpenStatus::Ready);
      EXPECT_FALSE(r.blockAccess());
      // Access blocked, but active accessorts prevent from locking
      EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
      EXPECT_FALSE(r.tryLock());
      EXPECT_EQ(r.open(OpenMode::Write).status(), OpenStatus::Retry);
      r.close(std::move(desc2));
    }
    EXPECT_FALSE(r.blockAccess());
    EXPECT_FALSE(r.tryLock());
    r.close(std::move(desc1));
  }
  EXPECT_TRUE(r.blockAccess());
  // Second block is noop
  EXPECT_TRUE(r.blockAccess());
  EXPECT_TRUE(r.tryLock());
  // Doesn't lock second time: exclusive access
  EXPECT_FALSE(r.tryLock());
  r.unlock();
  r.unblockAccess();

  {
    auto desc = r.open(OpenMode::Write);
    EXPECT_EQ(desc.status(), OpenStatus::Ready);
    r.close(std::move(desc));
  }
}

TEST(RegionManager, ReclaimLruAsFifo) {
  auto policy = std::make_unique<LruPolicy>(4);
  auto& ep = *policy;
  ep.track(RegionId{0});
  ep.track(RegionId{1});
  ep.track(RegionId{2});
  ep.track(RegionId{3});

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device = createMemoryDevice(kNumRegions * kRegionSize);
  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(kNumRegions, kRegionSize, 0, 1024,
                                            *device, 1, ex, std::move(evictCb),
                                            sizeClasses, std::move(policy));

  // without touch, the first region inserted is reclaimed
  EXPECT_EQ(0, rm->evict().index());
  EXPECT_EQ(1, rm->evict().index());
  EXPECT_EQ(2, rm->evict().index());
  EXPECT_EQ(3, rm->evict().index());
}

TEST(RegionManager, ReclaimLru) {
  auto policy = std::make_unique<LruPolicy>(4);
  auto& ep = *policy;
  ep.track(RegionId{0});
  ep.track(RegionId{1});
  ep.track(RegionId{2});
  ep.track(RegionId{3});

  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device = createMemoryDevice(kNumRegions * kRegionSize);
  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(kNumRegions, kRegionSize, 0, 1024,
                                            *device, 1, ex, std::move(evictCb),
                                            sizeClasses, std::move(policy));

  rm->touch(RegionId{0});
  rm->touch(RegionId{1});

  EXPECT_EQ(2, rm->evict().index());
  EXPECT_EQ(3, rm->evict().index());
  EXPECT_EQ(0, rm->evict().index());
  EXPECT_EQ(1, rm->evict().index());
}

TEST(RegionManager, Recovery) {
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device = createMemoryDevice(kNumRegions * kRegionSize);

  folly::IOBufQueue ioq;
  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    std::vector<uint32_t> sizeClasses{4096};
    RegionEvictCallback evictCb{
        [](RegionId, uint32_t, BufferView) { return 0; }};
    MockJobScheduler ex;
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, 1024, *device, 1, ex, std::move(evictCb),
        sizeClasses, std::move(policy));

    // Get 3 regions, assign and allocate
    for (uint32_t i = 0; i < 3; i++) {
      EXPECT_EQ(RegionId{i}, rm->getFree());
    }
    // Empty region, like it was evicted and reclaimed
    rm->getRegion(RegionId{0}).setClassId(0);
    rm->getRegion(RegionId{1}).setClassId(0);
    for (int i = 0; i < 20; i++) {
      rm->getRegion(RegionId{1}).allocate(101);
    }
    rm->pin(rm->getRegion(RegionId{1}));
    rm->getRegion(RegionId{2}).setClassId(1);
    for (int i = 0; i < 30; i++) {
      rm->getRegion(RegionId{2}).allocate(101);
    }
    EXPECT_EQ(1, rm->pinnedCount());

    auto rw = createMemoryRecordWriter(ioq);
    rm->persist(*rw);

    // Change region manager after persistence
    EXPECT_EQ(RegionId{3}, rm->getFree());
  }

  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<MockPolicy>(&hits);
    EXPECT_CALL(*policy, track(RegionId{0}));
    EXPECT_CALL(*policy, track(RegionId{2}));
    // Do not touch empty region 0
    EXPECT_CALL(*policy, touch(RegionId{2}));
    std::vector<uint32_t> sizeClasses{4096};
    RegionEvictCallback evictCb{
        [](RegionId, uint32_t, BufferView) { return 0; }};
    MockJobScheduler ex;
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, 1024, *device, 1, ex, std::move(evictCb),
        sizeClasses, std::move(policy));

    auto rr = createMemoryRecordReader(ioq);
    rm->recover(*rr);
    auto rid = rm->getFree();
    EXPECT_EQ(3, rid.index());

    EXPECT_EQ(0, rm->getRegion(RegionId{0}).getClassId());
    EXPECT_FALSE(rm->getRegion(RegionId{0}).isPinned());
    EXPECT_EQ(0, rm->getRegion(RegionId{0}).getLastEntryEndOffset());
    EXPECT_EQ(0, rm->getRegion(RegionId{0}).getNumItems());

    // @getClassId has assert on permanent
    EXPECT_TRUE(rm->getRegion(RegionId{1}).isPinned());
    EXPECT_EQ(2020, rm->getRegion(RegionId{1}).getLastEntryEndOffset());
    EXPECT_EQ(20, rm->getRegion(RegionId{1}).getNumItems());

    EXPECT_EQ(1, rm->getRegion(RegionId{2}).getClassId());
    EXPECT_FALSE(rm->getRegion(RegionId{2}).isPinned());
    EXPECT_EQ(3030, rm->getRegion(RegionId{2}).getLastEntryEndOffset());
    EXPECT_EQ(30, rm->getRegion(RegionId{2}).getNumItems());

    // this is a region that was not assigned to anything.
    EXPECT_EQ(Region::kClassIdMax, rm->getRegion(RegionId{3}).getClassId());
    EXPECT_FALSE(rm->getRegion(RegionId{3}).isPinned());
    EXPECT_EQ(0, rm->getRegion(RegionId{3}).getLastEntryEndOffset());
    EXPECT_EQ(0, rm->getRegion(RegionId{3}).getNumItems());

    EXPECT_EQ(1, rm->pinnedCount());
  }
}

TEST(RegionManager, ReadWrite) {
  constexpr uint64_t kBaseOffset = 1024;
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;

  auto device = createMemoryDevice(kBaseOffset + kNumRegions * kRegionSize);
  auto devicePtr = device.get();
  std::vector<uint32_t> sizeClasses{4096};
  RegionEvictCallback evictCb{[](RegionId, uint32_t, BufferView) { return 0; }};
  MockJobScheduler ex;
  auto rm = std::make_unique<RegionManager>(kNumRegions,
                                            kRegionSize,
                                            kBaseOffset,
                                            1024,
                                            *device,
                                            1,
                                            ex,
                                            std::move(evictCb),
                                            sizeClasses,
                                            std::make_unique<LruPolicy>(4));

  constexpr uint32_t kLocalOffset = 3 * 1024;
  constexpr uint32_t kSize = 1024;
  BufferGen bg;
  RelAddress addr{RegionId{1}, kLocalOffset};
  auto buf = bg.gen(kSize);
  EXPECT_TRUE(rm->write(addr, buf.view()));
  Buffer bufRead{kSize};
  EXPECT_TRUE(rm->read(addr, bufRead.mutableView()));
  EXPECT_EQ(buf.view(), bufRead.view());

  // Check device directly at the offset we expect data to be written
  auto expectedOfs = kBaseOffset + kRegionSize + kLocalOffset;
  Buffer bufReadDirect{kSize};
  EXPECT_TRUE(devicePtr->read(expectedOfs, kSize, bufReadDirect.data()));
  EXPECT_EQ(buf.view(), bufReadDirect.view());
}

TEST(RegionManager, RecoveryLRUOrder) {
  constexpr uint32_t kNumRegions = 4;
  constexpr uint32_t kRegionSize = 4 * 1024;
  auto device = createMemoryDevice(kNumRegions * kRegionSize);

  folly::IOBufQueue ioq;
  {
    auto policy = std::make_unique<LruPolicy>(kNumRegions);
    std::vector<uint32_t> sizeClasses{4096};
    RegionEvictCallback evictCb{
        [](RegionId, uint32_t, BufferView) { return 0; }};
    MockJobScheduler ex;
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, 1024, *device, 1, ex, std::move(evictCb),
        sizeClasses, std::move(policy));

    // Get all free regions. Mark 1 and 2 clean (num entries == 0), 0 and 3
    // used. After recovery, LRU should return clean before used, in order
    // of index.
    for (uint32_t i = 0; i < 4; i++) {
      EXPECT_EQ(RegionId{i}, rm->getFree());
    }
    EXPECT_EQ(0, rm->numFree());

    rm->getRegion(RegionId{0}).setClassId(1);
    for (int i = 0; i < 10; i++) {
      rm->getRegion(RegionId{0}).allocate(200);
    }
    rm->getRegion(RegionId{1}).setClassId(0);
    rm->getRegion(RegionId{2}).setClassId(0);
    rm->getRegion(RegionId{3}).setClassId(2);
    for (int i = 0; i < 20; i++) {
      rm->getRegion(RegionId{3}).allocate(150);
    }

    auto rw = createMemoryRecordWriter(ioq);
    rm->persist(*rw);
  }

  {
    auto policy = std::make_unique<LruPolicy>(kNumRegions);
    std::vector<uint32_t> sizeClasses{4096};
    RegionEvictCallback evictCb{
        [](RegionId, uint32_t, BufferView) { return 0; }};
    MockJobScheduler ex;
    auto rm = std::make_unique<RegionManager>(
        kNumRegions, kRegionSize, 0, 1024, *device, 1, ex, std::move(evictCb),
        sizeClasses, std::move(policy));

    auto rr = createMemoryRecordReader(ioq);
    rm->recover(*rr);

    EXPECT_EQ(0, rm->numFree());
    EXPECT_EQ(RegionId{1}, rm->evict());
    EXPECT_EQ(RegionId{2}, rm->evict());
    EXPECT_EQ(RegionId{0}, rm->evict());
    EXPECT_EQ(RegionId{3}, rm->evict());
    EXPECT_EQ(RegionId{}, rm->evict()); // Invalid
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
