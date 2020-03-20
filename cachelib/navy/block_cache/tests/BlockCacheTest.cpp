#include "cachelib/navy/block_cache/BlockCache.h"

#include <future>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/tests/MockPolicy.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/driver/Driver.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/MockJobScheduler.h"
#include "cachelib/navy/testing/SeqPoints.h"

using testing::_;
using testing::Invoke;
using testing::NiceMock;
using testing::Return;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
constexpr uint64_t kDeviceSize{64 * 1024};
constexpr uint64_t kRegionSize{16 * 1024};
constexpr size_t kSizeOfEntryDesc{24};

std::unique_ptr<JobScheduler> makeJobScheduler() {
  return std::make_unique<MockSingleThreadJobScheduler>();
}

// 4x16k regions
BlockCache::Config makeConfig(JobScheduler& scheduler,
                              std::unique_ptr<EvictionPolicy> policy,
                              Device& device,
                              std::vector<uint32_t> sizeClasses) {
  BlockCache::Config config;
  config.scheduler = &scheduler;
  config.blockSize = 1024;
  config.regionSize = kRegionSize;
  config.sizeClasses = std::move(sizeClasses);
  config.cacheSize = kDeviceSize;
  config.device = &device;
  config.evictionPolicy = std::move(policy);
  return config;
}

std::unique_ptr<Engine> makeEngine(BlockCache::Config&& config,
                                   size_t metadataSize = 0) {
  config.cacheBaseOffset = metadataSize;
  return std::make_unique<BlockCache>(std::move(config));
}

std::unique_ptr<Driver> makeDriver(std::unique_ptr<Engine> largeItemCache,
                                   std::unique_ptr<JobScheduler> scheduler,
                                   std::unique_ptr<Device> device = nullptr,
                                   size_t metadataSize = 0) {
  Driver::Config config;
  config.largeItemCache = std::move(largeItemCache);
  config.scheduler = std::move(scheduler);
  config.metadataSize = metadataSize;
  config.device = std::move(device);
  return std::make_unique<Driver>(std::move(config));
}

template <typename F>
void spinWait(F f) {
  while (!f()) {
    std::this_thread::yield();
  }
}

Buffer strzBuffer(const char* strz) { return Buffer{makeView(strz)}; }

class CacheEntry {
 public:
  CacheEntry(Buffer k, Buffer v) : key_{std::move(k)}, value_{std::move(v)} {}
  CacheEntry(CacheEntry&&) = default;
  CacheEntry& operator=(CacheEntry&&) = default;

  BufferView key() const { return key_.view(); }

  BufferView value() const { return value_.view(); }

 private:
  Buffer key_, value_;
};

InsertCallback saveEntryCb(CacheEntry&& e) {
  return [entry = std::move(e)](Status /* status */, BufferView /* key */) {};
}

void finishAllJobs(MockJobScheduler& ex) {
  while (ex.getQueueSize()) {
    ex.runFirst();
  }
}
} // namespace

TEST(BlockCache, InsertLookup) {
  std::vector<CacheEntry> log;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    EXPECT_EQ(e.value(), value.view());
    log.push_back(std::move(e));
    EXPECT_EQ(1, hits[0]);
  }

  // After 15 more we fill region fully. Before adding 16th, block cache adds
  // region to tracked.
  for (size_t i = 0; i < 16; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  for (size_t i = 0; i < 17; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
  EXPECT_EQ(17, hits[0]);
  EXPECT_EQ(1, hits[1]);
  EXPECT_EQ(0, hits[2]);
  EXPECT_EQ(0, hits[3]);
}

TEST(BlockCache, InsertLookupSync) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  for (size_t i = 0; i < 17; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
    // Value is immediately available to query
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    EXPECT_EQ(e.value(), value.view());
  }

  EXPECT_EQ(16, hits[0]);
  EXPECT_EQ(1, hits[1]);
  EXPECT_EQ(0, hits[2]);
  EXPECT_EQ(0, hits[3]);
}

TEST(BlockCache, AsyncCallbacks) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  MockInsertCB cbInsert;
  EXPECT_CALL(cbInsert, call(Status::Ok, makeView("key")));
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(
                makeView("key"), makeView("value"), {}, toCallback(cbInsert)));
  driver->flush();

  MockLookupCB cbLookup;
  EXPECT_CALL(cbLookup, call(Status::Ok, makeView("key"), makeView("value")));
  EXPECT_CALL(cbLookup, call(Status::NotFound, makeView("cat"), BufferView{}));
  EXPECT_EQ(Status::Ok,
            driver->lookupAsync(
                makeView("key"),
                [&cbLookup](Status status, BufferView key, Buffer value) {
                  cbLookup.call(status, key, value.view());
                }));
  EXPECT_EQ(Status::Ok,
            driver->lookupAsync(
                makeView("cat"),
                [&cbLookup](Status status, BufferView key, Buffer value) {
                  cbLookup.call(status, key, value.view());
                }));
  driver->flush();

  MockRemoveCB cbRemove;
  EXPECT_CALL(cbRemove, call(Status::Ok, makeView("key")));
  EXPECT_CALL(cbRemove, call(Status::NotFound, makeView("cat")));
  EXPECT_EQ(Status::Ok,
            driver->removeAsync(makeView("key"), toCallback(cbRemove)));
  EXPECT_EQ(Status::Ok,
            driver->removeAsync(makeView("cat"), toCallback(cbRemove)));
  driver->flush();
}

TEST(BlockCache, Remove) {
  std::vector<CacheEntry> log;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  {
    CacheEntry e{strzBuffer("cat"), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  {
    CacheEntry e{strzBuffer("dog"), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(log[0].key(), value));
  EXPECT_EQ(log[0].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
  EXPECT_EQ(log[1].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->remove(makeView("dog")));
  EXPECT_EQ(Status::NotFound, driver->remove(makeView("fox")));
  EXPECT_EQ(Status::Ok, driver->lookup(log[0].key(), value));
  EXPECT_EQ(log[0].value(), value.view());
  EXPECT_EQ(Status::NotFound, driver->lookup(log[1].key(), value));
  EXPECT_EQ(Status::NotFound, driver->remove(makeView("dog")));
  EXPECT_EQ(Status::NotFound, driver->remove(makeView("fox")));
  EXPECT_EQ(Status::Ok, driver->remove(makeView("cat")));
  EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
  EXPECT_EQ(Status::NotFound, driver->lookup(log[1].key(), value));
}

TEST(BlockCache, CollisionOverwrite) {
  // We can't make up a collision easily. Instead, let's write a key and then
  // modify it on device (memory device in this case).
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  CacheEntry e{strzBuffer("key"), strzBuffer("value")};
  EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), value));
  EXPECT_EQ(makeView("value"), value.view());
  driver->flush();

  uint8_t buf[1024]{};
  ASSERT_TRUE(device->read(0, sizeof(buf), buf));
  constexpr uint32_t kKeySize{3}; // "key"
  size_t keyOffset = sizeof(buf) - kSizeOfEntryDesc - kKeySize;
  BufferView keyOnDevice{kKeySize, buf + keyOffset};
  ASSERT_EQ(keyOnDevice, makeView("key"));

  std::memcpy(buf + keyOffset, "abc", 3);
  ASSERT_TRUE(device->write(0, Buffer{BufferView{sizeof(buf), buf}}));
  // Original key is not found, because it didn't pass key equality check
  EXPECT_EQ(Status::NotFound, driver->lookup(makeView("key"), value));
}

TEST(BlockCache, AllocClasses) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {2048, 3072, 4096});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  {
    CacheEntry e{bg.gen(8), bg.gen(1800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    EXPECT_EQ(e.value(), value.view());
  }
  {
    // Try different size class:
    CacheEntry e{bg.gen(8), bg.gen(2800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    EXPECT_EQ(e.value(), value.view());
  }
}

TEST(BlockCache, SimpleReclaim) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 16; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok,
                driver->insertAsync(e.key(), e.value(), {}, nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // This insert will trigger reclamation because there are 4 regions in total
  // and the device was configured to require 1 clean region at all times
  for (size_t i = 0; i < 16; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // First 16 are reclaimed and so missing
  for (size_t i = 0; i < 16; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 16; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

TEST(BlockCache, HoleStats) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 16; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok,
                driver->insertAsync(e.key(), e.value(), {}, nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(0, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(0, count);
    }
  });

  // Remove 3 entries from region 0
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[1].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  // Remove 2 entries from region 2
  EXPECT_EQ(Status::Ok, driver->remove(log[20].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[21].key()));

  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(5 * 1024, count);
    }
  });

  // Force reclamation on region 0. There are 4 regions and the device
  // was configured to require 1 clean region at all times
  for (size_t i = 0; i < 16; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // Reclaiming region 0 should have bumped down the hole count to
  // 2 remaining (from region 2)
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(2, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(2 * 1024, count);
    }
  });
}

TEST(BlockCache, StackAlloc) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = std::make_unique<MockSingleThreadJobScheduler>();
  auto exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.readBufferSize = 2048;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // Regular read case: read buffer size matches slot size
  CacheEntry e1{bg.gen(8), bg.gen(1800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e1.key(), e1.value(), {}, nullptr));
  exPtr->finish();
  // Buffer is too large (slot is smaller)
  CacheEntry e2{bg.gen(8), bg.gen(100)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e2.key(), e2.value(), {}, nullptr));
  exPtr->finish();
  // Buffer is too small
  CacheEntry e3{bg.gen(8), bg.gen(3000)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e3.key(), e3.value(), {}, nullptr));
  exPtr->finish();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(e1.key(), value));
  EXPECT_EQ(e1.value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(e2.key(), value));
  EXPECT_EQ(e2.value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(e3.key(), value));
  EXPECT_EQ(e3.value(), value.view());

  EXPECT_EQ(0, exPtr->getQueueSize());
}

TEST(BlockCache, RegionUnderflow) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  EXPECT_CALL(*device, writeImpl(0, 1024, _));
  // Although 2k read buffer, shouldn't underflow the region!
  EXPECT_CALL(*device, readImpl(0, 1024, _));
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 0;
  config.readBufferSize = 2048;

  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // 1k entry
  CacheEntry e{bg.gen(8), bg.gen(800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
  driver->flush();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
  EXPECT_EQ(e.value(), value.view());
}

TEST(BlockCache, RegionUnderflowInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  EXPECT_CALL(*device, writeImpl(0, 16 * 1024, _));
  // Although 2k read buffer, shouldn't underflow the region!
  EXPECT_CALL(*device, readImpl(0, 1024, _));
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 4;
  config.readBufferSize = 2048;

  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // 1k entry
  CacheEntry e{bg.gen(8), bg.gen(800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
  driver->flush();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
  EXPECT_EQ(e.value(), value.view());
}

TEST(BlockCache, StackAllocReclaim) {
  std::vector<CacheEntry> log;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // Fill region 0
  { // 2k
    CacheEntry e{bg.gen(8), bg.gen(2000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  { // 10k
    CacheEntry e{bg.gen(8), bg.gen(10'000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();
  // Fill region 1
  { // 8k
    CacheEntry e{bg.gen(8), bg.gen(8000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  { // 6k
    CacheEntry e{bg.gen(8), bg.gen(6000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();
  // Fill region 2
  { // 4k
    CacheEntry e{bg.gen(8), bg.gen(4000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  { // 4k
    CacheEntry e{bg.gen(8), bg.gen(4000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  { // 4k
    CacheEntry e{bg.gen(8), bg.gen(4000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();
  // Fill region 3
  // Triggers reclamation of region 0
  { // 15k
    CacheEntry e{bg.gen(8), bg.gen(15'000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  for (size_t i = 0; i < 2; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 2; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

TEST(BlockCache, ReadRegionDuringEviction) {
  std::vector<CacheEntry> log;
  SeqPoints sp;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  // Region 0 eviction
  EXPECT_CALL(*device, readImpl(0, 16 * 1024, _));
  // Lookup log[2]
  EXPECT_CALL(*device, readImpl(8192, 4096, _)).Times(2);
  EXPECT_CALL(*device, readImpl(4096, 4096, _))
      .WillOnce(Invoke([md = device.get(),
                        &sp](uint64_t offset, uint32_t size, void* buffer) {
        sp.reached(0);
        sp.wait(1);
        return md->getRealDeviceRef().read(offset, size, buffer);
      }));

  auto ex = std::make_unique<MockJobScheduler>();
  auto exPtr = ex.get();
  // Huge size class to fill a region in 4 allocs
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  config.numInMemBuffers = 0;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      CacheEntry e{bg.gen(8), bg.gen(1000)};
      driver->insertAsync(
          e.key(), e.value(), {}, [](Status status, BufferView /*key */) {
            EXPECT_EQ(Status::Ok, status);
          });
      log.push_back(std::move(e));
      finishAllJobs(*exPtr);
    }
  }

  std::thread lookupThread([&driver, &log] {
    Buffer value;
    // Doesn't block, only checks
    EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
    EXPECT_EQ(log[2].value(), value.view());
    // Blocks
    EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
    EXPECT_EQ(log[1].value(), value.view());
  });

  sp.wait(0);

  // Send insert. Will schedule a reclamation job.
  CacheEntry e{bg.gen(8), bg.gen(1000)};
  EXPECT_EQ(0, exPtr->getQueueSize());
  driver->insertAsync(
      e.key(), e.value(), {}, [](Status status, BufferView /*key */) {
        EXPECT_EQ(Status::Ok, status);
      });
  // Insert finds region is full and  puts region for tracking, resets allocator
  // and retries.
  EXPECT_TRUE(exPtr->runFirstIf("insert"));
  EXPECT_TRUE(exPtr->runFirstIf("reclaim"));

  Buffer value;

  EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
  EXPECT_EQ(log[2].value(), value.view());

  // Eviction blocks access
  EXPECT_FALSE(exPtr->runFirstIf("reclaim.evict"));

  std::thread lookupThread2([&driver, &log] {
    Buffer value2;
    // Can't access region 0: blocked. Will retry until unblocked.
    EXPECT_EQ(Status::NotFound, driver->lookup(log[2].key(), value2));
  });

  // To make sure that the reason for key not found is access block, but not
  // evicted from the index, remove it manually and expect it was found.
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  EXPECT_FALSE(exPtr->runFirstIf("reclaim.evict"));

  // Finish read and let evict region 0 entries
  sp.reached(1);

  finishAllJobs(*exPtr);
  EXPECT_EQ(Status::NotFound, driver->remove(log[1].key()));
  EXPECT_EQ(Status::NotFound, driver->remove(log[2].key()));

  lookupThread.join();
  lookupThread2.join();

  driver->flush();
}

TEST(BlockCache, DeviceFailure) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  EXPECT_CALL(*device, readImpl(1024, 1024, _)).WillOnce(Return(false));
  EXPECT_CALL(*device, readImpl(2048, 1024, _));

  EXPECT_CALL(*device, writeImpl(0, 1024, _)).WillOnce(Return(false));
  // In case of IO error, allocator moves forward anyways. Expect allocation
  // in the next slot.
  EXPECT_CALL(*device, writeImpl(1024, 1024, _));
  EXPECT_CALL(*device, writeImpl(2048, 1024, _));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  config.numInMemBuffers = 0;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  auto value1 = bg.gen(800);
  auto value2 = bg.gen(800);
  auto value3 = bg.gen(800);

  // Test setup:
  //   - "key1" write fails, read succeeds
  //   - "key2" write succeeds, read fails
  //   - "key3" both read and write succeeds

  EXPECT_EQ(Status::DeviceError,
            driver->insert(makeView("key1"), value1.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key2"), value2.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key3"), value3.view(), {}));

  Buffer value;
  EXPECT_EQ(Status::NotFound, driver->lookup(makeView("key1"), value));
  EXPECT_TRUE(value.isNull());
  EXPECT_EQ(Status::DeviceError, driver->lookup(makeView("key2"), value));
  EXPECT_TRUE(value.isNull());
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key3"), value));
  EXPECT_EQ(value3.view(), value.view());
}

TEST(BlockCache, Flush) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  EXPECT_CALL(*device, flushImpl());

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // 1k entry
  CacheEntry e{bg.gen(8), bg.gen(800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
  driver->flush();
}

namespace {
std::unique_ptr<Device> setupResetTestDevice(uint32_t size) {
  auto device = std::make_unique<NiceMock<MockDevice>>(size, 512);
  for (uint32_t i = 0; i < 17; i++) {
    EXPECT_CALL(*device, writeImpl(i * 1024, 1024, _));
  }
  return device;
}

std::unique_ptr<Device> setupResetTestDeviceInMemBuffers(uint32_t size) {
  auto device = std::make_unique<NiceMock<MockDevice>>(size, 512);
  for (uint32_t i = 0; i < 2; i++) {
    EXPECT_CALL(*device, writeImpl(i * 16 * 1024, 16 * 1024, _));
  }
  return device;
}

void resetTestRun(Driver& cache) {
  std::vector<CacheEntry> log;
  BufferGen bg;
  // Enough to get reclamation
  for (size_t i = 0; i < 17; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, cache.insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  cache.flush();
  for (size_t i = 0; i < 17; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, cache.lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}
} // namespace

TEST(BlockCache, Reset) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*policy, track(RegionId{0}));
    EXPECT_CALL(*policy, reset());
    EXPECT_CALL(*policy, track(RegionId{0}));
  }

  auto proxy = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto proxyPtr = proxy.get();
  // Setup delegating device before creating, because makeIOBuffer is called
  // during construction.
  proxyPtr->setRealDevice(setupResetTestDevice(kDeviceSize));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *proxyPtr, {1024});
  config.numInMemBuffers = 0;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  resetTestRun(*driver);

  driver->reset();

  // Create a new device with same expectations
  proxyPtr->setRealDevice(setupResetTestDevice(config.cacheSize));
  resetTestRun(*driver);
}

TEST(BlockCache, ResetInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*policy, track(RegionId{0}));
    EXPECT_CALL(*policy, track(RegionId{1}));
    EXPECT_CALL(*policy, reset());
    EXPECT_CALL(*policy, track(RegionId{0}));
    EXPECT_CALL(*policy, track(RegionId{1}));
  }

  auto proxy = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto proxyPtr = proxy.get();
  // Setup delegating device before creating, because makeIOBuffer is called
  // during construction.
  proxyPtr->setRealDevice(setupResetTestDeviceInMemBuffers(kDeviceSize));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *proxyPtr, {1024});
  config.numInMemBuffers = 3;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));
  driver->flush();

  resetTestRun(*driver);

  driver->reset();

  // Create a new device with same expectations
  proxyPtr->setRealDevice(setupResetTestDeviceInMemBuffers(config.cacheSize));
  driver->flush();
  resetTestRun(*driver);
}

TEST(BlockCache, DestructorCallback) {
  std::vector<CacheEntry> log;
  {
    BufferGen bg;
    // First two regions filled with authentic keys and values
    for (size_t i = 0; i < 8; i++) {
      log.emplace_back(bg.gen(8), bg.gen(800));
    }
    // Region 3 will get overwrites of region 0 entries (0 and 1) and region 1
    // entries (5 and 6).
    log.emplace_back(Buffer{log[0].key()}, bg.gen(800));
    log.emplace_back(Buffer{log[1].key()}, bg.gen(800));
    log.emplace_back(Buffer{log[5].key()}, bg.gen(800));
    log.emplace_back(Buffer{log[6].key()}, bg.gen(800));

    // One more to kick off reclamation
    log.emplace_back(bg.gen(8), bg.gen(800));
    ASSERT_EQ(13, log.size());
  }

  MockDestructor cb;
  {
    testing::InSequence inSeq;
    // Region evictions is backwards to the order of insertion.
    EXPECT_CALL(cb,
                call(log[7].key(), log[7].value(), DestructorEvent::Recycled));
    EXPECT_CALL(cb,
                call(log[6].key(), log[6].value(), DestructorEvent::Removed));
    EXPECT_CALL(cb,
                call(log[5].key(), log[5].value(), DestructorEvent::Removed));
    EXPECT_CALL(cb,
                call(log[4].key(), log[4].value(), DestructorEvent::Removed));
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{1}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  for (size_t i = 0; i < 12; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value(), {}));
  }
  // Remove before it gets evicted
  EXPECT_EQ(Status::Ok, driver->remove(log[4].key()));
  // Kick off eviction
  EXPECT_EQ(Status::Ok, driver->insert(log[12].key(), log[12].value(), {}));

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(log[0].key(), value));
  EXPECT_EQ(log[8].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[8].key(), value));
  EXPECT_EQ(log[8].value(), value.view());

  EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
  EXPECT_EQ(log[9].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[9].key(), value));
  EXPECT_EQ(log[9].value(), value.view());

  EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
  EXPECT_EQ(log[2].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[3].key(), value));
  EXPECT_EQ(log[3].value(), value.view());

  EXPECT_EQ(Status::NotFound, driver->lookup(log[4].key(), value));
  EXPECT_EQ(Status::Ok, driver->lookup(log[5].key(), value));
  EXPECT_EQ(log[10].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[6].key(), value));
  EXPECT_EQ(log[11].value(), value.view());
  EXPECT_EQ(Status::NotFound, driver->lookup(log[7].key(), value));

  EXPECT_EQ(Status::Ok, driver->lookup(log[12].key(), value));
  EXPECT_EQ(log[12].value(), value.view());
}

TEST(BlockCache, StackAllocDestructorCallback) {
  std::vector<CacheEntry> log;
  {
    BufferGen bg;
    // 1st region, 12k
    log.emplace_back(bg.gen(8), bg.gen(5'000));
    log.emplace_back(bg.gen(8), bg.gen(7'000));
    // 2nd region, 14k
    log.emplace_back(bg.gen(8), bg.gen(5'000));
    log.emplace_back(bg.gen(8), bg.gen(3'000));
    log.emplace_back(bg.gen(8), bg.gen(6'000));
    // 3rd region, 16k, overwrites
    log.emplace_back(Buffer{log[0].key()}, bg.gen(8'000));
    log.emplace_back(Buffer{log[3].key()}, bg.gen(8'000));
    // 4th region, 15k
    log.emplace_back(bg.gen(8), bg.gen(9'000));
    log.emplace_back(bg.gen(8), bg.gen(6'000));
    ASSERT_EQ(9, log.size());
  }

  MockDestructor cb;
  {
    testing::InSequence inSeq;
    // Region evictions is backwards to the order of insertion.
    EXPECT_CALL(cb,
                call(log[4].key(), log[4].value(), DestructorEvent::Recycled));
    EXPECT_CALL(cb,
                call(log[3].key(), log[3].value(), DestructorEvent::Removed));
    EXPECT_CALL(cb,
                call(log[2].key(), log[2].value(), DestructorEvent::Removed));
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{1}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  for (size_t i = 0; i < 7; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value(), {}));
  }
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));
  EXPECT_EQ(Status::Ok, driver->insert(log[7].key(), log[7].value(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(log[8].key(), log[8].value(), {}));

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(log[0].key(), value));
  EXPECT_EQ(log[5].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[5].key(), value));
  EXPECT_EQ(log[5].value(), value.view());

  EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
  EXPECT_EQ(log[1].value(), value.view());

  EXPECT_EQ(Status::NotFound, driver->lookup(log[2].key(), value));
  EXPECT_EQ(Status::Ok, driver->lookup(log[3].key(), value));
  EXPECT_EQ(log[6].value(), value.view());
  EXPECT_EQ(Status::NotFound, driver->lookup(log[4].key(), value));

  EXPECT_EQ(Status::Ok, driver->lookup(log[7].key(), value));
  EXPECT_EQ(log[7].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[8].key(), value));
  EXPECT_EQ(log[8].value(), value.view());
}

TEST(BlockCache, StackAllocDestructorCallbackInMemBuffers) {
  std::vector<CacheEntry> log;
  {
    BufferGen bg;
    // 1st region, 12k
    log.emplace_back(bg.gen(8), bg.gen(5'000));
    log.emplace_back(bg.gen(8), bg.gen(7'000));
    // 2nd region, 14k
    log.emplace_back(bg.gen(8), bg.gen(5'000));
    log.emplace_back(bg.gen(8), bg.gen(3'000));
    log.emplace_back(bg.gen(8), bg.gen(6'000));
    // 3rd region, 16k, overwrites
    log.emplace_back(Buffer{log[0].key()}, bg.gen(8'000));
    log.emplace_back(Buffer{log[3].key()}, bg.gen(8'000));
    // 4th region, 15k
    log.emplace_back(bg.gen(8), bg.gen(9'000));
    log.emplace_back(bg.gen(8), bg.gen(6'000));
    ASSERT_EQ(9, log.size());
  }

  MockDestructor cb;
  {
    testing::InSequence inSeq;
    // Region evictions is backwards to the order of insertion.
    EXPECT_CALL(cb,
                call(log[4].key(), log[4].value(), DestructorEvent::Recycled));
    EXPECT_CALL(cb,
                call(log[3].key(), log[3].value(), DestructorEvent::Removed));
    EXPECT_CALL(cb,
                call(log[2].key(), log[2].value(), DestructorEvent::Removed));
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{1}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 9;
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  for (size_t i = 0; i < 7; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value(), {}));
  }
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));
  EXPECT_EQ(Status::Ok, driver->insert(log[7].key(), log[7].value(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(log[8].key(), log[8].value(), {}));

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(log[0].key(), value));
  EXPECT_EQ(log[5].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[5].key(), value));
  EXPECT_EQ(log[5].value(), value.view());

  EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
  EXPECT_EQ(log[1].value(), value.view());

  EXPECT_EQ(Status::NotFound, driver->lookup(log[2].key(), value));
  EXPECT_EQ(Status::Ok, driver->lookup(log[3].key(), value));
  EXPECT_EQ(log[6].value(), value.view());
  EXPECT_EQ(Status::NotFound, driver->lookup(log[4].key(), value));

  EXPECT_EQ(Status::Ok, driver->lookup(log[7].key(), value));
  EXPECT_EQ(log[7].value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(log[8].key(), value));
  EXPECT_EQ(log[8].value(), value.view());
}

TEST(BlockCache, RegionLastOffset) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {2048});
  config.regionSize = 15 * 1024; // so regionSize is not multiple of sizeClass
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 7 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 7; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok,
                driver->insertAsync(e.key(), e.value(), {}, nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Triggers reclamation
  for (size_t i = 0; i < 7; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // First 7 are reclaimed and so are missing
  for (size_t i = 0; i < 7; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 7; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

TEST(BlockCache, RegionLastOffsetOnReset) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));
  EXPECT_CALL(*policy, reset());

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {2048});
  config.regionSize = 15 * 1024; // so regionSize is not multiple of sizeClass
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 7 inserts.
  BufferGen bg;
  for (size_t i = 0; i < 2; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    auto key = e.key();
    auto value = e.value();
    EXPECT_EQ(Status::Ok,
              driver->insertAsync(key, value, {}, saveEntryCb(std::move(e))));
  }
  driver->flush();
  driver->reset();

  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 7; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok,
                driver->insertAsync(e.key(), e.value(), {}, nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Triggers reclamation
  for (size_t i = 0; i < 7; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // First 7 (from after reset) are reclaimed and so are missing
  for (size_t i = 0; i < 7; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 7; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
  driver->flush();
}

TEST(BlockCache, Recovery) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*policy, track(RegionId{0}));
    EXPECT_CALL(*policy, track(RegionId{1}));
    EXPECT_CALL(*policy, reset());
    EXPECT_CALL(*policy, track(RegionId{0}));
    EXPECT_CALL(*policy, track(RegionId{1}));
    EXPECT_CALL(*policy, track(RegionId{2}));
    // reclamation job (RJ) scheduled in Allocator::init finds a free region.
    // Allocator picks it up for inserts and immediately schedules new RJ
    // becasue clean pool is empty. RJ scheduled first, before all inserts in
    // the queue. It evicts region 0 and puts into clean queue. Pending inserts
    // executed and go into region 0.
    EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));
  }

  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096, 8192});
  config.numInMemBuffers = 0;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(
      std::move(engine), std::move(ex), std::move(device), metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
      log.push_back(std::move(e));
    }
  }

  driver->persist();

  EXPECT_TRUE(driver->recover());

  for (auto& entry : log) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(entry.key(), value));
    EXPECT_EQ(entry.value(), value.view());
  }

  // This insertion should evict region 0
  for (size_t i = 0; i < 3; i++) {
    CacheEntry e{bg.gen(8), bg.gen(3200)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
    log.push_back(std::move(e));
  }

  driver->flush();
  for (size_t i = 0; i < 4; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 4; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

TEST(BlockCache, HoleStatsRecovery) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096, 8192});
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(
      std::move(engine), std::move(ex), std::move(device), metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;

  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
      log.push_back(std::move(e));
    }
  }

  // Remove some entries
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[1].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[5].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[8].key()));

  CounterVisitor validationFunctor = [](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(4, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(4 * 4096, count);
    }
  };

  driver->getCounters(validationFunctor);
  driver->persist();
  driver->reset();
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(0, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(0, count);
    }
  });
  EXPECT_TRUE(driver->recover());
  driver->getCounters(validationFunctor);
}

TEST(BlockCache, RecoveryBadConfig) {
  folly::IOBufQueue ioq;
  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    {
      testing::InSequence inSeq;
      EXPECT_CALL(*policy, track(RegionId{0}));
      EXPECT_CALL(*policy, track(RegionId{1}));
    }

    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    auto engine = makeEngine(std::move(config), metadataSize);
    auto driver = makeDriver(
        std::move(engine), std::move(ex), std::move(device), metadataSize);

    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t i = 0; i < 3; i++) {
      for (size_t j = 0; j < 4; j++) {
        CacheEntry e{bg.gen(8), bg.gen(3200)};
        EXPECT_EQ(Status::Ok,
                  driver->insertAsync(e.key(), e.value(), {}, nullptr));
        log.push_back(std::move(e));
      }
    }
    driver->flush();

    driver->persist();
  }

  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    {
      testing::InSequence inSeq;
      EXPECT_CALL(*policy, reset()).Times(0);
    }

    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    config.regionSize = 8 * 1024; // Region size differs from original
    auto engine = makeEngine(std::move(config), metadataSize);
    auto driver = makeDriver(
        std::move(engine), std::move(ex), std::move(device), metadataSize);

    EXPECT_FALSE(driver->recover());
  }
  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    {
      testing::InSequence inSeq;
      EXPECT_CALL(*policy, reset()).Times(0);
    }

    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    config.checksum = true;
    auto engine = makeEngine(std::move(config));
    auto driver = makeDriver(
        std::move(engine), std::move(ex), std::move(device), metadataSize);

    EXPECT_FALSE(driver->recover());
  }
}

TEST(BlockCache, RecoveryCorruptedData) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*policy, reset()).Times(0);
  }

  std::unique_ptr<Driver> driver;
  {
    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    auto engine = makeEngine(std::move(config), metadataSize);
    auto rw = createMetadataRecordWriter(*device, metadataSize);
    driver = makeDriver(
        std::move(engine), std::move(ex), std::move(device), metadataSize);

    // persist metadata
    driver->persist();

    // corrupt the data
    auto ioBuf = folly::IOBuf::createCombined(512);
    std::generate(
        ioBuf->writableData(), ioBuf->writableTail(), std::minstd_rand());

    rw->writeRecord(std::move(ioBuf));
  }

  // Expect recovery to fail.
  EXPECT_FALSE(driver->recover());
}

TEST(BlockCache, PermanentItem) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  MockDestructor cb;
  // Expectations set below

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Use region 0
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t i = 0; i < 2; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
    log.push_back(std::move(e));
  }

  // Use region 1 for permanent items
  for (size_t i = 0; i < 2; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(
        Status::Ok,
        driver->insert(e.key(), e.value(), InsertOptions().setPermanent()));
    log.push_back(std::move(e));
  }

  // Two more into region 0 to fill it up and last will use region 2
  for (size_t i = 0; i < 3; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
    log.push_back(std::move(e));
  }

  // Evicted items from region 0
  EXPECT_CALL(cb,
              call(log[0].key(), log[0].value(), DestructorEvent::Recycled));
  EXPECT_CALL(cb,
              call(log[1].key(), log[1].value(), DestructorEvent::Recycled));
  EXPECT_CALL(cb,
              call(log[4].key(), log[4].value(), DestructorEvent::Recycled));
  EXPECT_CALL(cb,
              call(log[5].key(), log[5].value(), DestructorEvent::Recycled));

  // Check stack properties of permanent item allocator. So far, we allocated
  // 2k (block size is 1k). 12k item triggers reclamation.
  for (size_t sz : {2 * 1024, 4 * 1024, 8 * 1024, 12 * 1024}) {
    CacheEntry e{bg.gen(8), bg.gen(sz - 32)};
    EXPECT_EQ(
        Status::Ok,
        driver->insert(e.key(), e.value(), InsertOptions().setPermanent()));
    log.push_back(std::move(e));
  }
  driver->flush();

  // Regions state:
  //   Region 0 evicted
  //   Region 1 sealed, permanent
  //   Region 2 allocating
  //   Region 3 allocating permanent

  EXPECT_EQ(11, log.size());
  Buffer value;
  for (size_t i = 0; i < log.size(); i++) {
    if (i == 0 || i == 1 || i == 4 || i == 5) {
      EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
    } else {
      EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
      EXPECT_EQ(log[i].value(), value.view());
    }
  }

  // Permanent item can be removed (they become invisible) and updated
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("key"),
                           makeView("value1"),
                           InsertOptions().setPermanent()));
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), value));
  EXPECT_EQ(makeView("value1"), value.view());

  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("key"),
                           makeView("value2"),
                           InsertOptions().setPermanent()));
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), value));
  EXPECT_EQ(makeView("value2"), value.view());

  EXPECT_EQ(Status::Ok, driver->remove(makeView("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeView("key"), value));
}

TEST(BlockCache, NoJobsOnStartup) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = std::make_unique<MockJobScheduler>();
  auto exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Should not schedule any jobs on creation. This is for TAO: callbacks might
  // be not ready.
  EXPECT_EQ(0, exPtr->getQueueSize());
}

TEST(BlockCache, Checksum) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = std::make_unique<MockSingleThreadJobScheduler>();
  auto exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.readBufferSize = 2048;
  config.checksum = true;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // Regular read case: read buffer size matches slot size
  // Located at offset 0
  CacheEntry e1{bg.gen(8), bg.gen(1800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e1.key(), e1.value(), {}, nullptr));
  exPtr->finish();
  // Buffer is too large (slot is smaller)
  // Located at offset 2 * 1024
  CacheEntry e2{bg.gen(8), bg.gen(100)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e2.key(), e2.value(), {}, nullptr));
  exPtr->finish();
  // Buffer is too small
  // Located at offset 3 * 1024
  CacheEntry e3{bg.gen(8), bg.gen(3000)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e3.key(), e3.value(), {}, nullptr));
  exPtr->finish();

  // Check everything is fine with checksumming before we corrupt data
  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(e1.key(), value));
  EXPECT_EQ(e1.value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(e2.key(), value));
  EXPECT_EQ(e2.value(), value.view());
  EXPECT_EQ(Status::Ok, driver->lookup(e3.key(), value));
  EXPECT_EQ(e3.value(), value.view());
  driver->flush();

  // Corrupt e1: header
  const char corruption[5]{"hack"};
  EXPECT_TRUE(device->write(
      2 * 1024 - 8,
      Buffer{BufferView{4, reinterpret_cast<const uint8_t*>(corruption)}}));
  EXPECT_EQ(Status::DeviceError, driver->lookup(e1.key(), value));

  // Corrupt e2: key, reported as "key not found"
  EXPECT_TRUE(device->write(
      3 * 1024 - kSizeOfEntryDesc - 4,
      Buffer{BufferView{4, reinterpret_cast<const uint8_t*>(corruption)}}));
  EXPECT_EQ(Status::NotFound, driver->lookup(e2.key(), value));

  // Corrupt e3: value
  EXPECT_TRUE(device->write(
      3 * 1024,
      Buffer{BufferView{4, reinterpret_cast<const uint8_t*>(corruption)}}));
  EXPECT_EQ(Status::DeviceError, driver->lookup(e3.key(), value));

  EXPECT_EQ(0, exPtr->getQueueSize());
}

TEST(BlockCache, HitsReinsertionPolicy) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  EXPECT_CALL(*policy, track(RegionId{0}));
  EXPECT_CALL(*policy, track(RegionId{1}));
  EXPECT_CALL(*policy, track(RegionId{2}));
  EXPECT_CALL(*policy, evict()).WillOnce(Return(RegionId{0}));

  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  config.reinsertionPolicy = std::make_unique<HitsReinsertionPolicy>(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok,
                driver->insertAsync(e.key(), e.value(), {}, nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Access the first three keys
  for (size_t i = 0; i < 3; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }

  // Delete the first key
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));

  // This insert will trigger reclamation because there are 4 regions in total
  // and the device was configured to require 1 clean region at all times
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}, nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // First key was deleted so missing
  {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
  }

  // Second and third items are reinserted so lookup should succeed
  for (size_t i = 1; i < 3; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }

  // Last item was never accessed so it was reclaimed and so missing
  for (size_t i = 3; i < 4; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }

  // Remaining items are still in cache so lookup should succeed
  for (size_t i = 4; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

TEST(BlockCache, HitsReinsertionPolicyRecovery) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096, 8192});
  config.reinsertionPolicy = std::make_unique<HitsReinsertionPolicy>(1);
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(
      std::move(engine), std::move(ex), std::move(device), metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value(), {}));
      log.push_back(std::move(e));
    }
  }

  driver->persist();
  driver->reset();
  EXPECT_TRUE(driver->recover());

  for (auto& entry : log) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(entry.key(), value));
    EXPECT_EQ(entry.value(), value.view());
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
