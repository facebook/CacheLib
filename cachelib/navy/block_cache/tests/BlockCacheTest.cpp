/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/File.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <future>
#include <vector>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/block_cache/BlockCache.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"
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
constexpr uint16_t kFlushRetryLimit{5};

std::unique_ptr<JobScheduler> makeJobScheduler() {
  return std::make_unique<MockSingleThreadJobScheduler>();
}

// 4x16k regions
BlockCache::Config makeConfig(JobScheduler& scheduler,
                              std::unique_ptr<EvictionPolicy> policy,
                              Device& device,
                              std::vector<uint32_t> sizeClasses,
                              uint64_t cacheSize = kDeviceSize) {
  BlockCache::Config config;
  config.scheduler = &scheduler;
  config.regionSize = kRegionSize;
  config.sizeClasses = std::move(sizeClasses);
  config.cacheSize = cacheSize;
  config.device = &device;
  config.evictionPolicy = std::move(policy);
  return config;
}

BlockCacheReinsertionConfig makeHitsReinsertionConfig(
    uint8_t hitsReinsertThreshold) {
  BlockCacheReinsertionConfig config{};
  config.enableHitsBased(hitsReinsertThreshold);
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
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    driver->getCounters([](folly::StringPiece name, double count) {
      if (name == "navy_bc_lookups") {
        EXPECT_EQ(1, count);
      }
    });

    EXPECT_EQ(e.value(), value.view());
    log.push_back(std::move(e));
    EXPECT_EQ(1, hits[0]);
  }

  // After 15 more we fill region fully. Before adding 16th, block cache adds
  // region to tracked.
  for (size_t i = 0; i < 16; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  for (size_t i = 0; i < 17; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
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

// assuming no collision of hash keys, we should have couldExist reflect the
// insertion or deletion of keys.
TEST(BlockCache, CouldExist) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  for (size_t i = 0; i < 17; i++) {
    CacheEntry e{bg.gen(8 + i), bg.gen(800)};
    EXPECT_FALSE(driver->couldExist(e.key()));
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
    EXPECT_TRUE(driver->couldExist(e.key()));
    EXPECT_EQ(Status::Ok, driver->remove(e.key()));
    EXPECT_FALSE(driver->couldExist(e.key()));
  }
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
            driver->insertAsync(makeView("key"), makeView("value"),
                                toCallback(cbInsert)));
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
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  {
    CacheEntry e{strzBuffer("dog"), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));

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
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    EXPECT_EQ(e.value(), value.view());
  }
  {
    // Try different size class:
    CacheEntry e{bg.gen(8), bg.gen(2800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
    EXPECT_EQ(e.value(), value.view());
  }
}

TEST(BlockCache, SmallAllocClass) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {512});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t i = 0; i < 32; i++) {
    CacheEntry e{bg.gen(8), bg.gen(300)};
    // Insert synchronously to ensure ordering is respected
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
    log.push_back(std::move(e));
  }

  // Verify reading the first item is correct
  {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log.begin()->key(), value));
    EXPECT_EQ(log.begin()->value(), value.view());
  }

  // Verify reading the last item is correct
  {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log.rbegin()->key(), value));
    EXPECT_EQ(log.rbegin()->value(), value.view());
  }
}

TEST(BlockCache, UnalignedAllocClass) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {700});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t i = 0; i < 20; i++) {
    CacheEntry e{bg.gen(8), bg.gen(300)};
    // Insert synchronously to ensure ordering is respected
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
    log.push_back(std::move(e));
  }

  // Verify reading the first item is correct
  {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log.begin()->key(), value));
    EXPECT_EQ(log.begin()->value(), value.view());
  }

  // Verify reading the last item is correct
  {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log.rbegin()->key(), value));
    EXPECT_EQ(log.rbegin()->value(), value.view());
  }
}

TEST(BlockCache, SimpleReclaim) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
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
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // This insert will trigger reclamation because there are 4 regions in total
  // and the device was configured to require 1 clean region at all times
  for (size_t i = 0; i < 16; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {1024});
  // items which are accessed once will be reinserted on reclaim
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 16; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  EXPECT_EQ(Status::Ok, driver->remove(log[33].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[34].key()));

  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(5 * 1024, count);
    }
  });

  // lookup this entry from region 0 that will be soon reclaimed
  Buffer val;
  EXPECT_EQ(Status::Ok, driver->lookup(log[4].key(), val));
  EXPECT_EQ(Status::Ok, driver->lookup(log[4].key(), val));

  // Force reclamation on region 0. There are 4 regions and the device
  // was configured to require 1 clean region at all times
  for (size_t i = 0; i < 16; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // Reclaiming region 0 should have bumped down the hole count to
  // 2 remaining (from region 2)
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_reinsertions") {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(2, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(2 * 1024, count);
    }
  });
}

TEST(BlockCache, ReclaimCorruption) {
  // This test verifies two behaviors in BlockCache regarding corruption during
  // reclaim. In the case of an item's entry header corruption, we must abort
  // the reclaim as we don't have a way to ensure we will safely proceed to read
  // the next entry. In the case of an item's value corruption, we can bump the
  // error stat and proceed to the next item.
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<MockDevice>(kDeviceSize, 1 /* ioAlignment */,
                                             nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device,
                           {} /* specify stack allocation */);
  config.checksum = true;
  config.numInMemBuffers = 1;
  // items which are accessed once will be reinserted on reclaim
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allow any number of writes in between and after our expected writes
  EXPECT_CALL(*device, writeImpl(_, _, _)).Times(testing::AtLeast(0));

  // Note even tho this item's value is corrupted, we would have aborted
  // the reclaim before we got here. So we will not bump the value checksum
  // error stat on this.
  EXPECT_CALL(*device, writeImpl(0, 16384, _))
      .WillOnce(testing::Invoke(
          [&device](uint64_t offset, uint32_t size, const void* data) {
            // Note that all items are aligned to 512 bytes in in-mem buffer
            // stacked mode, and we write around 800 bytes, so each is aligned
            // to 1024 bytes
            Buffer buffer = device->getRealDeviceRef().makeIOBuffer(size);
            std::memcpy(buffer.data(), reinterpret_cast<const uint8_t*>(data),
                        size);
            // Mutate a byte in the beginning to corrupt 3rd item's value
            buffer.data()[1024 * 2 + 300] += 1;
            // Mutate a byte in the end to corrupt 5th item's header
            buffer.data()[1024 * 4 + 1010] += 1;
            // Mutate a byte in the beginning to corrupt 7th item's value
            buffer.data()[1024 * 6 + 300] += 1;
            // Mutate a byte in the beginning to corrupt 9th item's value
            buffer.data()[1024 * 8 + 300] += 1;
            return device->getRealDeviceRef().write(offset, std::move(buffer));
          }));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 16; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }
  // Verify we have one header checksum error and two value checksum errors
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_reclaim") {
      EXPECT_EQ(4, count);
    }
  });

  // Force reclamation on region 0 by allocating region 3. There are 4 regions
  // and the device was configured to require 1 clean region at all times
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // Verify we have one header checksum error and two value checksum errors
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_reclaim") {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_reclaim_entry_header_checksum_errors") {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_reclaim_value_checksum_errors") {
      EXPECT_EQ(2, count);
    }
  });
}

TEST(BlockCache, ReclaimCorruptionSizeClass) {
  // This test verifies two behaviors in BlockCache regarding corruption during
  // reclaim. In the case of an item's entry header corruption, we must abort
  // the reclaim as we don't have a way to ensure we will safely proceed to read
  // the next entry. In the case of an item's value corruption, we can bump the
  // error stat and proceed to the next item.
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<MockDevice>(kDeviceSize, 1 /* ioAlignment */,
                                             nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device,
                           {1024} /* specify size class */);
  config.checksum = true;
  // items which are accessed once will be reinserted on reclaim
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allow any number of writes in between and after our expected writes
  EXPECT_CALL(*device, writeImpl(_, _, _)).Times(testing::AtLeast(0));

  EXPECT_CALL(*device, writeImpl(0, _, _))
      .WillOnce(testing::Invoke([&device](uint64_t offset, uint32_t size,
                                          const void* data) {
        Buffer buffer = device->getRealDeviceRef().makeIOBuffer(size);
        // Skip 500 bytes at the beginning to ensure the value is corrupted
        std::memcpy(buffer.data() + 500,
                    reinterpret_cast<const uint8_t*>(data) + 500, size - 500);
        return device->getRealDeviceRef().write(offset, std::move(buffer));
      }));
  EXPECT_CALL(*device, writeImpl(2048, _, _))
      .WillOnce(testing::Invoke(
          [&device](uint64_t offset, uint32_t size, const void* data) {
            Buffer buffer = device->getRealDeviceRef().makeIOBuffer(size);
            // Skip 10 bytes at the end to ensure the header is corrupted
            std::memcpy(buffer.data(), data, size - 10);
            return device->getRealDeviceRef().write(offset, std::move(buffer));
          }));
  EXPECT_CALL(*device, writeImpl(4096, _, _))
      .WillOnce(testing::Invoke([&device](uint64_t offset, uint32_t size,
                                          const void* data) {
        Buffer buffer = device->getRealDeviceRef().makeIOBuffer(size);
        // Skip 500 bytes at the beginning to ensure the value is corrupted
        std::memcpy(buffer.data() + 500,
                    reinterpret_cast<const uint8_t*>(data) + 500, size - 500);
        return device->getRealDeviceRef().write(offset, std::move(buffer));
      }));
  EXPECT_CALL(*device, writeImpl(8192, _, _))
      .WillOnce(testing::Invoke([&device](uint64_t offset, uint32_t size,
                                          const void* data) {
        Buffer buffer = device->getRealDeviceRef().makeIOBuffer(size);
        // Skip 500 bytes at the beginning to ensure the value is corrupted
        std::memcpy(buffer.data() + 500,
                    reinterpret_cast<const uint8_t*>(data) + 500, size - 500);
        return device->getRealDeviceRef().write(offset, std::move(buffer));
      }));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 16; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }
  // Verify we have one header checksum error and two value checksum errors
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_reclaim") {
      EXPECT_EQ(4, count);
    }
  });

  // Force reclamation on region 0 by allocating region 3. There are 4 regions
  // and the device was configured to require 1 clean region at all times
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // Verify we have one header checksum error and two value checksum errors
  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_reclaim") {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_reclaim_entry_header_checksum_errors") {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_reclaim_value_checksum_errors") {
      EXPECT_EQ(3, count);
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
  EXPECT_EQ(Status::Ok, driver->insertAsync(e1.key(), e1.value(), nullptr));
  exPtr->finish();
  // Buffer is too large (slot is smaller)
  CacheEntry e2{bg.gen(8), bg.gen(100)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e2.key(), e2.value(), nullptr));
  exPtr->finish();
  // Buffer is too small
  CacheEntry e3{bg.gen(8), bg.gen(3000)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e3.key(), e3.value(), nullptr));
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
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
  driver->flush();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
  EXPECT_EQ(e.value(), value.view());
}

TEST(BlockCache, SmallReadBuffer) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(
      kDeviceSize, 4096 /* io alignment size */);
  EXPECT_CALL(*device, writeImpl(0, 16 * 1024, _));
  EXPECT_CALL(*device, readImpl(0, 8192, _));
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 4;
  // Small read buffer. We will automatically align to 8192 when we read.
  // This is no longer useful with index saving the object sizes.
  // Remove after we deprecate read buffer from Navy
  config.readBufferSize = 5120; // 5KB

  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  CacheEntry e{bg.gen(8), bg.gen(5800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
  driver->flush();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));
  EXPECT_EQ(e.value(), value.view());
}

// This test enables in memory buffers and inserts items of size 208. With
// Alloc alignment of 512 and device size of 64K, we should be able to store
// 96 items since we have to keep one region free at all times. Read them back
// and make sure they are same as what were inserted.
TEST(BlockCache, SmallAllocAlignment) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 4;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  std::vector<CacheEntry> log;
  BufferGen bg;
  Status status;
  for (size_t i = 0; i < 96; i++) {
    CacheEntry e{bg.gen(8), bg.gen(200)};
    status = driver->insert(e.key(), e.value());
    EXPECT_EQ(Status::Ok, status);
    log.push_back(std::move(e));
  }
  for (size_t i = 0; i < 96; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
  return;

  // One more allocation should trigger reclaim
  {
    CacheEntry e{bg.gen(8), bg.gen(10)};
    status = driver->insert(e.key(), e.value());
    EXPECT_EQ(Status::Ok, status);
    log.push_back(std::move(e));
  }
  driver->flush();

  // Verify the first 32 items are now reclaimed and the others are still there
  for (size_t i = 0; i < 32; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 32; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

// This test enables in memory buffers and inserts items of size 1708. Each
// item spans multiple alloc aligned size of 512 bytes. With
// Alloc alignment of 512 and device size of 64K, we should be able to store
// 24 items with one region always evicted to be free.
// Read them back and make sure they are same as what were inserted.
TEST(BlockCache, MultipleAllocAlignmentSizeItems) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 4;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  std::vector<CacheEntry> log;
  BufferGen bg;
  Status status;
  for (size_t i = 0; i < 24; i++) {
    CacheEntry e{bg.gen(8), bg.gen(1700)};
    status = driver->insert(e.key(), e.value());
    EXPECT_EQ(Status::Ok, status);
    log.push_back(std::move(e));
  }
  for (size_t i = 0; i < 24; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
  return;

  // One more allocation should trigger reclaim
  {
    CacheEntry e{bg.gen(8), bg.gen(10)};
    status = driver->insert(e.key(), e.value());
    EXPECT_EQ(Status::Ok, status);
    log.push_back(std::move(e));
  }
  driver->flush();

  // Verify the first 8 items are now reclaimed and the others are still there
  for (size_t i = 0; i < 8; i++) {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[i].key(), value));
  }
  for (size_t i = 8; i < log.size(); i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

TEST(BlockCache, StackAllocReclaim) {
  std::vector<CacheEntry> log;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  // Fill region 0
  { // 2k
    CacheEntry e{bg.gen(8), bg.gen(2000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  { // 10k
    CacheEntry e{bg.gen(8), bg.gen(10'000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();
  // Fill region 1
  { // 8k
    CacheEntry e{bg.gen(8), bg.gen(8000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  { // 6k
    CacheEntry e{bg.gen(8), bg.gen(6000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();
  // Fill region 2
  { // 4k
    CacheEntry e{bg.gen(8), bg.gen(4000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  { // 4k
    CacheEntry e{bg.gen(8), bg.gen(4000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  { // 4k
    CacheEntry e{bg.gen(8), bg.gen(4000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();
  // Fill region 3
  // Triggers reclamation of region 0
  { // 15k
    CacheEntry e{bg.gen(8), bg.gen(15'000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  // Region 0 eviction
  EXPECT_CALL(*device, readImpl(0, 16 * 1024, _));
  // Lookup log[2]
  EXPECT_CALL(*device, readImpl(8192, 4096, _)).Times(2);
  EXPECT_CALL(*device, readImpl(4096, 4096, _))
      .WillOnce(Invoke([md = device.get(), &sp](uint64_t offset, uint32_t size,
                                                void* buffer) {
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

  // We expect the first three regions to be filled, last region to be
  // reclaimed to be the free region. First two regions will be tracked,
  // and the third region to remain in allocation state.
  BufferGen bg;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      CacheEntry e{bg.gen(8), bg.gen(1000)};
      driver->insertAsync(e.key(), e.value(),
                          [](Status status, BufferView /*key */) {
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

  // Send insert. Will schedule a reclamation job. We will also track
  // the third region as it had been filled up. We will also expect
  // to evict the first region eventually for the reclaim.
  CacheEntry e{bg.gen(8), bg.gen(1000)};
  EXPECT_EQ(0, exPtr->getQueueSize());
  driver->insertAsync(e.key(), e.value(),
                      [](Status status, BufferView /*key */) {
                        EXPECT_EQ(Status::Ok, status);
                      });
  // Insert finds region is full and  puts region for tracking, resets allocator
  // and retries.
  EXPECT_TRUE(exPtr->runFirstIf("insert"));
  EXPECT_TRUE(exPtr->runFirstIf("reclaim"));

  Buffer value;

  EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
  EXPECT_EQ(log[2].value(), value.view());

  // Eviction blocks access but reclaim will fail as there is still a reader
  // outstanding
  EXPECT_FALSE(exPtr->runFirstIf("reclaim.evict"));

  std::thread lookupThread2([&driver, &log] {
    Buffer value2;
    // Can't access region 0: blocked. Will retry until unblocked.
    EXPECT_EQ(Status::NotFound, driver->lookup(log[2].key(), value2));
  });

  // To make sure that the reason for key not found is access block, but not
  // evicted from the index, remove it manually and expect it was found.
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  // Reclaim still fails as the last reader is still outstanding
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
            driver->insert(makeView("key1"), value1.view()));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key2"), value2.view()));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key3"), value3.view()));

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
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  // Fill up the first region and write one entry into the second region
  for (size_t i = 0; i < 17; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, cache.insertAsync(e.key(), e.value(), nullptr));
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
  auto& mp = *policy.get();

  auto proxy = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto proxyPtr = proxy.get();
  // Setup delegating device before creating, because makeIOBuffer is called
  // during construction.
  proxyPtr->setRealDevice(setupResetTestDevice(kDeviceSize));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *proxyPtr, {1024});
  config.numInMemBuffers = 0;

  expectRegionsTracked(mp, {0, 1, 2, 3});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  expectRegionsTracked(mp, {0});
  resetTestRun(*driver);

  EXPECT_CALL(mp, reset());
  expectRegionsTracked(mp, {0, 1, 2, 3});
  driver->reset();

  // Create a new device with same expectations
  proxyPtr->setRealDevice(setupResetTestDevice(config.cacheSize));
  expectRegionsTracked(mp, {0});
  resetTestRun(*driver);
}

TEST(BlockCache, ResetInMemBuffers) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy.get();

  auto proxy = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  auto proxyPtr = proxy.get();
  // Setup delegating device before creating, because makeIOBuffer is called
  // during construction.
  proxyPtr->setRealDevice(setupResetTestDeviceInMemBuffers(kDeviceSize));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *proxyPtr, {1024});
  config.numInMemBuffers = 3;
  expectRegionsTracked(mp, {0, 1, 2, 3});
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  expectRegionsTracked(mp, {0, 1});
  resetTestRun(*driver);

  EXPECT_CALL(mp, reset());
  expectRegionsTracked(mp, {0, 1, 2, 3});
  driver->reset();

  // Create a new device with same expectations
  proxyPtr->setRealDevice(setupResetTestDeviceInMemBuffers(config.cacheSize));
  expectRegionsTracked(mp, {0, 1});
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
    // destructor callback is executed when evicted or explicit removed
    EXPECT_CALL(cb, call(log[6].key(), log[6].value(), _)).Times(0);
    EXPECT_CALL(cb, call(log[5].key(), log[5].value(), _)).Times(0);
    EXPECT_CALL(cb, call(log[4].key(), log[4].value(), _)).Times(0);
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto* exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  for (size_t i = 0; i < 12; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value()));
  }
  // Remove before it gets evicted
  EXPECT_EQ(Status::Ok, driver->remove(log[4].key()));
  exPtr->finish();
  // Kick off eviction of region 1
  mockRegionsEvicted(mp, {1});
  EXPECT_EQ(Status::Ok, driver->insert(log[12].key(), log[12].value()));
  exPtr->finish();

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
    // destructor callback is executed when evicted or explicit removed
    EXPECT_CALL(cb, call(log[3].key(), log[3].value(), _)).Times(0);
    EXPECT_CALL(cb, call(log[2].key(), log[2].value(), _)).Times(0);
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto* exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  mockRegionsEvicted(mp, {0, 1, 2, 3, 1});
  for (size_t i = 0; i < 7; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value()));
  }
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));
  EXPECT_EQ(Status::Ok, driver->insert(log[7].key(), log[7].value()));
  EXPECT_EQ(Status::Ok, driver->insert(log[8].key(), log[8].value()));

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

  exPtr->finish();
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
    // destructor callback is executed when evicted or explicit removed
    EXPECT_CALL(cb, call(log[3].key(), log[3].value(), _)).Times(0);
    EXPECT_CALL(cb, call(log[2].key(), log[2].value(), _)).Times(0);
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto* exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 9;
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  mockRegionsEvicted(mp, {0, 1, 2, 3, 1});
  for (size_t i = 0; i < 7; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value()));
  }
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));
  EXPECT_EQ(Status::Ok, driver->insert(log[7].key(), log[7].value()));
  EXPECT_EQ(Status::Ok, driver->insert(log[8].key(), log[8].value()));

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

  exPtr->finish();
}

TEST(BlockCache, RegionLastOffset) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
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
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Triggers reclamation
  for (size_t i = 0; i < 7; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  auto& mp = *policy;
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {2048});
  config.regionSize = 15 * 1024; // so regionSize is not multiple of sizeClass
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // We don't fill up a region so won't track any. But we will first evict
  // rid: 0 for the two allocations, and we will evict rid: 1 in order to
  // satisfy the one free region requirement.
  expectRegionsTracked(mp, {});
  BufferGen bg;
  for (size_t i = 0; i < 2; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    auto key = e.key();
    auto value = e.value();
    EXPECT_EQ(Status::Ok,
              driver->insertAsync(key, value, saveEntryCb(std::move(e))));
  }
  driver->flush();
  // We will reset eviction policy and also reinitialize by tracking all regions
  EXPECT_CALL(mp, reset());
  expectRegionsTracked(mp, {0, 1, 2, 3});
  driver->reset();

  // Allocator region fills every 7 inserts.
  expectRegionsTracked(mp, {0, 1});
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 7; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Triggers reclamation
  expectRegionsTracked(mp, {2});
  for (size_t i = 0; i < 7; i++) {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  auto& mp = *policy;
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096, 8192});
  config.numInMemBuffers = 0;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  expectRegionsTracked(mp, {0, 1});
  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
      log.push_back(std::move(e));
    }
  }

  driver->persist();

  {
    testing::InSequence s;
    EXPECT_CALL(mp, reset());
    expectRegionsTracked(mp, {0, 1, 2, 3});
    EXPECT_CALL(mp, reset());
    expectRegionsTracked(mp, {3, 0, 1, 2});
  }
  EXPECT_TRUE(driver->recover());

  for (auto& entry : log) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(entry.key(), value));
    EXPECT_EQ(entry.value(), value.view());
  }

  // This insertion should evict region 3 and the fact we start
  // writing into region 3 means we should start evicting region 0
  for (size_t i = 0; i < 3; i++) {
    CacheEntry e{bg.gen(8), bg.gen(3200)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
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

TEST(BlockCache, RecoveryWithDifferentCacheSize) {
  // Test this is a warm roll for changing cache size, we can remove this once
  // everyone is on V12 and beyond
  class MockRecordWriter : public RecordWriter {
   public:
    explicit MockRecordWriter(folly::IOBufQueue& ioq)
        : rw_{createMemoryRecordWriter(ioq)} {
      ON_CALL(*this, writeRecord(_))
          .WillByDefault(Invoke([this](std::unique_ptr<folly::IOBuf> iobuf) {
            rw_->writeRecord(std::move(iobuf));
          }));
    }

    MOCK_METHOD1(writeRecord, void(std::unique_ptr<folly::IOBuf>));

    bool invalidate() override { return rw_->invalidate(); }

   private:
    std::unique_ptr<RecordWriter> rw_;
  };

  std::vector<uint32_t> hits(4);
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();

  std::vector<std::string> keys;
  folly::IOBufQueue originalMetadata;
  folly::IOBufQueue largerSizeMetadata;
  folly::IOBufQueue largerSizeOldVersionMetadata;
  {
    auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits),
                             *device, {kRegionSize});
    auto engine = makeEngine(std::move(config), metadataSize);

    auto numItems = kDeviceSize / kRegionSize;
    for (uint64_t i = 0; i < numItems; i++) {
      auto key = folly::sformat("key_{}", i);
      while (true) {
        if (Status::Ok ==
            engine->insert(
                makeHK(BufferView{key.size(),
                                  reinterpret_cast<uint8_t*>(key.data())}),
                makeView("value"))) {
          break;
        }
        // Runs the async job to get a free region
        ex->finish();
      }
      // Do not keep track of the key in the first region, since it will
      // be evicted to maintain one clean region.
      if (i > 0) {
        keys.push_back(key);
      }
    }
    // We'll evict the first region to maintain one clean region
    ex->finish();

    for (auto& key : keys) {
      Buffer buffer;
      ASSERT_EQ(Status::Ok,
                engine->lookup(
                    makeHK(BufferView{key.size(),
                                      reinterpret_cast<uint8_t*>(key.data())}),
                    buffer));
    }

    auto rw1 = createMemoryRecordWriter(originalMetadata);
    engine->persist(*rw1);

    // We will make sure we write a larger cache size into the record
    MockRecordWriter rw2{largerSizeMetadata};
    EXPECT_CALL(rw2, writeRecord(_))
        .Times(testing::AtLeast(1))
        .WillOnce(Invoke([&rw2](std::unique_ptr<folly::IOBuf> iobuf) {
          Deserializer deserializer{iobuf->data(),
                                    iobuf->data() + iobuf->length()};
          auto c = deserializer.deserialize<serialization::BlockCacheConfig>();
          c.cacheSize_ref() = *c.cacheSize_ref() + 4096;
          serializeProto(c, rw2);
        }));
    engine->persist(rw2);

    // We will make sure we write a larger cache size and dummy version v11
    MockRecordWriter rw3{largerSizeOldVersionMetadata};
    EXPECT_CALL(rw3, writeRecord(_))
        .Times(testing::AtLeast(1))
        .WillOnce(Invoke([&rw3](std::unique_ptr<folly::IOBuf> iobuf) {
          Deserializer deserializer{iobuf->data(),
                                    iobuf->data() + iobuf->length()};
          auto c = deserializer.deserialize<serialization::BlockCacheConfig>();
          c.version_ref() = 11;
          c.cacheSize_ref() = *c.cacheSize_ref() + 4096;
          serializeProto(c, rw3);
        }));
    engine->persist(rw3);
  }

  // Recover with the right size
  {
    auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits),
                             *device, {kRegionSize});
    auto engine = makeEngine(std::move(config), metadataSize);
    auto rr = createMemoryRecordReader(originalMetadata);
    ASSERT_TRUE(engine->recover(*rr));

    // All the keys should be present
    for (auto& key : keys) {
      Buffer buffer;
      ASSERT_EQ(Status::Ok,
                engine->lookup(
                    makeHK(BufferView{key.size(),
                                      reinterpret_cast<uint8_t*>(key.data())}),
                    buffer));
    }
  }

  // Recover with larger size
  {
    auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits),
                             *device, {kRegionSize});
    // Uncomment this after BlockCache everywhere is on v12, and remove
    // the below recover test
    // ASSERT_THROW(makeEngine(std::move(config), metadataSize),
    //              std::invalid_argument);
    auto engine = makeEngine(std::move(config), metadataSize);
    auto rr = createMemoryRecordReader(largerSizeMetadata);
    ASSERT_FALSE(engine->recover(*rr));
  }

  // Recover with larger size but from v11 which should be a warm roll
  // Remove this after BlockCache everywhere is on v12
  {
    auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits),
                             *device, {kRegionSize});
    auto engine = makeEngine(std::move(config), metadataSize);
    auto rr = createMemoryRecordReader(largerSizeOldVersionMetadata);
    ASSERT_TRUE(engine->recover(*rr));

    // All the keys should be present
    for (auto& key : keys) {
      Buffer buffer;
      ASSERT_EQ(Status::Ok,
                engine->lookup(
                    makeHK(BufferView{key.size(),
                                      reinterpret_cast<uint8_t*>(key.data())}),
                    buffer));
    }
  }
}

// This test does the following
// 1. Test creation of BlockCache with BlockCache::kMinAllocAlignSize aligned
//    slot sizes fail when in memory buffers are not enabled
// 2. Test the following order of operations succeed when in memory buffers
//    are enabled and slot sizes are BlockCache::kMinAllocAlignSize aligned
//    * create with two class-sizes of size 3K and 6.5K
//    * insert 12 items in two different regions
//    * lookup the 12 items to make sure they are in the cache
//    * flush the device
//    * lookup again to make sure the 12 items still exist
//    * persist the cache
//    * recover the cache
//    * lookup the 12 items again to make sure they still exist
//
TEST(BlockCache, SmallerSlotSizesWithInMemBuffers) {
  std::vector<uint32_t> hits(4);
  std::vector<uint32_t> sizeClasses = {6 * BlockCache::kMinAllocAlignSize,
                                       14 * BlockCache::kMinAllocAlignSize};
  {
    auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

    size_t metadataSize = 3 * 1024 * 1024;
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, sizeClasses);
    config.numInMemBuffers = 0;
    try {
      auto engine = makeEngine(std::move(config), metadataSize);
    } catch (const std::invalid_argument& e) {
      EXPECT_EQ(e.what(), std::string("invalid size class: 3072"));
    }
  }
  const uint64_t myDeviceSize = 16 * 1024 * 1024;
  auto device = createMemoryDevice(myDeviceSize, nullptr /* encryption */);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  size_t metadataSize = 3 * 1024 * 1024;
  auto ex = makeJobScheduler();
  auto config =
      makeConfig(*ex, std::move(policy), *device, sizeClasses, myDeviceSize);
  config.numInMemBuffers = 4;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 2 regions
  for (size_t j = 0; j < 5; j++) {
    CacheEntry e{bg.gen(8), bg.gen(2700)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
    log.push_back(std::move(e));
  }
  for (size_t j = 0; j < 3; j++) {
    CacheEntry e{bg.gen(8), bg.gen(5700)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
    log.push_back(std::move(e));
  }
  for (size_t i = 0; i < 8; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }

  driver->flush();
  for (size_t i = 0; i < 8; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
  driver->persist();
  driver->reset();
  EXPECT_TRUE(driver->recover());
  for (size_t i = 0; i < 8; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
}

// Create devices with various sizes and verify that the alloc align size
// is equal to expected size. Some of the device size are not 2 power aligned
// and is expected to get aligned alloc size.
TEST(BlockCache, testAllocAlignSizesInMemBuffers) {
  std::vector<uint32_t> hits(4);

  std::vector<std::pair<size_t, uint32_t>> testSizes = {
      {4 * 1024 * 1024, BlockCache::kMinAllocAlignSize},
      {5UL * 1024 * 1024 * 1024, BlockCache::kMinAllocAlignSize},
      {8UL * 1024 * 1024 * 1024 * 1024,
       static_cast<uint32_t>(8UL * 1024 * 1024 * 1024 * 1024 >> 32)},
      {745UL * 4 * 1024 * 1024 * 1024, 1024},
      {1370UL * 4 * 1024 * 1024 * 1024, 2048},
      {1800UL * 4 * 1024 * 1024 * 1024, 2048},
      {2900UL * 4 * 1024 * 1024 * 1024, 4096}};

  for (size_t i = 0; i < testSizes.size(); i++) {
    auto deviceSize = testSizes[i].first;
    int fd = open("/dev/null", O_RDWR);
    folly::File f = folly::File(fd);
    auto device =
        createDirectIoFileDevice(std::move(f), deviceSize, 4096, nullptr, 0);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096, 8192});
    config.numInMemBuffers = 4;
    auto blockCache = std::make_unique<BlockCache>(std::move(config));
    EXPECT_EQ(blockCache->getAllocAlignSize(), testSizes[i].second);
  }
}

// This test creates a cache based on RAID devices with in-memory buffers
// DISABLED, inserts some items and persists the cache. Then creates
// a cache with the same RAID device with in-memory buffers ENABLED and
// tries to recover it. Verifies that the recovery fails because enabling
// in-memory buffers makes alloc align size to be
// 512(BlockCache::kMinAllocAlignSize) and the cache at the persist has
// 4K as the alloc align size.
//
// We cannot use memory device for this test because we need a way to persist
// and recover by using different "driver". And this is not possible with
// in-memory devices. It has to be a file based device where the device
// name is known
//
TEST(BlockCache, PersistRecoverWithInMemBuffers) {
  auto filePath = folly::sformat("/tmp/DEVICE_RAID0IO_TEST-{}", ::getpid());

  int deviceSize = 16 * 1024 * 1024;
  int ioAlignSize = 4096;
  int fd = open(filePath.c_str(), O_RDWR | O_CREAT);
  folly::File f = folly::File(fd);
  auto device = createDirectIoFileDevice(std::move(f), deviceSize, ioAlignSize,
                                         nullptr, 0);

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  size_t metadataSize = 3 * 1024 * 1024;
  auto ex = makeJobScheduler();
  auto config =
      makeConfig(*ex, std::move(policy), *device, {4096, 8192}, deviceSize);
  config.numInMemBuffers = 0;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
      log.push_back(std::move(e));
    }
  }

  driver->persist();

  int newFd = open(filePath.c_str(), O_RDWR | O_CREAT);
  folly::File newF = folly::File(newFd);
  auto newDevice = createDirectIoFileDevice(std::move(newF), deviceSize,
                                            ioAlignSize, nullptr, 0);
  auto newPolicy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto newEx = makeJobScheduler();
  auto newConfig = makeConfig(*newEx, std::move(newPolicy), *newDevice,
                              {4096, 8192}, deviceSize);
  newConfig.numInMemBuffers = 4;
  auto newEngine = makeEngine(std::move(newConfig), metadataSize);
  auto newDriver = makeDriver(std::move(newEngine), std::move(newEx),
                              std::move(newDevice), metadataSize);
  // recovery should fail because we have enabled in memory buffers which would
  // change the min alloc alignment to 512.
  EXPECT_FALSE(newDriver->recover());
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
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
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
    auto& mp = *policy;
    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    auto engine = makeEngine(std::move(config), metadataSize);
    auto driver = makeDriver(std::move(engine), std::move(ex),
                             std::move(device), metadataSize);

    expectRegionsTracked(mp, {0, 1});
    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t i = 0; i < 3; i++) {
      for (size_t j = 0; j < 4; j++) {
        CacheEntry e{bg.gen(8), bg.gen(3200)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
        log.push_back(std::move(e));
      }
    }
    driver->flush();

    driver->persist();
  }

  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    config.regionSize = 8 * 1024; // Region size differs from original
    auto engine = makeEngine(std::move(config), metadataSize);
    auto driver = makeDriver(std::move(engine), std::move(ex),
                             std::move(device), metadataSize);

    EXPECT_FALSE(driver->recover());
  }
  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    config.checksum = true;
    auto engine = makeEngine(std::move(config));
    auto driver = makeDriver(std::move(engine), std::move(ex),
                             std::move(device), metadataSize);

    EXPECT_FALSE(driver->recover());
  }
}

TEST(BlockCache, RecoveryCorruptedData) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  std::unique_ptr<Driver> driver;
  {
    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, {4096});
    auto engine = makeEngine(std::move(config), metadataSize);
    auto rw = createMetadataRecordWriter(*device, metadataSize);
    driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                        metadataSize);

    // persist metadata
    driver->persist();

    // corrupt the data
    auto ioBuf = folly::IOBuf::createCombined(512);
    std::generate(ioBuf->writableData(), ioBuf->writableTail(),
                  std::minstd_rand());

    rw->writeRecord(std::move(ioBuf));
  }

  // Expect recovery to fail.
  EXPECT_FALSE(driver->recover());
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

  // Should not schedule any jobs on creation.
  EXPECT_EQ(0, exPtr->getQueueSize());
}

// This test is written with the assumption that the device is block device
// with size 1024. So, create the Memory Device with the block size of 1024
TEST(BlockCache, Checksum) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  constexpr uint32_t kIOAlignSize = 1024;
  auto device =
      createMemoryDevice(kDeviceSize, nullptr /* encryption */, kIOAlignSize);
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
  EXPECT_EQ(Status::Ok, driver->insertAsync(e1.key(), e1.value(), nullptr));
  exPtr->finish();
  // Buffer is too large (slot is smaller)
  // Located at offset 2 * kIOAlignSize
  CacheEntry e2{bg.gen(8), bg.gen(100)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e2.key(), e2.value(), nullptr));
  exPtr->finish();
  // Buffer is too small
  // Located at offset 3 * kIOAlignSize
  CacheEntry e3{bg.gen(8), bg.gen(3000)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e3.key(), e3.value(), nullptr));
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
  Buffer buf{2 * kIOAlignSize, kIOAlignSize};
  memcpy(buf.data() + buf.size() - 4, "hack", 4);
  EXPECT_TRUE(device->write(0, std::move(buf)));
  EXPECT_EQ(Status::DeviceError, driver->lookup(e1.key(), value));

  const char corruption[kIOAlignSize]{"hack"};
  // Corrupt e2: key, reported as "key not found"
  EXPECT_TRUE(device->write(
      2 * kIOAlignSize,
      Buffer{BufferView{kIOAlignSize,
                        reinterpret_cast<const uint8_t*>(corruption)},
             kIOAlignSize}));
  EXPECT_EQ(Status::NotFound, driver->lookup(e2.key(), value));

  // Corrupt e3: value
  EXPECT_TRUE(device->write(
      3 * kIOAlignSize,
      Buffer{BufferView{1024, reinterpret_cast<const uint8_t*>(corruption)},
             kIOAlignSize}));
  EXPECT_EQ(Status::DeviceError, driver->lookup(e3.key(), value));

  EXPECT_EQ(0, exPtr->getQueueSize());
}

TEST(BlockCache, HitsReinsertionPolicy) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {4096});
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
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
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3200)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
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
}

TEST(BlockCache, UsePriorities) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kRegionSize * 6;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config =
      makeConfig(*ex, std::move(policy), *device, {}, kRegionSize * 6);
  config.numPriorities = 3;
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  // Enable in-mem buffer so size align on 512 bytes boundary
  config.numInMemBuffers = 3;
  config.cleanRegionsPool = 3;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  std::vector<CacheEntry> log;
  BufferGen bg;

  EXPECT_CALL(mp, track(_)).Times(4);
  EXPECT_CALL(mp, track(EqRegionPri(1)));
  EXPECT_CALL(mp, track(EqRegionPri(2)));

  // Populate 4 regions to trigger eviction
  for (size_t i = 0; i < 4; i++) {
    for (size_t j = 0; j < 4; j++) {
      // This should give us a 4KB payload due to 512 byte alignment
      CacheEntry e{bg.gen(8), bg.gen(3800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
    if (i == 0) {
      Buffer value;
      // Look up 2nd item twice, so we'll reinsert it with pri-1
      EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
      // Look up 3rd item three times, so we'll reinsert it with pri-2
      // Note that we reinsert with pri-2, because any hits larger than
      // max priority will be assigned the max priority.
      EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
      EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
      EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
    }
  }

  // Verify the 1st region is evicted but 2nd and 3rd items are reinserted
  {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
    EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
    EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
  }
}

TEST(BlockCache, UsePrioritiesSizeClass) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kRegionSize * 6;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {2048, 4096},
                           kRegionSize * 6);
  config.numPriorities = 3;
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  // Enable in-mem buffer so size align on 512 bytes boundary
  config.numInMemBuffers = 3;
  config.cleanRegionsPool = 3;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  std::vector<CacheEntry> log;
  BufferGen bg;

  EXPECT_CALL(mp, track(_)).Times(4);
  EXPECT_CALL(mp, track(EqRegionPri(1)));
  EXPECT_CALL(mp, track(EqRegionPri(2)));

  // Populate 4 regions to trigger eviction
  for (size_t i = 0; i < 2; i++) {
    for (size_t j = 0; j < 4; j++) {
      // This should give us a 4KB payload
      CacheEntry e{bg.gen(8), bg.gen(3800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
    for (size_t j = 0; j < 8; j++) {
      // This should give us a 2KB payload
      CacheEntry e{bg.gen(8), bg.gen(1800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
    if (i == 0) {
      Buffer value;
      // Look up 2nd item twice, so we'll reinsert it with pri-1
      EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
      // Look up 3rd item three times, so we'll reinsert it with pri-2
      // Note that we reinsert with pri-2, because any hits larger than
      // max priority will be assigned the max priority.
      EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
      EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
      EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
    }
  }

  // Verify the 1st region is evicted but 2nd and 3rd items are reinserted
  {
    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
    EXPECT_EQ(Status::Ok, driver->lookup(log[1].key(), value));
    EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
  }
}

TEST(BlockCache, DeviceFlushFailureSync) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<MockDevice>(kDeviceSize, 1024);

  testing::InSequence inSeq;
  EXPECT_CALL(*device, writeImpl(_, _, _)).WillRepeatedly(Return(false));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 4;
  config.inMemBufFlushRetryLimit = kFlushRetryLimit;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  CacheEntry e{bg.gen(8), bg.gen(800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}));
  driver->flush();

  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_inmem_flush_retries") {
      EXPECT_EQ(kFlushRetryLimit, count);
    }
    if (name == "navy_bc_inmem_flush_failures") {
      EXPECT_EQ(1, count);
    }
  });
}

TEST(BlockCache, DeviceFlushFailureAsync) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<MockDevice>(kDeviceSize, 1024);

  testing::InSequence inSeq;
  EXPECT_CALL(*device, writeImpl(_, _, _)).WillRepeatedly(Return(false));

  auto ex = makeJobScheduler();
  auto config =
      makeConfig(*ex, std::move(policy), *device, {16 * 1024 /* size class */});
  config.numInMemBuffers = 4;
  config.inMemBufFlushRetryLimit = kFlushRetryLimit;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  CacheEntry e{bg.gen(8), bg.gen(800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}));
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}));
  driver->flush();

  driver->getCounters([](folly::StringPiece name, double count) {
    if (name == "navy_bc_inmem_flush_retries") {
      EXPECT_EQ(kFlushRetryLimit * 2, count);
    }
    if (name == "navy_bc_inmem_flush_failures") {
      EXPECT_EQ(2, count);
    }
  });
}

TEST(BlockCache, testItemDestructor) {
  std::vector<CacheEntry> log;
  {
    BufferGen bg;
    // 1st region, 12k
    log.emplace_back(Buffer{makeView("key_000")}, bg.gen(5'000));
    log.emplace_back(Buffer{makeView("key_001")}, bg.gen(7'000));
    // 2nd region, 14k
    log.emplace_back(Buffer{makeView("key_002")}, bg.gen(5'000));
    log.emplace_back(Buffer{makeView("key_003")}, bg.gen(3'000));
    log.emplace_back(Buffer{makeView("key_004")}, bg.gen(6'000));
    // 3rd region, 16k, overwrites
    log.emplace_back(Buffer{log[0].key()}, bg.gen(8'000));
    log.emplace_back(Buffer{log[3].key()}, bg.gen(8'000));
    // 4th region, 15k
    log.emplace_back(Buffer{makeView("key_007")}, bg.gen(9'000));
    log.emplace_back(Buffer{makeView("key_008")}, bg.gen(6'000));
    ASSERT_EQ(9, log.size());
  }

  MockDestructor cb;
  ON_CALL(cb, call(_, _, _))
      .WillByDefault(
          Invoke([](BufferView key, BufferView val, DestructorEvent event) {
            XLOGF(ERR, "cb key: {}, val: {}, event: {}", toString(key),
                  toString(val).substr(0, 20), toString(event));
          }));

  {
    testing::InSequence inSeq;
    // explicit remove 2
    EXPECT_CALL(cb,
                call(log[2].key(), log[2].value(), DestructorEvent::Removed));
    // explicit remove 0
    EXPECT_CALL(cb,
                call(log[0].key(), log[5].value(), DestructorEvent::Removed));
    // Region evictions is backwards to the order of insertion.
    EXPECT_CALL(cb,
                call(log[4].key(), log[4].value(), DestructorEvent::Recycled));
    EXPECT_CALL(cb, call(log[3].key(), log[3].value(), _)).Times(0);
    EXPECT_CALL(cb, call(log[2].key(), log[2].value(), _)).Times(0);
  }

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto* exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device, {});
  config.numInMemBuffers = 9;
  config.destructorCb = toCallback(cb);
  config.itemDestructorEnabled = true;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  mockRegionsEvicted(mp, {0, 1, 2, 3, 1});
  for (size_t i = 0; i < 7; i++) {
    XLOG(ERR, "insert ") << toString(log[i].key());
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value()));
  }

  // remove with cb triggers destructor Immediately
  XLOG(ERR, "remove ") << toString(log[2].key());
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  // remove with cb triggers destructor Immediately
  XLOG(ERR, "remove ") << toString(log[0].key());
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));

  // remove again
  EXPECT_EQ(Status::NotFound, driver->remove(log[2].key()));
  EXPECT_EQ(Status::NotFound, driver->remove(log[0].key()));

  XLOG(ERR, "insert ") << toString(log[7].key());
  EXPECT_EQ(Status::Ok, driver->insert(log[7].key(), log[7].value()));
  // insert will trigger evictions
  XLOG(ERR, "insert ") << toString(log[8].key());
  EXPECT_EQ(Status::Ok, driver->insert(log[8].key(), log[8].value()));

  Buffer value;
  EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
  EXPECT_EQ(Status::NotFound, driver->lookup(log[5].key(), value));

  exPtr->finish();
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
