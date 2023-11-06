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

#include <folly/File.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <future>
#include <vector>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/common/ConditionVariable.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Utils.h"
#include "cachelib/common/inject_pause.h"
#include "cachelib/navy/block_cache/BlockCache.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Utils.h"
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

namespace facebook::cachelib::navy::tests {
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
                              uint64_t cacheSize = kDeviceSize) {
  BlockCache::Config config;
  config.scheduler = &scheduler;
  config.regionSize = kRegionSize;
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
  config.enginePairs.emplace_back(nullptr, std::move(largeItemCache), 0,
                                  scheduler.get());
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
  CacheEntry(HashedKey k, Buffer v)
      : key_{makeView(k.key())}, value_{std::move(v)} {}
  CacheEntry(CacheEntry&&) = default;
  CacheEntry& operator=(CacheEntry&&) = default;

  HashedKey key() const { return makeHK(key_); }

  BufferView value() const { return value_.view(); }

 private:
  Buffer key_, value_;
};

InsertCallback saveEntryCb(CacheEntry&& e) {
  return [entry = std::move(e)](Status /* status */, HashedKey /* key */) {};
}

void finishAllJobs(MockJobScheduler& ex) {
  while (ex.getQueueSize()) {
    ex.runFirst();
  }
}

// This class creates a Driver that has an entry on its device with a similar
// situation as hash collision.
class CollisionCreator {
 public:
  // After the initialization:
  // It has the hash value of key in block cache index.
  // On the block cache device, it has an entry with key+addtion as key, and val
  // as value.
  CollisionCreator(
      const char* key,
      const char* val,
      const char* addition,
      std::function<void(BlockCache::Config&)> configModifier = {}) {
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    if (configModifier) {
      configModifier(config);
    }
    auto engine = makeEngine(std::move(config));
    driver = makeDriver(std::move(engine), std::move(ex));
    CacheEntry e{strzBuffer(key), strzBuffer(val)};
    EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key), value));
    EXPECT_EQ(makeView(val), value.view());
    driver->flush();

    uint8_t buf[512]{}; // 512 is the minimal alloc alignment size
    EXPECT_TRUE(device->read(0, sizeof(buf), buf));
    size_t kKeySize{strlen(key)}; // "key"
    size_t keyOffset = sizeof(buf) - kSizeOfEntryDesc - kKeySize;
    BufferView keyOnDevice{kKeySize, buf + keyOffset};
    EXPECT_EQ(keyOnDevice, makeView(key));

    std::memcpy(buf + keyOffset, addition, strlen(addition));
    EXPECT_TRUE(device->write(0, Buffer{BufferView{sizeof(buf), buf}}));
  }

  std::unique_ptr<Driver> driver;

 private:
  std::vector<uint32_t> hits{4};
  std::unique_ptr<Device> device;
};

} // namespace

TEST(BlockCache, InsertLookup) {
  std::vector<CacheEntry> log;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    // Flush the first region
    driver->flush();
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.key(), value));

    driver->getCounters({[](folly::StringPiece name, double count,
                            CounterVisitor::CounterType type) {
      if (name == "navy_bc_lookups" &&
          type == CounterVisitor::CounterType::RATE) {
        EXPECT_EQ(1, count);
      }
    }});

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
  EXPECT_EQ(2, hits[0]);
  EXPECT_EQ(16, hits[1]);
  EXPECT_EQ(0, hits[2]);
  EXPECT_EQ(0, hits[3]);
}

TEST(BlockCache, InsertLookupSync) {
  std::vector<CacheEntry> log;
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 1;
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
    log.push_back(std::move(e));
  }

  // Zero hits because the buffer has not been flushed when the lookups
  // happened. We do not "touch" a region until it has been flushed
  // to the device.
  EXPECT_EQ(0, hits[0]);
  EXPECT_EQ(0, hits[1]);
  EXPECT_EQ(0, hits[2]);
  EXPECT_EQ(0, hits[3]);

  for (size_t i = 0; i < 17; i++) {
    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[i].key(), value));
    EXPECT_EQ(log[i].value(), value.view());
  }
  // Still zero hit in the second region, because we didn't fill it
  // up. It's not flushed to the device, and thus no hits.
  EXPECT_EQ(16, hits[0]);
  EXPECT_EQ(0, hits[1]);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));
  MockInsertCB cbInsert;
  EXPECT_CALL(cbInsert, call(Status::Ok, makeHK("key")));
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(makeHK("key"), makeView("value"),
                                toCallback(cbInsert)));
  driver->flush();

  MockLookupCB cbLookup;
  EXPECT_CALL(cbLookup, call(Status::Ok, makeHK("key"), makeView("value")));
  EXPECT_CALL(cbLookup, call(Status::NotFound, makeHK("cat"), BufferView{}));
  driver->lookupAsync(makeHK("key"),
                      [&cbLookup](Status status, HashedKey key, Buffer value) {
                        cbLookup.call(status, key, value.view());
                      });

  driver->lookupAsync(makeHK("cat"),
                      [&cbLookup](Status status, HashedKey key, Buffer value) {
                        cbLookup.call(status, key, value.view());
                      });
  driver->flush();

  MockRemoveCB cbRemove;
  EXPECT_CALL(cbRemove, call(Status::Ok, makeHK("key")));
  EXPECT_CALL(cbRemove, call(Status::NotFound, makeHK("cat")));
  driver->removeAsync(makeHK("key"), toCallback(cbRemove));
  driver->removeAsync(makeHK("cat"), toCallback(cbRemove));
  driver->flush();
}

TEST(BlockCache, Remove) {
  std::vector<CacheEntry> log;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  EXPECT_EQ(Status::Ok, driver->remove(makeHK("dog")));
  EXPECT_EQ(Status::NotFound, driver->remove(makeHK("fox")));
  EXPECT_EQ(Status::Ok, driver->lookup(log[0].key(), value));
  EXPECT_EQ(log[0].value(), value.view());
  EXPECT_EQ(Status::NotFound, driver->lookup(log[1].key(), value));
  EXPECT_EQ(Status::NotFound, driver->remove(makeHK("dog")));
  EXPECT_EQ(Status::NotFound, driver->remove(makeHK("fox")));
  EXPECT_EQ(Status::Ok, driver->remove(makeHK("cat")));
  EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
  EXPECT_EQ(Status::NotFound, driver->lookup(log[1].key(), value));
}

// Test precise remove flag works
TEST(BlockCache, PreciseRemove) {
  {
    // default, not preciseRemove.
    auto collision = CollisionCreator("key", "value", "abc");
    Buffer value;
    // Behavior: old "key" can remove the entry "key"+"abc": "value".
    EXPECT_EQ(Status::Ok, collision.driver->remove(makeHK("key")));
    collision.driver->getCounters({[](folly::StringPiece name, double count,
                                      CounterVisitor::CounterType type) {
      // The counter is not populated because preciseRemove_ and item
      // destructors are not triggered.
      if (name == "navy_bc_remove_attempt_collisions" &&
          type == CounterVisitor::CounterType::RATE) {
        EXPECT_EQ(0, count);
      }
    }});
  }

  {
    // With preciseRemove.
    auto collision =
        CollisionCreator("key", "value", "abc",
                         [](BlockCache::Config& c) { c.preciseRemove = true; });
    Buffer value;
    // Behavior: old "key" can not remove the entry "key"+"abc": "value"
    EXPECT_EQ(Status::NotFound, collision.driver->remove(makeHK("key")));
    collision.driver->getCounters({[](folly::StringPiece name, double count,
                                      CounterVisitor::CounterType type) {
      // The counter is populated
      if (name == "navy_bc_remove_attempt_collisions" &&
          type == CounterVisitor::CounterType::RATE) {
        EXPECT_EQ(1, count);
      }
    }});
  }

  {
    // Without preciseRemove, with item destructor. Remove is not precise, but
    // the ODS counter should be incremented.
    MockDestructor cb;

    auto collision =
        CollisionCreator("key", "value", "abc", [&cb](BlockCache::Config& c) {
          c.itemDestructorEnabled = true;
          c.destructorCb = toCallback(cb);
        });
    Buffer value;

    // Behavior: old "key" can not remove the entry "key"+"abc": "value"
    EXPECT_EQ(Status::Ok, collision.driver->remove(makeHK("key")));
    collision.driver->getCounters({[](folly::StringPiece name, double count,
                                      CounterVisitor::CounterType type) {
      // The counter is populated.
      if (name == "navy_bc_remove_attempt_collisions" &&
          type == CounterVisitor::CounterType::RATE) {
        EXPECT_EQ(1, count);
      }
    }});
  }
}

TEST(BlockCache, CollisionOverwrite) {
  auto collision = CollisionCreator("key", "value", "abc");
  Buffer value;
  // Original key is not found, because it didn't pass key equality check
  EXPECT_EQ(Status::NotFound, collision.driver->lookup(makeHK("key"), value));
}

TEST(BlockCache, SimpleReclaim) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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

  driver->getCounters({[](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(0, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(0, count);
    }
  }});

  // Remove 3 entries from region 0
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[1].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  // Remove 2 entries from region 2
  EXPECT_EQ(Status::Ok, driver->remove(log[33].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[34].key()));

  // Drain all async removes
  driver->drain();
  driver->getCounters({[](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(5 * 1024, count);
    }
  }});

  // lookup this entry from region 0 that will be soon reclaimed
  Buffer val;
  EXPECT_EQ(Status::Ok, driver->lookup(log[4].key(), val));
  EXPECT_EQ(Status::Ok, driver->lookup(log[4].key(), val));

  // Force reclamation on region 0. There are 4 regions and the device
  // was configured to require 1 clean region at all times.
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->drain();

  // Reclaiming region 0 should have bumped down the hole count to
  // 2 remaining (from region 2)
  driver->getCounters({[](folly::StringPiece name, double count,
                          CounterVisitor::CounterType type) {
    if (name == "navy_bc_reinsertions" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(2, count);
    }
    // Initial 4 reclaims and 1 reclaim for region 0
    if (name == "navy_bc_reclaim") {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(2 * 1024, count);
    }
  }});
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.checksum = true;
  // items which are accessed once will be reinserted on reclaim
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allow any number of writes in between and after our expected writes
  EXPECT_CALL(*device, writeImpl(_, _, _, _)).Times(testing::AtLeast(0));

  // Note even tho this item's value is corrupted, we would have aborted
  // the reclaim before we got here. So we will not bump the value checksum
  // error stat on this.
  EXPECT_CALL(*device, writeImpl(0, 16384, _, _))
      .WillOnce(testing::Invoke(
          [&device](uint64_t offset, uint32_t size, const void* data, int) {
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
  driver->getCounters({[](folly::StringPiece name, double count,
                          CounterVisitor::CounterType type) {
    if (name == "navy_bc_reclaim" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(4, count);
    }
  }});

  // Force reclamation on region 0 by allocating region 3. There are 4 regions
  // and the device was configured to require 1 clean region at all times
  {
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
    log.push_back(std::move(e));
  }
  driver->flush();

  // Verify we have one header checksum error and two value checksum errors
  driver->getCounters({[](folly::StringPiece name, double count,
                          CounterVisitor::CounterType type) {
    if (name == "navy_bc_reclaim" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(5, count);
    }
    if (name == "navy_bc_reclaim_entry_header_checksum_errors" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(1, count);
    }
    if (name == "navy_bc_reclaim_value_checksum_errors" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(2, count);
    }
  }});
}

TEST(BlockCache, StackAlloc) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = std::make_unique<MockSingleThreadJobScheduler>();
  auto exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  EXPECT_CALL(*device, writeImpl(0, 16 * 1024, _, _));
  // Although 2k read buffer, shouldn't underflow the region!
  EXPECT_CALL(*device, readImpl(0, 1024, _));
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  EXPECT_CALL(*device, writeImpl(0, 16 * 1024, _, _));
  EXPECT_CALL(*device, readImpl(0, 8192, _));
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  // Lookup log[1]
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // We expect the first three regions to be filled, last region to be
  // reclaimed to be the free region. First three regions will be tracked.
  BufferGen bg;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      CacheEntry e{bg.gen(8), bg.gen(3800)};
      driver->insertAsync(e.key(), e.value(),
                          [](Status status, HashedKey /*key */) {
                            EXPECT_EQ(Status::Ok, status);
                          });
      log.push_back(std::move(e));
      finishAllJobs(*exPtr);
      driver->drain();
    }
  }
  driver->flush();

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

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_reclaim_begin");
  injectPauseSet("pause_reclaim_done");

  // Send insert. Will schedule a reclamation job. We will also track
  // the third region as it had been filled up. We will also expect
  // to evict the first region eventually for the reclaim.
  CacheEntry e{bg.gen(8), bg.gen(1000)};
  EXPECT_EQ(0, exPtr->getQueueSize());
  driver->insertAsync(
      e.key(), e.value(),
      [](Status status, HashedKey /*key */) { EXPECT_EQ(Status::Ok, status); });
  // Insert finds region is full and  puts region for tracking, resets allocator
  // and retries.
  EXPECT_TRUE(exPtr->runFirstIf("insert"));

  // The reclaim should have been started
  EXPECT_TRUE(injectPauseWait("pause_reclaim_begin", 1 /* numThreads */,
                              false /* wakeup */));

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(log[2].key(), value));
  EXPECT_EQ(log[2].value(), value.view());

  // Now, let the eviction thread goes; this would block access to the region
  // but reclaim cannot be done as there is still an active reader outstanding.
  injectPauseClear("pause_reclaim_begin");
  // Wait for 5s to confirm the reclaim has not been done
  EXPECT_FALSE(injectPauseWait("pause_reclaim_done", 1, false, 5000));

  std::thread lookupThread2([&driver, &log] {
    Buffer value2;
    // Can't access region 0: blocked. Will retry until unblocked.
    EXPECT_EQ(Status::NotFound, driver->lookup(log[2].key(), value2));
  });

  // To make sure that the reason for key not found is access block, but not
  // evicted from the index, remove it manually and expect it was found.
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  // Reclaim still fails as the last reader is still outstanding; wait for 5s
  EXPECT_FALSE(injectPauseWait("pause_reclaim_done", 1, false, 5000));

  // Finish read and let evict region 0 entries
  sp.reached(1);

  // Reclaim should have been completed now
  EXPECT_TRUE(injectPauseWait("pause_reclaim_done", 1, true, 5000));

  finishAllJobs(*exPtr);
  driver->drain();
  EXPECT_EQ(Status::NotFound, driver->remove(log[1].key()));
  EXPECT_EQ(Status::NotFound, driver->remove(log[2].key()));

  lookupThread.join();
  lookupThread2.join();

  driver->flush();
}

TEST(BlockCache, DeviceFailure) {
  // Test setup:
  //   - "key1" write fails once but succeeds on retry, read succeeds
  //   - "key2" write succeeds, read fails
  //   - "key3" both read and write succeeds

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  {
    testing::InSequence seq;
    EXPECT_CALL(*device, writeImpl(0, kRegionSize, _, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*device, writeImpl(0, kRegionSize, _, _));
    EXPECT_CALL(*device, writeImpl(kRegionSize, kRegionSize, _, _));
    EXPECT_CALL(*device, writeImpl(kRegionSize * 2, kRegionSize, _, _));

    EXPECT_CALL(*device, readImpl(0, 1024, _));
    EXPECT_CALL(*device, readImpl(kRegionSize, 1024, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*device, readImpl(kRegionSize * 2, 1024, _));
  }

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  BufferGen bg;
  auto value1 = bg.gen(800);
  auto value2 = bg.gen(800);
  auto value3 = bg.gen(800);

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key1"), value1.view()));
  driver->flush();
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key2"), value2.view()));
  driver->flush();
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key3"), value3.view()));
  driver->flush();

  Buffer value;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key1"), value));
  EXPECT_EQ(value1.view(), value.view());
  EXPECT_EQ(Status::DeviceError, driver->lookup(makeHK("key2"), value));
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key3"), value));
  EXPECT_EQ(value3.view(), value.view());
}

TEST(BlockCache, Flush) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

  auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
  EXPECT_CALL(*device, flushImpl());

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  for (uint32_t i = 0; i < 2; i++) {
    EXPECT_CALL(*device, writeImpl(i * 16 * 1024, 16 * 1024, _, _));
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
  auto config = makeConfig(*ex, std::move(policy), *proxyPtr);
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
  proxyPtr->setRealDevice(setupResetTestDevice(kDeviceSize));
  expectRegionsTracked(mp, {0, 1});
  resetTestRun(*driver);
}

TEST(BlockCache, DestructorCallback) {
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
    log.emplace_back(log[0].key(), bg.gen(8'000));
    log.emplace_back(log[3].key(), bg.gen(8'000));
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 9;
  config.destructorCb = toCallback(cb);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  mockRegionsEvicted(mp, {0, 1, 2, 3, 1});

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  // Pause at eviction done
  injectPauseSet("pause_do_eviction_done");

  for (size_t i = 0; i < 7; i++) {
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value()));
  }
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));
  // Next insertion should start the eviction
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

  // Make sure that the eviction for the region 1 is completed
  EXPECT_TRUE(injectPauseWait("pause_do_eviction_done"));
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // Allocator region fills every 7 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 7; i++) {
      CacheEntry e{bg.gen(8), bg.gen(1800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Triggers reclamation
  for (size_t i = 0; i < 7; i++) {
    CacheEntry e{bg.gen(8), bg.gen(1800)};
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // We don't fill up a region so won't track any. But we will first evict
  // rid: 0 for the two allocations, and we will evict rid: 1 in order to
  // satisfy the one free region requirement.
  expectRegionsTracked(mp, {0});
  BufferGen bg;
  for (size_t i = 0; i < 2; i++) {
    CacheEntry e{bg.gen(8), bg.gen(1800)};
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
  expectRegionsTracked(mp, {0, 1, 2});
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 7; i++) {
      CacheEntry e{bg.gen(8), bg.gen(1800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    driver->flush();
  }

  // Triggers reclamation
  expectRegionsTracked(mp, {3});
  for (size_t i = 0; i < 7; i++) {
    CacheEntry e{bg.gen(8), bg.gen(1800)};
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
  uint32_t ioAlignSize = 4096;
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto& mp = *policy;
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  // Create MemoryDevice with ioAlignSize{4096} allows Header to fit in.
  auto device =
      createMemoryDevice(deviceSize, nullptr /* encryption */, ioAlignSize);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 2;
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  expectRegionsTracked(mp, {0, 1, 2});
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

  driver->flush();
  driver->persist();

  {
    testing::InSequence s;
    EXPECT_CALL(mp, reset());
    expectRegionsTracked(mp, {0, 1, 2, 3});
    EXPECT_CALL(mp, reset());
    expectRegionsTracked(mp, {3, 0, 1, 2, 3});
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
  std::vector<uint32_t> hits(4);
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
  auto ex = makeJobScheduler();

  std::vector<std::string> keys;
  folly::IOBufQueue originalMetadata;
  {
    auto config =
        makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits), *device);
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
    engine->flush();

    for (auto& key : keys) {
      Buffer buffer;
      ASSERT_EQ(Status::Ok,
                engine->lookup(
                    makeHK(BufferView{key.size(),
                                      reinterpret_cast<uint8_t*>(key.data())}),
                    buffer));
    }

    auto rw = createMemoryRecordWriter(originalMetadata);
    engine->persist(*rw);
  }

  // Recover with the right size
  {
    auto config =
        makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits), *device);
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

  // Creat engine with larger size
  {
    auto config =
        makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits), *device);
    config.cacheSize += 4096;
    ASSERT_THROW(makeEngine(std::move(config), metadataSize),
                 std::invalid_argument);
  }
}

// This test does the following
// 1. Test creation of BlockCache with BlockCache::kMinAllocAlignSize aligned
//    slot sizes fail when in memory buffers are not enabled
// 2. Test the following order of operations succeed when in memory buffers
//    are enabled and slot sizes are BlockCache::kMinAllocAlignSize aligned
//    * insert 12 items in two different regions
//    * lookup the 12 items to make sure they are in the cache
//    * flush the device
//    * lookup again to make sure the 12 items still exist
//    * persist the cache
//    * recover the cache
//    * lookup the 12 items again to make sure they still exist
//
TEST(BlockCache, SmallerSlotSizes) {
  std::vector<uint32_t> hits(4);
  uint32_t ioAlignSize = 4096;

  {
    auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

    size_t metadataSize = 3 * 1024 * 1024;
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    try {
      auto engine = makeEngine(std::move(config), metadataSize);
    } catch (const std::invalid_argument& e) {
      EXPECT_EQ(e.what(), std::string("invalid size class: 3072"));
    }
  }
  const uint64_t myDeviceSize = 16 * 1024 * 1024;
  // Create MemoryDevice with ioAlignSize{4096} allows Header to fit in.
  auto device =
      createMemoryDevice(myDeviceSize, nullptr /* encryption */, ioAlignSize);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  size_t metadataSize = 3 * 1024 * 1024;
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device, myDeviceSize);
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

TEST(BlockCache, HoleStatsRecovery) {
  std::vector<uint32_t> hits(4);
  uint32_t ioAlignSize = 4096;
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  // Create MemoryDevice with ioAlignSize{4096} allows Header to fit in.
  auto device =
      createMemoryDevice(deviceSize, nullptr /* encryption */, ioAlignSize);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config), metadataSize);
  auto driver = makeDriver(std::move(engine), std::move(ex), std::move(device),
                           metadataSize);

  BufferGen bg;
  std::vector<CacheEntry> log;
  // Allocate 3 regions
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 4; j++) {
      CacheEntry e{bg.gen(8), bg.gen(3800)};
      EXPECT_EQ(Status::Ok, driver->insert(e.key(), e.value()));
      log.push_back(std::move(e));
    }
  }

  // Remove some entries
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[1].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[5].key()));
  EXPECT_EQ(Status::Ok, driver->remove(log[8].key()));

  CounterVisitor validationFunctor{[](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(4, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(4 * 4096, count);
    }
  }};

  driver->getCounters(validationFunctor);
  driver->persist();
  driver->reset();
  driver->getCounters({[](folly::StringPiece name, double count) {
    if (name == "navy_bc_hole_count") {
      EXPECT_EQ(0, count);
    }
    if (name == "navy_bc_hole_bytes") {
      EXPECT_EQ(0, count);
    }
  }});
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
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto engine = makeEngine(std::move(config), metadataSize);
    auto driver = makeDriver(std::move(engine), std::move(ex),
                             std::move(device), metadataSize);

    expectRegionsTracked(mp, {0, 1, 2});
    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t i = 0; i < 3; i++) {
      for (size_t j = 0; j < 4; j++) {
        CacheEntry e{bg.gen(8), bg.gen(3200)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
        log.push_back(std::move(e));
      }
      driver->flush();
    }

    driver->persist();
  }

  {
    std::vector<uint32_t> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    size_t metadataSize = 3 * 1024 * 1024;
    auto deviceSize = metadataSize + kDeviceSize;
    auto device = createMemoryDevice(deviceSize, nullptr /* encryption */);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
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
    auto config = makeConfig(*ex, std::move(policy), *device);
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
    auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.reinsertionConfig = makeHitsReinsertionConfig(1);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  folly::fibers::TimedMutex mutex;
  bool reclaimStarted = false;
  size_t numCleanRegions = 0;
  util::ConditionVariable cv;

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  // In this test case, it is important to prevent the reinsertions are
  // not skipped due to allocation failures. To do so, we need to
  // guarantee that the insertions (from this test case) do not race
  // for the free space in the region against the reinsertions for reclaim.
  // This can be achieved by making sure that the insertion proceeds with the
  // allocation only if there is no outstanding reclaim; i.e., there should be
  // at least one clean region reserved for the reclaim
  injectPauseSet("pause_blockcache_clean_alloc_locked", [&]() {
    std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
    XDCHECK_GT(numCleanRegions, 0u);
    numCleanRegions--;
  });

  injectPauseSet("pause_blockcache_clean_free_locked", [&]() {
    std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
    if (numCleanRegions++ == 0u) {
      cv.notifyAll();
    }
    reclaimStarted = true;
  });

  injectPauseSet("pause_blockcache_insert_entry", [&]() {
    std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
    if (numCleanRegions == 0u && reclaimStarted) {
      cv.wait(lk);
    }
  });

  // Allocator region fills every 16 inserts.
  BufferGen bg;
  std::vector<CacheEntry> log;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      CacheEntry e{bg.gen(8), bg.gen(800)};
      EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), nullptr));
      log.push_back(std::move(e));
    }
    // flush region such that the regions are closed
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

  // Drain all insert requests
  driver->drain();

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
  uint32_t ioAlignSize = 4096;
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  // Create MemoryDevice with ioAlignSize{4096} allows Header to fit in.
  auto device =
      createMemoryDevice(deviceSize, nullptr /* encryption */, ioAlignSize);
  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
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

  driver->flush();
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
  auto config = makeConfig(*ex, std::move(policy), *device, kRegionSize * 6);
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
  auto config = makeConfig(*ex, std::move(policy), *device, kRegionSize * 6);
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
  EXPECT_CALL(*device, writeImpl(_, _, _, _)).WillRepeatedly(Return(false));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 4;
  config.inMemBufFlushRetryLimit = kFlushRetryLimit;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_blockcache_insert_done");

  BufferGen bg;
  CacheEntry e{bg.gen(8), bg.gen(800)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}));
  // Make sure that the insertion is scheduled and allocated a clean region
  EXPECT_TRUE(injectPauseWait("pause_blockcache_insert_done"));

  // Issue flush sync
  driver->flush();

  // Flush should have failed due to the injected write failures
  driver->getCounters({[](folly::StringPiece name, double count,
                          CounterVisitor::CounterType type) {
    if (name == "navy_bc_inmem_flush_retries" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(kFlushRetryLimit, count);
    }
    if (name == "navy_bc_inmem_flush_failures" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(1, count);
    }
  }});
}

TEST(BlockCache, DeviceFlushFailureAsync) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = std::make_unique<MockDevice>(kDeviceSize, 1024);

  testing::InSequence inSeq;
  EXPECT_CALL(*device, writeImpl(_, _, _, _)).WillRepeatedly(Return(false));

  auto ex = makeJobScheduler();
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 4;
  config.inMemBufFlushRetryLimit = kFlushRetryLimit;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  ENABLE_INJECT_PAUSE_IN_SCOPE();

  injectPauseSet("pause_blockcache_insert_done");
  injectPauseSet("pause_flush_failure");

  BufferGen bg;
  CacheEntry e{bg.gen(8), bg.gen(15 * 1024)};
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}));
  // Make sure that the insertion is scheduled and allocated a clean region
  EXPECT_TRUE(injectPauseWait("pause_blockcache_insert_done"));

  // The region size is 16KB, so the second insertions should allocate
  // a new region and start a flush async for the first region
  EXPECT_EQ(Status::Ok, driver->insertAsync(e.key(), e.value(), {}));
  EXPECT_TRUE(injectPauseWait("pause_flush_failure"));
  EXPECT_TRUE(injectPauseWait("pause_blockcache_insert_done"));

  // Now run another flush sync; pause point should be cleared
  injectPauseClear("pause_flush_failure");
  driver->flush();

  // Flush should have failed due to the injected write failures
  driver->getCounters({[](folly::StringPiece name, double count,
                          CounterVisitor::CounterType type) {
    if (name == "navy_bc_inmem_flush_retries" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(kFlushRetryLimit * 2, count);
    }
    if (name == "navy_bc_inmem_flush_failures" &&
        type == CounterVisitor::CounterType::RATE) {
      EXPECT_EQ(2, count);
    }
  }});
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
    log.emplace_back(log[0].key(), bg.gen(8'000));
    log.emplace_back(log[3].key(), bg.gen(8'000));
    // 4th region, 15k
    log.emplace_back(Buffer{makeView("key_007")}, bg.gen(9'000));
    log.emplace_back(Buffer{makeView("key_008")}, bg.gen(6'000));
    ASSERT_EQ(9, log.size());
  }

  MockDestructor cb;
  ON_CALL(cb, call(_, _, _))
      .WillByDefault(
          Invoke([](HashedKey key, BufferView val, DestructorEvent event) {
            XLOGF(ERR, "cb key: {}, val: {}, event: {}", key.key(),
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
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 9;
  config.destructorCb = toCallback(cb);
  config.itemDestructorEnabled = true;
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  mockRegionsEvicted(mp, {0, 1, 2, 3, 1});
  for (size_t i = 0; i < 7; i++) {
    XLOG(ERR, "insert ") << log[i].key().key();
    EXPECT_EQ(Status::Ok, driver->insert(log[i].key(), log[i].value()));
  }

  // remove with cb triggers destructor Immediately
  XLOG(ERR, "remove ") << log[2].key().key();
  EXPECT_EQ(Status::Ok, driver->remove(log[2].key()));

  // remove with cb triggers destructor Immediately
  XLOG(ERR, "remove ") << log[0].key().key();
  EXPECT_EQ(Status::Ok, driver->remove(log[0].key()));

  // remove again
  EXPECT_EQ(Status::NotFound, driver->remove(log[2].key()));
  EXPECT_EQ(Status::NotFound, driver->remove(log[0].key()));

  XLOG(ERR, "insert ") << log[7].key().key();
  EXPECT_EQ(Status::Ok, driver->insert(log[7].key(), log[7].value()));
  // insert will trigger evictions
  XLOG(ERR, "insert ") << log[8].key().key();
  EXPECT_EQ(Status::Ok, driver->insert(log[8].key(), log[8].value()));

  Buffer value;
  EXPECT_EQ(Status::NotFound, driver->lookup(log[0].key(), value));
  EXPECT_EQ(Status::NotFound, driver->lookup(log[5].key(), value));

  exPtr->finish();
}

TEST(BlockCache, RandomAlloc) {
  std::unordered_map<std::string, CacheEntry> log;
  SeqPoints sp;

  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  auto device = createMemoryDevice(kDeviceSize, nullptr /* encryption */);
  auto ex = std::make_unique<MockJobScheduler>();
  auto exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device);
  auto engine = makeEngine(std::move(config));
  auto driver = makeDriver(std::move(engine), std::move(ex));

  // We expect the first three regions to be filled
  BufferGen bg;
  for (size_t j = 0; j < 3; j++) {
    for (size_t i = 0; i < 4; i++) {
      auto key = folly::sformat("{}:{}", j, i);
      CacheEntry e{makeHK(key.c_str()), bg.gen(3800)};
      driver->insertAsync(e.key(), e.value(),
                          [](Status status, HashedKey /*key */) {
                            EXPECT_EQ(Status::Ok, status);
                          });
      log.emplace(key, std::move(e));
      finishAllJobs(*exPtr);
    }
  }
  driver->flush();

  size_t succ_cnt = 0;
  std::unordered_map<std::string, size_t> getCnts;
  static constexpr size_t loopCnt = 10000;
  for (size_t i = 0; i < loopCnt; i++) {
    Buffer value;
    auto [status, keyStr] = driver->getRandomAlloc(value);
    if (status != navy::Status::Ok) {
      continue;
    }
    succ_cnt++;
    getCnts[keyStr]++;
    auto it = log.find(keyStr);
    EXPECT_NE(it, log.end());
    EXPECT_EQ(it->second.value(), value.view());
  }
  std::vector<size_t> cnts;
  std::transform(
      getCnts.begin(), getCnts.end(), std::back_inserter(cnts),
      [](const std::pair<std::string, size_t>& p) { return p.second; });
  auto [avg, stddev] = util::getMeanDeviation(cnts);

  // Expected success rate is 3 regions / 4 regions
  // Allow margins of upto 20% for success rate and
  // 20% for deviation of each item
  EXPECT_GT(succ_cnt, (size_t)((double)loopCnt * 3.0 * 0.8 / 4.0));
  EXPECT_LT(succ_cnt, (size_t)((double)loopCnt * 3.0 * 1.2 / 4.0));
  EXPECT_LT(stddev, avg * 0.2);
}

// Test size alignment calculations on an alignment other than the default (512
// on less than 2TB device size)
TEST(BlockCache, SizeAndAlignment) {
  std::vector<uint32_t> hits(4);
  auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
  // Make the device of 2TB so that we need 1024 bytes alignmetn on block cache.
  const uint32_t alignSize = 1024;
  auto device = std::make_unique<SizeMockDevice>(
      alignSize * (static_cast<uint64_t>(1) << 32));
  auto ex = makeJobScheduler();
  // auto* exPtr = ex.get();
  auto config = makeConfig(*ex, std::move(policy), *device);
  config.numInMemBuffers = 9;

  config.itemDestructorEnabled = true;
  auto engine = makeEngine(std::move(config));
  BufferGen bg;
  auto smallValue = bg.gen(16);
  EXPECT_EQ(engine->estimateWriteSize(HashedKey{"key"}, smallValue.view()),
            alignSize);

  // assumption: the item descriptor size is 24.
  // Make an item at the size of 1024.
  auto largeValue = bg.gen(alignSize - 24 - 3);
  EXPECT_EQ(engine->estimateWriteSize(HashedKey{"key"}, largeValue.view()),
            alignSize);

  // Add one more byte and need 2*alignSize
  auto hugeValue = bg.gen(alignSize - 24 - 3 + 1);
  EXPECT_EQ(engine->estimateWriteSize(HashedKey{"key"}, hugeValue.view()),
            alignSize * 2);
}
} // namespace facebook::cachelib::navy::tests
