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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <unordered_map>

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/driver/Driver.h"
#include "cachelib/navy/driver/NoopEngine.h"
#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"
#include "cachelib/navy/testing/MockJobScheduler.h"
#include "cachelib/navy/testing/SeqPoints.h"

using testing::_;
using testing::Invoke;
using testing::Return;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
constexpr uint32_t kSmallItemMaxSize{32};

struct HashedKeyHash {
  uint64_t operator()(HashedKey hk) const { return hk.keyHash(); }
};

class MockEngine final : public Engine {
 public:
  explicit MockEngine(std::string name = "", MockDestructor* cb = nullptr)
      : name_(name),
        destructorCb_{
            [cb](HashedKey key, BufferView value, DestructorEvent event) {
              if (cb) {
                cb->call(key, value, event);
              }
            }} {
    ON_CALL(*this, insert(_, _))
        .WillByDefault(Invoke([this](HashedKey hk, BufferView value) {
          auto entry =
              std::make_pair(Buffer{makeView(hk.key())}, Buffer{value});
          auto entryHK = makeHK(entry.first); // Capture before std::move
          cache_[entryHK] = std::move(entry);
          return Status::Ok;
        }));
    ON_CALL(*this, lookup(_, _))
        .WillByDefault(Invoke([this](HashedKey hk, Buffer& value) {
          auto itr = cache_.find(hk);
          if (itr == cache_.end()) {
            return Status::NotFound;
          }
          value = itr->second.second.copy();
          return Status::Ok;
        }));
    ON_CALL(*this, couldExist(_)).WillByDefault(Invoke([this](HashedKey hk) {
      auto itr = cache_.find(hk);
      if (itr == cache_.end()) {
        return false;
      }
      return true;
    }));

    ON_CALL(*this, remove(_)).WillByDefault(Invoke([this](HashedKey hk) {
      return evict(hk) ? Status::Ok : Status::NotFound;
    }));
    ON_CALL(*this, flush()).WillByDefault(Return());
    ON_CALL(*this, reset()).WillByDefault(Return());
    ON_CALL(*this, mockPersistData()).WillByDefault(Return(""));
    ON_CALL(*this, mockRecoverData(_)).WillByDefault(Return(true));
  }

  ~MockEngine() override = default;

  MOCK_METHOD2(insert, Status(HashedKey hk, BufferView value));
  MOCK_METHOD2(lookup, Status(HashedKey hk, Buffer& value));
  MOCK_METHOD1(couldExist, bool(HashedKey hk));
  MOCK_METHOD1(remove, Status(HashedKey hk));

  MOCK_METHOD0(flush, void());
  MOCK_METHOD0(reset, void());

  void persist(RecordWriter& rw) override {
    mockPersistData();
    rw.writeRecord(folly::IOBuf::copyBuffer(name_));
  }

  bool recover(RecordReader& rr) override {
    auto buf = rr.readRecord();
    return mockRecoverData(
        std::string{reinterpret_cast<const char*>(buf->data()), buf->length()});
  }

  MOCK_METHOD0(mockPersistData, std::string());
  MOCK_METHOD1(mockRecoverData, bool(const std::string&));

  void getCounters(const CounterVisitor& /* visitor */) const override {}
  uint64_t getMaxItemSize() const override { return UINT32_MAX; }

  // Returns true if key found and can be actually evicted in the real world
  bool evict(HashedKey key) {
    auto itr = cache_.find(key);
    if (itr == cache_.end()) {
      return false;
    }
    auto value = std::move(itr->second.second);
    cache_.erase(itr);
    if (destructorCb_) {
      destructorCb_(key, value.view(), DestructorEvent::Removed);
    }
    return true;
  }

 private:
  // (key, value) pair
  using EntryType = std::pair<Buffer, Buffer>;
  std::string name_;
  const DestructorCallback destructorCb_{};
  std::unordered_map<HashedKey, EntryType, HashedKeyHash> cache_;
};

std::unique_ptr<JobScheduler> makeJobScheduler() {
  return std::make_unique<MockSingleThreadJobScheduler>();
}

Driver::Config makeDriverConfig(std::unique_ptr<Engine> bc,
                                std::unique_ptr<Engine> si,
                                std::unique_ptr<JobScheduler> ex) {
  constexpr uint64_t kDeviceSize{64 * 1024};
  uint32_t ioAlignSize = 4096;
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  Driver::Config config;
  config.scheduler = std::move(ex);
  config.largeItemCache = std::move(bc);
  // Create MemoryDevice with ioAlignSize{4096} allows Header to fit in.
  config.device =
      createMemoryDevice(deviceSize, nullptr /* encryption */, ioAlignSize);
  config.smallItemMaxSize = kSmallItemMaxSize;
  config.smallItemCache = std::move(si);
  config.metadataSize = metadataSize;
  return config;
}
} // namespace

// test that the behavior of could exist works as expected when one of the
// engine is a no-op engine.
void testCouldExistWithOneEngine(bool small) {
  BufferGen bg;
  auto value = bg.gen(kSmallItemMaxSize + (small ? -5 : 5));

  auto ex = makeJobScheduler();
  auto exPtr = ex.get();

  std::unique_ptr<Engine> li;
  std::unique_ptr<Engine> si;

  if (small) {
    li = std::make_unique<NoopEngine>();
    auto s = std::make_unique<MockEngine>();
    {
      testing::InSequence inSeq;
      // current implementation always checks the small item engine first,
      // before checking large item engine.
      EXPECT_CALL(*s, couldExist(makeHK("key")));
      EXPECT_CALL(*s, couldExist(makeHK("key")));
      EXPECT_CALL(*s, couldExist(makeHK("key1")));
    }

    si = std::move(s);
  } else {
    si = std::make_unique<NoopEngine>();
    auto l = std::make_unique<MockEngine>();
    {
      testing::InSequence inSeq;
      // current implementation always checks the small item engine first,
      // before checking large item engine.
      EXPECT_CALL(*l, couldExist(makeHK("key")));
      EXPECT_CALL(*l, couldExist(makeHK("key")));
      EXPECT_CALL(*l, couldExist(makeHK("key1")));
    }

    li = std::move(l);
  }

  auto config = makeDriverConfig(std::move(li), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));
  EXPECT_FALSE(driver->couldExist(makeHK("key")));

  // Test async insert for small item cache
  auto valCopy = Buffer{value.view()};
  auto valView = valCopy.view();
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), valView));
  exPtr->finish();
  EXPECT_TRUE(driver->couldExist(makeHK("key")));
  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_FALSE(driver->couldExist(makeHK("key1")));

  uint32_t numLookups{std::numeric_limits<uint32_t>::max()};
  uint32_t numLookupSuccess{std::numeric_limits<uint32_t>::max()};
  driver->getCounters([&](folly::StringPiece name, double value) {
    if (name == "navy_lookups") {
      numLookups = static_cast<uint32_t>(value);

    } else if (name == "navy_succ_lookups") {
      numLookupSuccess = static_cast<uint32_t>(value);
    }
  });
  EXPECT_EQ(3, numLookups);
  EXPECT_EQ(1, numLookupSuccess);
}

TEST(Driver, CouldExistSmallWithNoopEngine) {
  testCouldExistWithOneEngine(true);
}
TEST(Driver, CouldExistLargeWithNoopEngine) {
  testCouldExistWithOneEngine(false);
}

TEST(Driver, SmallItem) {
  BufferGen bg;
  auto value = bg.gen(16);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    // current implementation always checks the small item engine first,
    // before checking large item engine.
    EXPECT_CALL(*si, couldExist(makeHK("key")));
    EXPECT_CALL(*bc, couldExist(makeHK("key")));
    EXPECT_CALL(*si, insert(makeHK("key"), value.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto exPtr = ex.get();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  MockInsertCB cbInsert;
  EXPECT_CALL(cbInsert, call(Status::Ok, makeHK("key")));

  EXPECT_FALSE(driver->couldExist(makeHK("key")));

  // Test async insert for small item cache
  auto valCopy = Buffer{value.view()};
  auto valView = valCopy.view();
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(
                makeHK("key"),
                valView,
                [&cbInsert, v = std::move(valCopy)](
                    Status status, HashedKey k) { cbInsert.call(status, k); }));
  exPtr->finish();

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(value.view(), valueLookup.view());
}

TEST(Driver, LargeItem) {
  BufferGen bg;
  auto value = bg.gen(32);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, insert(makeHK("key"), value.view()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto exPtr = ex.get();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  MockInsertCB cbInsert;
  EXPECT_CALL(cbInsert, call(Status::Ok, makeHK("key")));

  // Test async insert for large item cache
  auto valCopy = Buffer{value.view()};
  auto valView = valCopy.view();
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(
                makeHK("key"),
                valView,
                [&cbInsert, v = std::move(valCopy)](
                    Status status, HashedKey k) { cbInsert.call(status, k); }));
  exPtr->finish();

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(value.view(), valueLookup.view());
}

TEST(Driver, SmallAndLargeItem) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), largeValue.view()));
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(smallValue.view(), valueLookup.view());
}

TEST(Driver, InsertFailed) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  // Insert a small item. Then insert a large one. Simulate large failed,
  // device error reported. Small is still available.
  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()))
        .WillOnce(Return(Status::DeviceError));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));
  EXPECT_EQ(Status::DeviceError,
            driver->insert(makeHK("key"), largeValue.view()));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(smallValue.view(), valueLookup.view());
}

TEST(Driver, InsertFailedRemoveOther) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  // Insert a small item. Then insert a large one. Insert succeeds, but remove
  // of the small one fails. Driver reports bad state.
  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()));
    EXPECT_CALL(*si, remove(makeHK("key")))
        .WillOnce(Return(Status::DeviceError));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));
  EXPECT_EQ(Status::BadState, driver->insert(makeHK("key"), largeValue.view()));

  // We don't provide any guarantees what is available. But in our test we
  // can check what is visible.
  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(largeValue.view(), valueLookup.view());
}

TEST(Driver, Remove) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
    // test retry
    EXPECT_CALL(*si, remove(makeHK("test retry")))
        .WillOnce(Return(Status::NotFound));
    EXPECT_CALL(*bc, remove(makeHK("test retry")))
        .WillOnce(Return(Status::Retry))
        .WillOnce(Return(Status::Ok));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), largeValue.view()));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(largeValue.view(), valueLookup.view());

  EXPECT_EQ(Status::Ok, driver->remove(makeHK("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeHK("key"), valueLookup));

  EXPECT_EQ(Status::Ok, driver->remove(makeHK("test retry")));
}

// Comment about EvictionBlockCache and EvictionSmallItemCache:
//
// Our guarantee is that "for every insert user will get eviction callback".
// In these tests it looks like broken: eviction callback is not called for
// ovewritten items. This is mock test limitation. Block cache will call it
// on region eviction. Small item cache (BigHash) will call it immediately on
// remove.
//
// Because of the note above:
// TODO: T95788512 Eviction test with real engine when both ready

TEST(Driver, EvictBlockCache) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  MockDestructor ecbBC;
  EXPECT_CALL(ecbBC,
              call(makeHK("key"), largeValue.view(), DestructorEvent::Removed));
  auto bc = std::make_unique<MockEngine>("", &ecbBC);
  auto bcPtr = bc.get();

  MockDestructor ecbSI;
  // Nothing should be called
  auto si = std::make_unique<MockEngine>("", &ecbSI);
  auto siPtr = si.get();

  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), largeValue.view()));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(largeValue.view(), valueLookup.view());

  // If we inserted in block cache, eviction from small cache should not
  // happen, because it is removed.
  EXPECT_FALSE(siPtr->evict(makeHK("key")));

  // But it can be evicted from block cache
  EXPECT_TRUE(bcPtr->evict(makeHK("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeHK("key"), valueLookup));
}

TEST(Driver, EvictSmallItemCache) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  MockDestructor ecbBC;
  auto bc = std::make_unique<MockEngine>("", &ecbBC);
  // Nothing should be called
  auto bcPtr = bc.get();

  MockDestructor ecbSI;
  EXPECT_CALL(ecbSI,
              call(makeHK("key"), smallValue.view(), DestructorEvent::Removed));
  auto si = std::make_unique<MockEngine>("", &ecbSI);
  auto siPtr = si.get();

  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), largeValue.view()));
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(smallValue.view(), valueLookup.view());

  EXPECT_FALSE(bcPtr->evict(makeHK("key")));

  EXPECT_TRUE(siPtr->evict(makeHK("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeHK("key"), valueLookup));
}

TEST(Driver, Recovery) {
  auto bc = std::make_unique<MockEngine>("block cache data");
  auto si = std::make_unique<MockEngine>("small cache data");

  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, mockPersistData()).WillOnce(Return("block cache data"));
    EXPECT_CALL(*si, mockPersistData()).WillOnce(Return("small cache data"));
    EXPECT_CALL(*bc, mockRecoverData("block cache data"));
    EXPECT_CALL(*si, mockRecoverData("small cache data"));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));
  driver->persist();
  EXPECT_TRUE(driver->recover());
}

TEST(Driver, RecoveryError) {
  auto bc = std::make_unique<MockEngine>("block cache data");
  auto si = std::make_unique<MockEngine>("small cache data");
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, mockPersistData()).WillOnce(Return("block cache data"));
    EXPECT_CALL(*si, mockPersistData()).WillOnce(Return("small cache data"));
    EXPECT_CALL(*bc, mockRecoverData("block cache data"));
    EXPECT_CALL(*si, mockRecoverData("small cache data"))
        .WillOnce(Return(false));
    EXPECT_CALL(*si, reset());
    EXPECT_CALL(*bc, reset());
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  driver->persist();
  EXPECT_FALSE(driver->recover());
}

TEST(Driver, ConcurrentInserts) {
  SeqPoints sp;
  sp.setName(0, "first insert started");
  sp.setName(1, "second insert started");
  sp.setName(2, "inserts finished");

  auto bc = std::make_unique<MockEngine>();
  EXPECT_CALL(*bc, insert(makeHK("1"), makeView("v1")))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(0);
        sp.wait(2);
        return Status::Ok;
      }));
  EXPECT_CALL(*bc, insert(makeHK("2"), makeView("v2")))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(1);
        sp.wait(2);
        return Status::Ok;
      }));

  auto ex = std::make_unique<ThreadPoolJobScheduler>(1, 10);
  auto config = makeDriverConfig(std::move(bc), nullptr, std::move(ex));
  config.maxConcurrentInserts = 2;
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insertAsync(makeHK("1"), makeView("v1"), {}));
  EXPECT_EQ(Status::Ok, driver->insertAsync(makeHK("2"), makeView("v2"), {}));

  sp.wait(0);
  sp.wait(1);
  EXPECT_EQ(Status::Rejected,
            driver->insertAsync(makeHK("3"), makeView("v3"), {}));
  sp.reached(2);

  uint32_t statRejected = 0;
  driver->getCounters([&statRejected](folly::StringPiece name, double value) {
    if (name == "navy_rejected_concurrent_inserts") {
      statRejected = static_cast<uint32_t>(value);
    }
  });
  EXPECT_EQ(1, statRejected);
}

TEST(Driver, ParcelMemory) {
  SeqPoints sp;

  BufferGen bg;
  auto v1 = bg.gen(1000);
  auto v2 = bg.gen(1500);
  auto v3 = bg.gen(500); // Even 500 wouldn't find because of keys
  auto bc = std::make_unique<MockEngine>();
  EXPECT_CALL(*bc, insert(makeHK("1"), v1.view()))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(0);
        sp.wait(2);
        return Status::Ok;
      }));
  EXPECT_CALL(*bc, insert(makeHK("2"), v2.view()))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(1);
        sp.wait(3);
        return Status::Ok;
      }));
  EXPECT_CALL(*bc, insert(makeHK("3"), v3.view()));

  auto ex = std::make_unique<ThreadPoolJobScheduler>(1, 10);
  auto config = makeDriverConfig(std::move(bc), nullptr, std::move(ex));
  config.maxParcelMemory = 3000;
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insertAsync(makeHK("1"), v1.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insertAsync(makeHK("2"), v2.view(), {}));
  sp.wait(0);
  sp.wait(1);

  EXPECT_EQ(Status::Rejected, driver->insertAsync(makeHK("3"), v3.view(), {}));

  sp.reached(2);
  // There is some gap between we unblock callback and counters go down. Keep
  // counting rejects.
  uint32_t rejects = 1;
  Status st;
  while ((st = driver->insertAsync(makeHK("3"), v3.view(), {})) ==
         Status::Rejected) {
    rejects++;
    std::this_thread::yield();
  }
  sp.reached(3);

  uint32_t statRejected = 0;
  driver->getCounters([&statRejected](folly::StringPiece name, double value) {
    if (name == "navy_rejected_parcel_memory") {
      statRejected = static_cast<uint32_t>(value);
    }
  });
  EXPECT_EQ(rejects, statRejected);
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
