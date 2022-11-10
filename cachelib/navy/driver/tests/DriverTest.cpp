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

#include <unordered_map>

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/driver/Driver.h"
#include "cachelib/navy/engine/NoopEngine.h"
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
namespace {
constexpr uint32_t kSmallItemMaxSize{32};

struct HashedKeyHash {
  uint64_t operator()(HashedKey hk) const { return hk.keyHash(); }
};

class MockEngine final : public Engine {
 public:
  explicit MockEngine(std::string name = "",
                      MockDestructor* cb = nullptr,
                      uint64_t itemMaxSize = UINT32_MAX)
      : name_(name),
        destructorCb_{
            [cb](HashedKey key, BufferView value, DestructorEvent event) {
              if (cb) {
                cb->call(key, value, event);
              }
            }},
        itemMaxSize_(itemMaxSize) {
    ON_CALL(*this, insert(_, _))
        .WillByDefault(Invoke([this](HashedKey hk, BufferView value) {
          auto keybuffer = Buffer{makeView(hk.key())};
          auto valbuffer = Buffer{value};
          auto entry =
              std::make_pair(std::move(keybuffer), std::move(valbuffer));
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

  uint64_t getSize() const override { return UINT32_MAX; }

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
  uint64_t getMaxItemSize() const override { return itemMaxSize_; }
  std::pair<Status, std::string /* key */> getRandomAlloc(
      Buffer& value) override {
    (void)value;
    return std::make_pair(Status::NotFound, "");
  }

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
  const uint64_t itemMaxSize_;
};

std::unique_ptr<JobScheduler> makeJobScheduler() {
  return std::make_unique<MockSingleThreadJobScheduler>();
}

EnginePair makeEnginePair(JobScheduler* scheduler,
                          std::unique_ptr<Engine> largeItemCache = nullptr,
                          std::unique_ptr<Engine> smallItemCache = nullptr,
                          uint32_t smallItemMaxSize = kSmallItemMaxSize) {
  return EnginePair{std::move(smallItemCache), std::move(largeItemCache),
                    smallItemMaxSize, scheduler};
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

  auto p = makeEnginePair(config.scheduler.get(), std::move(bc), std::move(si));

  // Create MemoryDevice with ioAlignSize{4096} allows Header to fit in.
  config.device =
      createMemoryDevice(deviceSize, nullptr /* encryption */, ioAlignSize);
  config.smallItemMaxSize = kSmallItemMaxSize;
  config.enginePairs.push_back(std::move(p));
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
  driver->getCounters({[&](folly::StringPiece name, double value,
                           util::CounterVisitor::CounterType type) {
    if (name == "navy_lookups" &&
        type == util::CounterVisitor::CounterType::RATE) {
      numLookups = static_cast<uint32_t>(value);

    } else if (name == "navy_succ_lookups" &&
               type == util::CounterVisitor::CounterType::RATE) {
      numLookupSuccess = static_cast<uint32_t>(value);
    }
  }});
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

TEST(Driver, InsertRetryRemoveOther) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  // Insert a large item, then insert a small item with the same key.
  // The small item engine is configured to retry on the first remove.

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view()));
    EXPECT_CALL(*si, remove(makeHK("key")));

    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view()));
    EXPECT_CALL(*bc, remove(makeHK("key")))
        .WillOnce(Return(Status::Retry))
        .WillRepeatedly(testing::DoDefault());
    ;

    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  MockJobScheduler* exPtr = static_cast<MockJobScheduler*>(ex.get());
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), largeValue.view()));
  EXPECT_EQ(exPtr->getRescheduleCount(), 0);
  EXPECT_EQ(exPtr->getDoneCount(), 1);

  // The returned status code is Ok because it's not rejected by admission test.
  // Under the hood, the schedule went through one reschedule and one succeed.
  EXPECT_EQ(Status::Ok, driver->insert(makeHK("key"), smallValue.view()));
  EXPECT_EQ(exPtr->getRescheduleCount(), 1);
  EXPECT_EQ(exPtr->getDoneCount(), 2);

  Buffer valueLookup;
  // Look up the key, which now only exist in small item engine.
  // Both engines will be queried.
  EXPECT_EQ(Status::Ok, driver->lookup(makeHK("key"), valueLookup));
  EXPECT_EQ(smallValue.view(), valueLookup.view());
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
  driver->getCounters({[&statRejected](folly::StringPiece name, double value,
                                       CounterVisitor::CounterType type) {
    if (name == "navy_rejected_concurrent_inserts" &&
        type == CounterVisitor::CounterType::RATE) {
      statRejected = static_cast<uint32_t>(value);
    }
  }});
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
  driver->getCounters({[&statRejected](folly::StringPiece name, double value,
                                       CounterVisitor::CounterType type) {
    if (name == "navy_rejected_parcel_memory" &&
        type == CounterVisitor::CounterType::RATE) {
      statRejected = static_cast<uint32_t>(value);
    }
  }});
  EXPECT_EQ(rejects, statRejected);
}

TEST(Driver, EnginePairSetupErrors) {
  {
    // No engine pairs. Should not happen if created via NavyConfig.
    auto ex = makeJobScheduler();
    auto config = makeDriverConfig(nullptr, nullptr, std::move(ex));
    config.enginePairs.pop_back();
    EXPECT_THROW(std::make_unique<Driver>(std::move(config)),
                 std::invalid_argument);
  }

  {
    // Two pairs, no engine selector.
    auto ex = makeJobScheduler();
    auto config = makeDriverConfig(nullptr, nullptr, std::move(ex));
    auto p = makeEnginePair(config.scheduler.get());
    config.enginePairs.push_back(std::move(p));
    EXPECT_THROW(std::make_unique<Driver>(std::move(config)),
                 std::invalid_argument);
  }

  {
    // Small item cache max size too small.
    auto ex = makeJobScheduler();
    auto config = makeDriverConfig(nullptr, nullptr, std::move(ex));
    auto p = makeEnginePair(config.scheduler.get(), nullptr,
                            std::make_unique<MockEngine>(
                                "problematic small item engine", nullptr, 30),
                            50);
    config.enginePairs.push_back(std::move(p));
    config.selector = [](HashedKey) { return 1; };
    EXPECT_THROW(std::make_unique<Driver>(std::move(config)),
                 std::invalid_argument);
  }
}

// Different comibnations of engine pairs.
TEST(Driver, EnginePairCombinations) {
  BufferGen bg;

  {
    // A key that goes to engine pair 0.
    const char key0[] = "key0";
    auto smallValue0 = bg.gen(28);
    auto largeValue0 = bg.gen(29);

    // A key that goes to engine pair 1.
    const char key1[] = "key";
    auto smallValue1 = bg.gen(17);
    auto largeValue1 = bg.gen(18);
    auto ex = makeJobScheduler();

    // The first pair by default has a small item threshold of 32.
    auto bh0 = std::make_unique<MockEngine>("bh 0", nullptr, 32);
    auto bc0 = std::make_unique<MockEngine>("bc 0");
    auto bh1 = std::make_unique<MockEngine>("bh 1", nullptr, 20);
    auto bc1 = std::make_unique<MockEngine>("bc 1");
    {
      testing::InSequence inSeq;
      // Insert to small 1, found at bh 1.
      EXPECT_CALL(*bh1, insert(makeHK(key1), smallValue1.view()));
      EXPECT_CALL(*bc1, remove(makeHK(key1)));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));
      EXPECT_CALL(*bh1, lookup(makeHK(key1), _));

      // Insert large 0, found at bc 0.
      EXPECT_CALL(*bc0, insert(makeHK(key0), largeValue0.view()));
      EXPECT_CALL(*bh0, remove(makeHK(key0)));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));

      // Insert small 0, found at bh 0.
      EXPECT_CALL(*bh0, insert(makeHK(key0), smallValue0.view()));
      EXPECT_CALL(*bc0, remove(makeHK(key0)));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));
      EXPECT_CALL(*bh0, lookup(makeHK(key0), _));

      // Insert to large 1, found at bc 1.
      EXPECT_CALL(*bc1, insert(makeHK(key1), largeValue1.view()));
      EXPECT_CALL(*bh1, remove(makeHK(key1)));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));
    }

    auto config =
        makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
    auto p = makeEnginePair(config.scheduler.get(), std::move(bc1),
                            std::move(bh1), 20);
    config.enginePairs.push_back(std::move(p));
    config.selector = [](HashedKey k) { return k.key().size() % 2; };

    auto driver = std::make_unique<Driver>(std::move(config));
    Buffer valueLookup;
    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), smallValue1.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
    EXPECT_EQ(valueLookup.view(), smallValue1.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), largeValue0.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
    EXPECT_EQ(valueLookup.view(), largeValue0.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), smallValue0.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
    EXPECT_EQ(valueLookup.view(), smallValue0.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), largeValue1.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
    EXPECT_EQ(valueLookup.view(), largeValue1.view());
  }

  {
    // 0. BC; 1. BC+BH.

    // A key that goes to engine pair 0.
    const char key0[] = "key0";
    auto smallValue0 = bg.gen(28);
    auto largeValue0 = bg.gen(29);

    // A key that goes to engine pair 1.
    const char key1[] = "key";
    auto smallValue1 = bg.gen(17);
    auto largeValue1 = bg.gen(18);
    auto ex = makeJobScheduler();

    auto bh0 = nullptr;
    auto bc0 = std::make_unique<MockEngine>("bc 0");

    auto bh1 = std::make_unique<MockEngine>("bh 1", nullptr, 20);
    auto bc1 = std::make_unique<MockEngine>("bc 1");

    auto* rawBc0 = bc0.get();

    {
      testing::InSequence inSeq;
      // Insert to small 1, found at bh 1.
      EXPECT_CALL(*bh1, insert(makeHK(key1), smallValue1.view()));
      EXPECT_CALL(*bc1, remove(makeHK(key1)));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));
      EXPECT_CALL(*bh1, lookup(makeHK(key1), _));

      // Insert large 0, found at bc 0.
      EXPECT_CALL(*bc0, insert(makeHK(key0), largeValue0.view()));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));

      // Insert small 0, found at bc 0.
      EXPECT_CALL(*bc0, insert(makeHK(key0), smallValue0.view()));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));

      // Insert to large 1, found at bc 1.
      EXPECT_CALL(*bc1, insert(makeHK(key1), largeValue1.view()));
      EXPECT_CALL(*bh1, remove(makeHK(key1)));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));
    }
    auto config =
        makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
    auto p = makeEnginePair(config.scheduler.get(), std::move(bc1),
                            std::move(bh1), 20);
    config.enginePairs.push_back(std::move(p));
    config.selector = [](HashedKey k) { return k.key().size() % 2; };

    auto driver = std::make_unique<Driver>(std::move(config));
    Buffer valueLookup;
    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), smallValue1.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
    EXPECT_EQ(valueLookup.view(), smallValue1.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), largeValue0.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
    EXPECT_EQ(valueLookup.view(), largeValue0.view());

    // Have to do an evict, because MockEngine does not support replace.
    rawBc0->evict(makeHK(key0));
    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), smallValue0.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
    EXPECT_EQ(valueLookup.view(), smallValue0.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), largeValue1.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
    EXPECT_EQ(valueLookup.view(), largeValue1.view());
  }

  {
    // 0. BC+BH; 1. BC.

    // A key that goes to engine pair 0.
    const char key0[] = "key0";
    auto smallValue0 = bg.gen(28);
    auto largeValue0 = bg.gen(29);

    // A key that goes to engine pair 1.
    const char key1[] = "key";
    auto smallValue1 = bg.gen(17);
    auto largeValue1 = bg.gen(18);
    auto ex = makeJobScheduler();

    auto bh0 = std::make_unique<MockEngine>("bh 0", nullptr, 32);
    auto bc0 = std::make_unique<MockEngine>("bc 0");

    auto bh1 = nullptr;
    auto bc1 = std::make_unique<MockEngine>("bc 1");

    auto* rawBc1 = bc1.get();

    {
      testing::InSequence inSeq;
      // Insert to small 1, found at bc 1.
      EXPECT_CALL(*bc1, insert(makeHK(key1), smallValue1.view()));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));

      // Insert large 0, found at bc 0.
      EXPECT_CALL(*bc0, insert(makeHK(key0), largeValue0.view()));
      EXPECT_CALL(*bh0, remove(makeHK(key0)));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));

      // Insert small 0, found at bh 0.
      EXPECT_CALL(*bh0, insert(makeHK(key0), smallValue0.view()));
      EXPECT_CALL(*bc0, remove(makeHK(key0)));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));
      EXPECT_CALL(*bh0, lookup(makeHK(key0), _));

      bc1->evict(makeHK(key1));
      // Insert to large 1, found at bc 1.
      EXPECT_CALL(*bc1, insert(makeHK(key1), largeValue1.view()));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));
    }
    auto config =
        makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
    auto p = makeEnginePair(config.scheduler.get(), std::move(bc1),
                            std::move(bh1), 20);
    config.enginePairs.push_back(std::move(p));
    config.selector = [](HashedKey k) { return k.key().size() % 2; };

    auto driver = std::make_unique<Driver>(std::move(config));
    Buffer valueLookup;
    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), smallValue1.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
    EXPECT_EQ(valueLookup.view(), smallValue1.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), largeValue0.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
    EXPECT_EQ(valueLookup.view(), largeValue0.view());

    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), smallValue0.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
    EXPECT_EQ(valueLookup.view(), smallValue0.view());

    // Have to do an evict, because MockEngine does not support replace.
    rawBc1->evict(makeHK(key1));
    EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), largeValue1.view()));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
    EXPECT_EQ(valueLookup.view(), largeValue1.view());
  }

  {
    // Three full pairs.
    // A key that goes to engine pair 0.
    const char key0[] = "key";
    auto smallValue0 = bg.gen(29);
    auto largeValue0 = bg.gen(30);

    // A key that goes to engine pair 1.
    const char key1[] = "key1";
    auto smallValue1 = bg.gen(16);
    auto largeValue1 = bg.gen(17);

    // A key that goes to engine pair 2.
    const char key2[] = "keys2";
    auto smallValue2 = bg.gen(19);
    auto largeValue2 = bg.gen(20);

    auto ex = makeJobScheduler();

    auto bh0 = std::make_unique<MockEngine>("bh 0", nullptr, 32);
    auto bc0 = std::make_unique<MockEngine>("bc 0");

    auto bh1 = std::make_unique<MockEngine>("bh 1", nullptr, 20);
    auto bc1 = std::make_unique<MockEngine>("bc 1");

    auto bh2 = std::make_unique<MockEngine>("bh 2", nullptr, 24);
    auto bc2 = std::make_unique<MockEngine>("bc 2");

    {
      testing::InSequence inSeq;
      // Insert to small 1, found at bh 1.
      EXPECT_CALL(*bh1, insert(makeHK(key1), smallValue1.view()));
      EXPECT_CALL(*bc1, remove(makeHK(key1)));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));
      EXPECT_CALL(*bh1, lookup(makeHK(key1), _));

      // Insert large 0, found at bc 0.
      EXPECT_CALL(*bc0, insert(makeHK(key0), largeValue0.view()));
      EXPECT_CALL(*bh0, remove(makeHK(key0)));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));

      // Insert large 2, found at bc 2.
      EXPECT_CALL(*bc2, insert(makeHK(key2), largeValue2.view()));
      EXPECT_CALL(*bh2, remove(makeHK(key2)));
      EXPECT_CALL(*bc2, lookup(makeHK(key2), _));

      // Insert small 0, found at bh 0.
      EXPECT_CALL(*bh0, insert(makeHK(key0), smallValue0.view()));
      EXPECT_CALL(*bc0, remove(makeHK(key0)));
      EXPECT_CALL(*bc0, lookup(makeHK(key0), _));
      EXPECT_CALL(*bh0, lookup(makeHK(key0), _));

      // Insert to large 1, found at bc 1.
      EXPECT_CALL(*bc1, insert(makeHK(key1), largeValue1.view()));
      EXPECT_CALL(*bh1, remove(makeHK(key1)));
      EXPECT_CALL(*bc1, lookup(makeHK(key1), _));

      // Insert small 2, found at bh 2.
      EXPECT_CALL(*bh2, insert(makeHK(key2), smallValue2.view()));
      EXPECT_CALL(*bc2, remove(makeHK(key2)));
      EXPECT_CALL(*bc2, lookup(makeHK(key2), _));
      EXPECT_CALL(*bh2, lookup(makeHK(key2), _));

      auto config =
          makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
      auto p = makeEnginePair(config.scheduler.get(), std::move(bc1),
                              std::move(bh1), 20);
      auto p1 = makeEnginePair(config.scheduler.get(), std::move(bc2),
                               std::move(bh2), 24);
      config.enginePairs.push_back(std::move(p));
      config.enginePairs.push_back(std::move(p1));
      config.selector = [](HashedKey k) { return k.key().size() % 3; };

      auto driver = std::make_unique<Driver>(std::move(config));
      Buffer valueLookup;
      EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), smallValue1.view()));
      EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
      EXPECT_EQ(valueLookup.view(), smallValue1.view());

      EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), largeValue0.view()));
      EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
      EXPECT_EQ(valueLookup.view(), largeValue0.view());

      EXPECT_EQ(Status::Ok, driver->insert(makeHK(key2), largeValue2.view()));
      EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key2), valueLookup));
      EXPECT_EQ(valueLookup.view(), largeValue2.view());

      EXPECT_EQ(Status::Ok, driver->insert(makeHK(key0), smallValue0.view()));
      EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key0), valueLookup));
      EXPECT_EQ(valueLookup.view(), smallValue0.view());

      EXPECT_EQ(Status::Ok, driver->insert(makeHK(key1), largeValue1.view()));
      EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key1), valueLookup));
      EXPECT_EQ(valueLookup.view(), largeValue1.view());

      EXPECT_EQ(Status::Ok, driver->insert(makeHK(key2), smallValue2.view()));
      EXPECT_EQ(Status::Ok, driver->lookup(makeHK(key2), valueLookup));
      EXPECT_EQ(valueLookup.view(), smallValue2.view());
    }
  }
}

TEST(Driver, MultiRecovery) {
  // Recover two pairs of engines into a new instance of two pairs of engines.
  {
    const char bhdata0[] = "small cache0";
    const char bcdata0[] = "large cache0";
    const char bhdata1[] = "small cache1";
    const char bcdata1[] = "large cache1";

    // Two pairs to be persisted.
    auto bh0 = std::make_unique<MockEngine>(bhdata0, nullptr, 32);
    auto bc0 = std::make_unique<MockEngine>(bcdata0);
    auto bh1 = std::make_unique<MockEngine>(bhdata1, nullptr, 20);
    auto bc1 = std::make_unique<MockEngine>(bcdata1);

    // Two pairs to be recovered.
    auto newbh0 = std::make_unique<MockEngine>(bhdata0, nullptr, 32);
    auto newbc0 = std::make_unique<MockEngine>(bcdata0);
    auto newbh1 = std::make_unique<MockEngine>(bhdata1, nullptr, 20);
    auto newbc1 = std::make_unique<MockEngine>(bcdata1);
    {
      testing::InSequence inSeq;
      EXPECT_CALL(*bc0, mockPersistData()).WillOnce(Return(bcdata0));
      EXPECT_CALL(*bh0, mockPersistData()).WillOnce(Return(bhdata0));
      EXPECT_CALL(*bc1, mockPersistData()).WillOnce(Return(bcdata1));
      EXPECT_CALL(*bh1, mockPersistData()).WillOnce(Return(bhdata1));
      EXPECT_CALL(*newbc0, mockRecoverData(bcdata0));
      EXPECT_CALL(*newbh0, mockRecoverData(bhdata0));
      EXPECT_CALL(*newbc1, mockRecoverData(bcdata1));
      EXPECT_CALL(*newbh1, mockRecoverData(bhdata1));
    }

    auto ex = makeJobScheduler();
    auto config =
        makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
    config.enginePairs.push_back(makeEnginePair(
        config.scheduler.get(), std::move(bc1), std::move(bh1), 20));
    config.selector = [](HashedKey) { return 0; };
    auto driver = std::make_unique<Driver>(std::move(config));
    driver->persist();

    // Creating new driver instance.
    auto newEx = makeJobScheduler();
    auto newConfig = makeDriverConfig(std::move(newbc0), std::move(newbh0),
                                      std::move(newEx));
    newConfig.enginePairs.push_back(makeEnginePair(
        newConfig.scheduler.get(), std::move(newbc1), std::move(newbh1), 20));
    // The new driver would start on the same device so that the same content
    // can be restored.
    newConfig.device = std::move(driver->device_);
    driver.reset();
    newConfig.selector = [](HashedKey) { return 0; };

    auto newDriver = std::make_unique<Driver>(std::move(newConfig));
    EXPECT_TRUE(newDriver->recover());
  }

  // Increasing number of engine pairs require cold roll.
  // One pair persisted. Can not be recovered into two pairs.
  {
    const char bhdata0[] = "small cache0";
    const char bcdata0[] = "large cache0";
    const char bhdata1[] = "small cache1";
    const char bcdata1[] = "large cache1";

    // Two pairs to be persisted.
    auto bh0 = std::make_unique<MockEngine>(bhdata0, nullptr, 32);
    auto bc0 = std::make_unique<MockEngine>(bcdata0);

    // Two pairs to be recovered.
    auto newbh0 = std::make_unique<MockEngine>(bhdata0, nullptr, 32);
    auto newbc0 = std::make_unique<MockEngine>(bcdata0);
    auto newbh1 = std::make_unique<MockEngine>(bhdata1, nullptr, 20);
    auto newbc1 = std::make_unique<MockEngine>(bcdata1);
    {
      testing::InSequence inSeq;
      EXPECT_CALL(*bc0, mockPersistData()).WillOnce(Return(bcdata0));
      EXPECT_CALL(*bh0, mockPersistData()).WillOnce(Return(bhdata0));
      EXPECT_CALL(*newbc0, mockRecoverData(bcdata0));
      EXPECT_CALL(*newbh0, mockRecoverData(bhdata0));
      // Exception will throw after this.
    }

    auto ex = makeJobScheduler();
    auto config =
        makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
    auto driver = std::make_unique<Driver>(std::move(config));
    driver->persist();

    // Creating new driver instance.
    auto newEx = makeJobScheduler();
    auto newConfig = makeDriverConfig(std::move(newbc0), std::move(newbh0),
                                      std::move(newEx));
    newConfig.enginePairs.push_back(makeEnginePair(
        newConfig.scheduler.get(), std::move(newbc1), std::move(newbh1), 20));
    // The new driver would start on the same device so that the same content
    // can be restored.
    newConfig.device = std::move(driver->device_);
    driver.reset();
    newConfig.selector = [](HashedKey) { return 0; };

    auto newDriver = std::make_unique<Driver>(std::move(newConfig));
    // Throw logic_error because the record reader has ended
    EXPECT_THROW(newDriver->recover(), std::logic_error);
  }

  // Same number of engine pairs, different setup.
  {
    const char bhdata0[] = "small cache0";
    const char bcdata0[] = "large cache0";
    const char bhdata1[] = "small cache1";
    const char bcdata1[] = "large cache1";

    // Two pairs to be persisted.
    auto bh0 = std::make_unique<MockEngine>(bhdata0, nullptr, 32);
    auto bc0 = std::make_unique<MockEngine>(bcdata0);
    auto bh1 = std::make_unique<MockEngine>(bhdata1, nullptr, 20);
    auto bc1 = std::make_unique<MockEngine>(bcdata1);

    // Two pairs to be recovered. The second pair has a different setup.
    auto newbh0 = std::make_unique<MockEngine>(bhdata0, nullptr, 32);
    auto newbc0 = std::make_unique<MockEngine>(bcdata0);
    auto newbh1 = std::make_unique<MockEngine>(bhdata1, nullptr, 20);
    auto newbc1 = std::make_unique<MockEngine>("new large cache1");
    {
      testing::InSequence inSeq;
      EXPECT_CALL(*bc0, mockPersistData()).WillOnce(Return(bcdata0));
      EXPECT_CALL(*bh0, mockPersistData()).WillOnce(Return(bhdata0));
      EXPECT_CALL(*bc1, mockPersistData()).WillOnce(Return(bcdata1));
      EXPECT_CALL(*bh1, mockPersistData()).WillOnce(Return(bhdata1));
      EXPECT_CALL(*newbc0, mockRecoverData(bcdata0));
      EXPECT_CALL(*newbh0, mockRecoverData(bhdata0));
      // Trying to restore from the old small cache setup and resulted in false.
      EXPECT_CALL(*newbc1, mockRecoverData(bcdata1)).WillOnce(Return(false));
      // The small item cache is not trying to restore because the restore has
      // failed.
    }

    auto ex = makeJobScheduler();
    auto config =
        makeDriverConfig(std::move(bc0), std::move(bh0), std::move(ex));
    config.enginePairs.push_back(makeEnginePair(
        config.scheduler.get(), std::move(bc1), std::move(bh1), 20));
    config.selector = [](HashedKey) { return 0; };
    auto driver = std::make_unique<Driver>(std::move(config));
    driver->persist();

    // Creating new driver instance.
    auto newEx = makeJobScheduler();
    auto newConfig = makeDriverConfig(std::move(newbc0), std::move(newbh0),
                                      std::move(newEx));
    newConfig.enginePairs.push_back(makeEnginePair(
        newConfig.scheduler.get(), std::move(newbc1), std::move(newbh1), 20));
    // The new driver would start on the same device so that the same content
    // can be restored.
    newConfig.device = std::move(driver->device_);
    driver.reset();
    newConfig.selector = [](HashedKey) { return 0; };
    auto newDriver = std::make_unique<Driver>(std::move(newConfig));
    // Return false,
    EXPECT_FALSE(newDriver->recover());
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
