#include "cachelib/navy/driver/Driver.h"

#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/common/Hash.h"
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
            [cb](BufferView key, BufferView value, DestructorEvent event) {
              if (cb) {
                cb->call(key, value, event);
              }
            }} {
    ON_CALL(*this, insert(_, _, _))
        .WillByDefault(
            Invoke([this](HashedKey hk, BufferView value, InsertOptions) {
              auto entry = std::make_pair(Buffer{hk.key()}, Buffer{value});
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
    ON_CALL(*this, remove(_)).WillByDefault(Invoke([this](HashedKey hk) {
      return cache_.erase(hk) == 0 ? Status::NotFound : Status::Ok;
    }));
    ON_CALL(*this, flush()).WillByDefault(Return());
    ON_CALL(*this, reset()).WillByDefault(Return());
    ON_CALL(*this, mockPersistData()).WillByDefault(Return(""));
    ON_CALL(*this, mockRecoverData(_)).WillByDefault(Return(true));
  }

  ~MockEngine() override = default;

  MOCK_METHOD3(insert,
               Status(HashedKey hk, BufferView value, InsertOptions opt));
  MOCK_METHOD2(lookup, Status(HashedKey hk, Buffer& value));
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

  // Returns true if key found and can be actually evicted in the real world
  bool evict(BufferView key) {
    auto itr = cache_.find(makeHK(key));
    if (itr == cache_.end()) {
      return false;
    }
    auto value = std::move(itr->second.second);
    cache_.erase(itr);
    if (destructorCb_) {
      destructorCb_(key, value.view(), DestructorEvent::Recycled);
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

class MockAP final : public AdmissionPolicy {
 public:
  MOCK_METHOD2(accept, bool(HashedKey hk, BufferView value));
  MOCK_METHOD0(reset, void());
  MOCK_CONST_METHOD1(getCounters, void(const CounterVisitor& visitor));
};

std::unique_ptr<JobScheduler> makeJobScheduler() {
  return std::make_unique<MockSingleThreadJobScheduler>();
}

Driver::Config makeDriverConfig(std::unique_ptr<Engine> bc,
                                std::unique_ptr<Engine> si,
                                std::unique_ptr<JobScheduler> ex) {
  constexpr uint64_t kDeviceSize{64 * 1024};
  size_t metadataSize = 3 * 1024 * 1024;
  auto deviceSize = metadataSize + kDeviceSize;
  Driver::Config config;
  config.scheduler = std::move(ex);
  config.largeItemCache = std::move(bc);
  config.device = createMemoryDevice(deviceSize);
  config.smallItemMaxSize = kSmallItemMaxSize;
  config.smallItemCache = std::move(si);
  config.metadataSize = metadataSize;
  return config;
}
} // namespace

TEST(Driver, SmallItem) {
  BufferGen bg;
  auto value = bg.gen(16);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), value.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto exPtr = ex.get();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  MockInsertCB cbInsert;
  EXPECT_CALL(cbInsert, call(Status::Ok, makeView("key")));

  // Test async insert for small item cache
  auto valCopy = Buffer{value.view()};
  auto valView = valCopy.view();
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(makeView("key"),
                                valView,
                                InsertOptions{},
                                [&cbInsert, v = std::move(valCopy)](
                                    Status status, BufferView k) {
                                  cbInsert.call(status, k);
                                }));
  exPtr->finish();

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
  EXPECT_EQ(value.view(), valueLookup.view());
}

TEST(Driver, LargeItem) {
  BufferGen bg;
  auto value = bg.gen(32);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, insert(makeHK("key"), value.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto exPtr = ex.get();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  MockInsertCB cbInsert;
  EXPECT_CALL(cbInsert, call(Status::Ok, makeView("key")));

  // Test async insert for large item cache
  auto valCopy = Buffer{value.view()};
  auto valView = valCopy.view();
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(makeView("key"),
                                valView,
                                InsertOptions{},
                                [&cbInsert, v = std::move(valCopy)](
                                    Status status, BufferView k) {
                                  cbInsert.call(status, k);
                                }));
  exPtr->finish();

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
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
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), largeValue.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), smallValue.view(), {}));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
  EXPECT_EQ(smallValue.view(), valueLookup.view());
}

TEST(Driver, InsertFailed) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  // Insert a small item. Then insert a large one. Simulate large failed, device
  // error reported. Small is still available.
  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();
  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view(), InsertOptions()))
        .WillOnce(Return(Status::DeviceError));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), smallValue.view(), {}));
  EXPECT_EQ(Status::DeviceError,
            driver->insert(makeView("key"), largeValue.view(), {}));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
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
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key")))
        .WillOnce(Return(Status::DeviceError));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), smallValue.view(), {}));
  EXPECT_EQ(Status::BadState,
            driver->insert(makeView("key"), largeValue.view(), {}));

  // We don't provide any guarantees what is available. But in our test we
  // can check what is visible.
  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
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
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), smallValue.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), largeValue.view(), {}));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
  EXPECT_EQ(largeValue.view(), valueLookup.view());

  EXPECT_EQ(Status::Ok, driver->remove(makeView("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeView("key"), valueLookup));
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
// TODO: Eviction test with real engine when both ready

TEST(Driver, EvictBlockCache) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);

  MockDestructor ecbBC;
  EXPECT_CALL(
      ecbBC,
      call(makeView("key"), largeValue.view(), DestructorEvent::Recycled));
  auto bc = std::make_unique<MockEngine>("", &ecbBC);
  auto bcPtr = bc.get();

  MockDestructor ecbSI;
  // Nothing should be called
  auto si = std::make_unique<MockEngine>("", &ecbSI);
  auto siPtr = si.get();

  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), smallValue.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), largeValue.view(), {}));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
  EXPECT_EQ(largeValue.view(), valueLookup.view());

  // If we inserted in block cache, eviction from small cache should not happen,
  // because it is removed.
  EXPECT_FALSE(siPtr->evict(makeView("key")));

  // But it can be evicted from block cache
  EXPECT_TRUE(bcPtr->evict(makeView("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeView("key"), valueLookup));
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
  EXPECT_CALL(
      ecbSI,
      call(makeView("key"), smallValue.view(), DestructorEvent::Recycled));
  auto si = std::make_unique<MockEngine>("", &ecbSI);
  auto siPtr = si.get();

  {
    testing::InSequence inSeq;
    EXPECT_CALL(*bc, insert(makeHK("key"), largeValue.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key")));
    EXPECT_CALL(*si, insert(makeHK("key"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key")));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
    EXPECT_CALL(*bc, lookup(makeHK("key"), _));
    EXPECT_CALL(*si, lookup(makeHK("key"), _));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), largeValue.view(), {}));
  EXPECT_EQ(Status::Ok, driver->insert(makeView("key"), smallValue.view(), {}));

  Buffer valueLookup;
  EXPECT_EQ(Status::Ok, driver->lookup(makeView("key"), valueLookup));
  EXPECT_EQ(smallValue.view(), valueLookup.view());

  EXPECT_FALSE(bcPtr->evict(makeView("key")));

  EXPECT_TRUE(siPtr->evict(makeView("key")));
  EXPECT_EQ(Status::NotFound, driver->lookup(makeView("key"), valueLookup));
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

TEST(Driver, PermanentItem) {
  BufferGen bg;
  auto smallValue = bg.gen(16);
  auto largeValue = bg.gen(32);
  auto veryBigKey = bg.gen(256);

  auto bc = std::make_unique<MockEngine>();
  auto si = std::make_unique<MockEngine>();

  {
    testing::InSequence inSeq;
    EXPECT_CALL(*si,
                insert(makeHK("key1"), smallValue.view(), InsertOptions()));
    EXPECT_CALL(*bc, remove(makeHK("key1")));
    EXPECT_CALL(*bc,
                insert(makeHK("key2"), largeValue.view(), InsertOptions()));
    EXPECT_CALL(*si, remove(makeHK("key2")));
    EXPECT_CALL(*bc,
                insert(makeHK("perm1"),
                       smallValue.view(),
                       InsertOptions().setPermanent()));
    EXPECT_CALL(*si, remove(makeHK("perm1")));
    EXPECT_CALL(*bc,
                insert(makeHK("perm2"),
                       largeValue.view(),
                       InsertOptions().setPermanent()));
    EXPECT_CALL(*si, remove(makeHK("perm2")));
  }

  auto ex = makeJobScheduler();
  auto config = makeDriverConfig(std::move(bc), std::move(si), std::move(ex));
  auto driver = std::make_unique<Driver>(std::move(config));

  // Regular value lands depending on size
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("key1"), smallValue.view(), {}));
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("key2"), largeValue.view(), {}));

  // Permanent value always goes to large item cache, disregarding size
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("perm1"),
                           smallValue.view(),
                           InsertOptions().setPermanent()));
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("perm2"),
                           largeValue.view(),
                           InsertOptions().setPermanent()));

  // Permanent item also gets rejected if its key size is too big
  EXPECT_EQ(Status::Rejected,
            driver->insert(veryBigKey.view(),
                           makeView("value"),
                           InsertOptions().setPermanent()));
}

TEST(Driver, AdmissionPolicy) {
  auto bc = std::make_unique<MockEngine>();
  EXPECT_CALL(
      *bc,
      insert(makeHK("key"), makeView("value"), InsertOptions().setPermanent()));
  auto ex = makeJobScheduler();
  auto ap = std::make_unique<MockAP>();
  EXPECT_CALL(*ap, accept(_, _)).WillRepeatedly(testing::Return(false));
  auto config = makeDriverConfig(std::move(bc), nullptr, std::move(ex));
  config.admissionPolicy = std::move(ap);
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Rejected,
            driver->insert(makeView("key"), makeView("value"), {}));
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("key"),
                           makeView("value"),
                           InsertOptions().setPermanent()));
}

TEST(Driver, ConcurrentInserts) {
  SeqPoints sp;
  sp.setName(0, "first insert started");
  sp.setName(1, "second insert started");
  sp.setName(2, "permanent insert finished");

  auto bc = std::make_unique<MockEngine>();
  EXPECT_CALL(*bc, insert(makeHK("1"), makeView("v1"), InsertOptions()))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(0);
        sp.wait(2);
        return Status::Ok;
      }));
  EXPECT_CALL(*bc, insert(makeHK("2"), makeView("v2"), InsertOptions()))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(1);
        sp.wait(2);
        return Status::Ok;
      }));
  EXPECT_CALL(
      *bc,
      insert(makeHK("perm"), makeView("vP"), InsertOptions().setPermanent()));

  auto ex = std::make_unique<ThreadPoolJobScheduler>(1, 10);
  auto config = makeDriverConfig(std::move(bc), nullptr, std::move(ex));
  config.maxConcurrentInserts = 2;
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok,
            driver->insertAsync(makeView("1"), makeView("v1"), {}, {}));
  EXPECT_EQ(Status::Ok,
            driver->insertAsync(makeView("2"), makeView("v2"), {}, {}));

  sp.wait(0);
  sp.wait(1);
  EXPECT_EQ(Status::Rejected,
            driver->insertAsync(makeView("3"), makeView("v3"), {}, {}));
  EXPECT_EQ(Status::Ok,
            driver->insert(makeView("perm"),
                           makeView("vP"),
                           InsertOptions().setPermanent()));
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
  EXPECT_CALL(*bc, insert(makeHK("1"), v1.view(), InsertOptions()))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(0);
        sp.wait(2);
        return Status::Ok;
      }));
  EXPECT_CALL(*bc, insert(makeHK("2"), v2.view(), InsertOptions()))
      .WillOnce(testing::InvokeWithoutArgs([&sp] {
        sp.reached(1);
        sp.wait(3);
        return Status::Ok;
      }));
  EXPECT_CALL(*bc, insert(makeHK("3"), v3.view(), InsertOptions()));

  auto ex = std::make_unique<ThreadPoolJobScheduler>(1, 10);
  auto config = makeDriverConfig(std::move(bc), nullptr, std::move(ex));
  config.maxParcelMemory = 3000;
  auto driver = std::make_unique<Driver>(std::move(config));

  EXPECT_EQ(Status::Ok, driver->insertAsync(makeView("1"), v1.view(), {}, {}));
  EXPECT_EQ(Status::Ok, driver->insertAsync(makeView("2"), v2.view(), {}, {}));
  sp.wait(0);
  sp.wait(1);

  EXPECT_EQ(Status::Rejected,
            driver->insertAsync(makeView("3"), v3.view(), {}, {}));

  sp.reached(2);
  // There is some gap between we unblock callback and counters go down. Keep
  // counting rejects.
  uint32_t rejects = 1;
  Status st;
  while ((st = driver->insertAsync(makeView("3"), v3.view(), {}, {})) ==
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
