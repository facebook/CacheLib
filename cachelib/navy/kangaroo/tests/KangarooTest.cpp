#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <map>

#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/kangaroo/Kangaroo.h"
#include "cachelib/navy/kangaroo/KangarooLog.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/SeqPoints.h"

using testing::_;
using testing::AtLeast;
using testing::InSequence;
using testing::NiceMock;
using testing::Return;
using testing::StrictMock;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
void setLayout(Kangaroo::Config& config,
               uint32_t bs,
               uint32_t numKangarooBuckets) {
  config.bucketSize = bs;
  config.totalSetSize = uint64_t{bs} * numKangarooBuckets;
  config.avgSmallObjectSize = 10;

  auto bv = std::make_unique<RripBitVector>(numKangarooBuckets);
  config.rripBitVector = std::move(bv);
}

void setLog(KangarooLog::Config& config,
            uint32_t readSize,
            uint32_t threshold,
            uint32_t physicalPartitions,
            SetNumberCallback setNumCb,
            SetMultiInsertCallback insertCb) {
  config.readSize = readSize;
  config.segmentSize = 2 * config.readSize;
  config.logSize = 4 * config.segmentSize;
  config.threshold = threshold;
  config.logPhysicalPartitions = physicalPartitions;
  config.setNumberCallback = setNumCb;
  config.setMultiInsertCallback = insertCb;
}
} // namespace

TEST(Kangaroo, InsertAndRemove) {
  Kangaroo::Config config;
  setLayout(config, 128, 2);
  auto device =
      std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 128);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  EXPECT_EQ(Status::Ok, bh.remove(makeHK("key")));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(Status::NotFound, bh.remove(makeHK("key")));
}

TEST(Kangaroo, InsertAndRemoveLog) {
  Kangaroo::Config config;
  setLayout(config, 64, 8);

  auto testInsertCb = [](std::vector<std::unique_ptr<ObjectInfo>>&,
                         ReadmitCallback) { return; };
  auto testSetNumCb = [](uint64_t id) { return KangarooBucketId(id % 2); };
  setLog(config.logConfig, 64, 1, 2, testSetNumCb, testInsertCb);
  config.logConfig.numTotalIndexBuckets = 8;
  config.logIndexPartitionsPerPhysical = 2;
  auto device = std::make_unique<NiceMock<MockDevice>>(
      config.totalSetSize + config.logConfig.logSize, 64);
  config.device = device.get();
  config.logConfig.device = config.device;
  config.logConfig.mergeThreads = 1;
  config.cacheBaseOffset = config.logConfig.logSize;

  Kangaroo bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  EXPECT_EQ(Status::Ok, bh.remove(makeHK("key")));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(Status::NotFound, bh.remove(makeHK("key")));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key1"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key1"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key2"), makeView("22222")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key2"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key3"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key2"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key4"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key2"), value));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key5"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key2"), value));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key6"), makeView("66666")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key6"), value));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key6"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key7"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key6"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key8"), makeView("88888")));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key9"), makeView("99999")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("ke1&"), makeView("1&1&1")));

  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key2"), value));
  EXPECT_EQ(makeView("22222"), value.view());
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key6"), value));
  EXPECT_EQ(makeView("66666"), value.view());
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key8"), value));
  EXPECT_EQ(makeView("88888"), value.view());
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key9"), value));
  EXPECT_EQ(makeView("99999"), value.view());
}

TEST(Kangaroo, ReorderEviction) {
  Kangaroo::Config config;
  size_t bucketCount = 1;
  setLayout(config, 128, bucketCount);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 64);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key1"), makeView("11112222")));
  EXPECT_EQ(Status::Ok,
            bh.insert(makeHK("key2"), makeView("22223333test22223333")));

  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key1"), value));
  EXPECT_EQ(makeView("11112222"), value.view());

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key3"), makeView("33334444")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key4"), makeView("44445555")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key1"), value));
  EXPECT_EQ(makeView("11112222"), value.view());
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key2"), value));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key3"), value));
  EXPECT_EQ(makeView("33334444"), value.view());
}

TEST(Kangaroo, SimpleStats) {
  Kangaroo::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 64);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  {
    MockCounterVisitor helper;
    EXPECT_CALL(helper, call(_, _)).Times(AtLeast(0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_approx_bytes_in_size_64"), 8));
    EXPECT_CALL(helper, call(strPiece("navy_bh_items"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_inserts"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_inserts"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_lookups"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_lookups"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_removes"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_removes"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_evictions"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_logical_written"), 8));
    EXPECT_CALL(helper, call(strPiece("navy_bh_physical_written"), 64));
    EXPECT_CALL(helper, call(strPiece("navy_bh_io_errors"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_bf_false_positive_pct"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_checksum_errors"), 0));
    bh.getCounters(toCallback(helper));
  }

  EXPECT_EQ(Status::Ok, bh.remove(makeHK("key")));
  {
    MockCounterVisitor helper;
    EXPECT_CALL(helper, call(_, _)).Times(AtLeast(0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_approx_bytes_in_size_64"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_items"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_inserts"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_inserts"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_lookups"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_lookups"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_removes"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_removes"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_evictions"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_logical_written"), 8));
    EXPECT_CALL(helper, call(strPiece("navy_bh_physical_written"), 128));
    EXPECT_CALL(helper, call(strPiece("navy_bh_io_errors"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_bf_false_positive_pct"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_checksum_errors"), 0));
    bh.getCounters(toCallback(helper));
  }
}

TEST(Kangaroo, EvictionStats) {
  Kangaroo::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 64);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key1"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key2"), makeView("123456789")));
  {
    MockCounterVisitor helper;
    EXPECT_CALL(helper, call(_, _)).Times(AtLeast(0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_approx_bytes_in_size_64"), 13));
    EXPECT_CALL(helper, call(strPiece("navy_bh_items"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_inserts"), 2));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_inserts"), 2));
    EXPECT_CALL(helper, call(strPiece("navy_bh_lookups"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_lookups"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_removes"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_removes"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_evictions"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_logical_written"), 22));
    EXPECT_CALL(helper, call(strPiece("navy_bh_physical_written"), 128));
    EXPECT_CALL(helper, call(strPiece("navy_bh_io_errors"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_bf_false_positive_pct"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_checksum_errors"), 0));
    bh.getCounters(toCallback(helper));
  }
}

TEST(Kangaroo, DeviceErrorStats) {
  Kangaroo::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 64);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key1"), makeView("1")));
  EXPECT_CALL(*device, writeImpl(0, 64, _)).WillOnce(Return(false));
  EXPECT_EQ(Status::DeviceError, bh.insert(makeHK("key2"), makeView("1")));
  {
    MockCounterVisitor helper;
    EXPECT_CALL(helper, call(_, _)).Times(AtLeast(0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_items"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_inserts"), 2));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_inserts"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_lookups"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_lookups"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_removes"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_succ_removes"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_evictions"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_logical_written"), 5));
    EXPECT_CALL(helper, call(strPiece("navy_bh_physical_written"), 64));
    EXPECT_CALL(helper, call(strPiece("navy_bh_io_errors"), 1));
    EXPECT_CALL(helper, call(strPiece("navy_bh_bf_false_positive_pct"), 0));
    EXPECT_CALL(helper, call(strPiece("navy_bh_checksum_errors"), 0));
    bh.getCounters(toCallback(helper));
  }
}

TEST(Kangaroo, DoubleInsert) {
  Kangaroo::Config config;
  setLayout(config, 128, 2);
  auto device =
      std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 128);
  config.device = device.get();

  MockDestructor helper;
  config.destructorCb = toCallback(helper);

  Kangaroo bh(std::move(config));

  Buffer value;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  EXPECT_CALL(
      helper,
      call(makeView("key"), makeView("12345"), DestructorEvent::Removed));

  // Insert the same key a second time will overwrite the previous value.
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("45678")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("45678"), value.view());
}

TEST(Kangaroo, DestructorCallback) {
  Kangaroo::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 64);
  config.device = device.get();

  MockDestructor helper;
  EXPECT_CALL(
      helper,
      call(makeView("key 1"), makeView("value 1"), DestructorEvent::Recycled));
  EXPECT_CALL(
      helper,
      call(makeView("key 2"), makeView("value 2"), DestructorEvent::Removed));
  config.destructorCb = toCallback(helper);

  Kangaroo bh(std::move(config));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 1"), makeView("value 1")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 2"), makeView("value 2")));
  EXPECT_EQ(Status::Ok, bh.remove(makeHK("key 2")));
}

TEST(Kangaroo, Reset) {
  Kangaroo::Config config;
  size_t bucketCount = 2;
  setLayout(config, 128, bucketCount);

  auto bf = std::make_unique<BloomFilter>(bucketCount, 2, 4);
  config.bloomFilter = std::move(bf);

  auto device =
      std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 128);
  auto readFirstKangarooBucket = [&dev = device->getRealDeviceRef()] {
    // Read only the bucket after the initial generation and checksum fields
    // since the former can change with reset() while the latter can change
    // with every write.
    Buffer buf{92};
    dev.read(8, 92, buf.data());
    return buf;
  };
  config.device = device.get();

  Kangaroo bh(std::move(config));

  Buffer value;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());
  auto oldKangarooBucketContent = readFirstKangarooBucket();

  bh.reset();
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));

  // bh reset should reset the bloom filter as well.
  EXPECT_EQ(1, bh.bfRejectCount());

  // The new bucket content must be identical to that of the old since
  // after a reset, our first write is equivalent to writing to a brand
  // new bucket.
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  auto newKangarooBucketContent = readFirstKangarooBucket();
  EXPECT_EQ(oldKangarooBucketContent.view(), newKangarooBucketContent.view());
}

TEST(Kangaroo, WriteInTwoKangarooBuckets) {
  // Following will generate a list of numbers and their hashes, I pick
  // two that collide and another one that doesn't for this test.
  //
  // for (uint8_t i = 'A'; i < 'z'; ++i) {
  //   Buffer key{1};
  //   *key.data() = i;
  //   std::bitset<sizeof(uint64_t)> out{hashBuffer(key.view())};
  //   std::cerr << "key: " << i << ", hash: " << out << std::endl;
  // }
  //
  // key: A, hash: 10111010
  // key: B, hash: 10110001
  // key: C, hash: 11100010
  // key: D, hash: 01101100

  Kangaroo::Config config;
  config.cacheBaseOffset = 256;
  setLayout(config, 128, 2);
  auto device = std::make_unique<StrictMock<MockDevice>>(
      config.cacheBaseOffset + config.totalSetSize, 128);
  {
    InSequence inSeq;
    EXPECT_CALL(*device, readImpl(256, 128, _));
    EXPECT_CALL(*device, writeImpl(256, 128, _));
    EXPECT_CALL(*device, readImpl(384, 128, _));
    EXPECT_CALL(*device, writeImpl(384, 128, _));
    EXPECT_CALL(*device, readImpl(256, 128, _));
    EXPECT_CALL(*device, writeImpl(256, 128, _));
  }
  config.device = device.get();

  Kangaroo bh(std::move(config));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("A"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("B"), makeView("45678")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("C"), makeView("67890")));
}

TEST(Kangaroo, RemoveNotFound) {
  // Remove that is not found will not write to disk, but rebuild bloom filter
  // and second remove would not read the device.
  Kangaroo::Config config;
  setLayout(config, 128, 1);
  auto device =
      std::make_unique<StrictMock<MockDevice>>(config.totalSetSize, 128);
  {
    InSequence inSeq;
    EXPECT_CALL(*device, readImpl(0, 128, _));
    EXPECT_CALL(*device, writeImpl(0, 128, _));
    EXPECT_CALL(*device, readImpl(0, 128, _));
    EXPECT_CALL(*device, writeImpl(0, 128, _));
    EXPECT_CALL(*device, readImpl(0, 128, _));
  }
  config.device = device.get();

  Kangaroo bh(std::move(config));

  bh.insert(makeHK("key"), makeView("12345"));
  bh.remove(makeHK("key"));
  bh.remove(makeHK("key"));
}

TEST(Kangaroo, CorruptKangarooBucket) {
  // Write a bucket, then corrupt a byte so we won't be able to read it
  Kangaroo::Config config;
  setLayout(config, 128, 2);
  auto device =
      std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 128);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  bh.insert(makeHK("key"), makeView("12345"));

  Buffer value;
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  // Corrupt data. Use device directly to avoid alignment checks
  unsigned char badBytes[4] = {13, 17, 19, 23};
  Buffer b{BufferView{sizeof(badBytes), &badBytes[0]}};
  device->getRealDeviceRef().write(10, std::move(b));

  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
}

TEST(Kangaroo, Recovery) {
  Kangaroo::Config config;
  config.totalSetSize = 16 * 1024;
  auto bv = std::make_unique<RripBitVector>(config.numBuckets());
  config.rripBitVector = std::move(bv);
  auto device = createMemoryDevice(config.totalSetSize, nullptr);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  folly::IOBufQueue queue;
  auto rw = createMemoryRecordWriter(queue);
  bh.persist(*rw);

  auto rr = createMemoryRecordReader(queue);
  ASSERT_TRUE(bh.recover(*rr));

  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());
}

TEST(Kangaroo, RecoveryBadConfig) {
  folly::IOBufQueue queue;
  {
    Kangaroo::Config config;
    config.totalSetSize = 16 * 1024;
    config.bucketSize = 4 * 1024;
    auto device = createMemoryDevice(config.totalSetSize, nullptr);
    config.device = device.get();
    auto bv = std::make_unique<RripBitVector>(config.numBuckets());
    config.rripBitVector = std::move(bv);

    Kangaroo bh(std::move(config));

    Buffer value;
    EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    auto rw = createMemoryRecordWriter(queue);
    bh.persist(*rw);
  }
  {
    Kangaroo::Config config;
    // Config is different. Number of buckets the same, but size is different.
    config.totalSetSize = 32 * 1024;
    config.bucketSize = 8 * 1024;
    auto device = createMemoryDevice(config.totalSetSize, nullptr);
    config.device = device.get();
    auto bv = std::make_unique<RripBitVector>(config.numBuckets());
    config.rripBitVector = std::move(bv);

    Kangaroo bh(std::move(config));
    auto rr = createMemoryRecordReader(queue);
    ASSERT_FALSE(bh.recover(*rr));
  }
}

TEST(Kangaroo, RecoveryCorruptedData) {
  Kangaroo::Config config;
  config.totalSetSize = 1024 * 1024;
  auto bv = std::make_unique<RripBitVector>(config.numBuckets());
  config.rripBitVector = std::move(bv);
  auto device = createMemoryDevice(config.totalSetSize, nullptr);
  config.device = device.get();

  Kangaroo bh(std::move(config));

  Buffer value;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345")));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  auto ioBuf = folly::IOBuf::createCombined(512);
  std::generate(
      ioBuf->writableData(), ioBuf->writableTail(), std::minstd_rand());

  folly::IOBufQueue queue;
  auto rw = createMemoryRecordWriter(queue);
  rw->writeRecord(std::move(ioBuf));

  auto rr = createMemoryRecordReader(queue);
  ASSERT_FALSE(bh.recover(*rr));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
}

TEST(Kangaroo, ConcurrentRead) {
  // In one bucket, we can have multiple readers at the same time.
  Kangaroo::Config config;
  setLayout(config, 128, 1);
  auto device =
      std::make_unique<NiceMock<MockDevice>>(config.totalSetSize, 128);
  config.device = device.get();

  Kangaroo bh(std::move(config));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 1"), makeView("1")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 2"), makeView("2")));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 3"), makeView("3")));

  struct MockLookupHelper {
    MOCK_METHOD2(call, void(BufferView, BufferView));
  };
  MockLookupHelper helper;
  EXPECT_CALL(helper, call(makeView("key 1"), makeView("1")));
  EXPECT_CALL(helper, call(makeView("key 2"), makeView("2")));
  EXPECT_CALL(helper, call(makeView("key 3"), makeView("3")));

  auto runRead = [&bh, &helper](HashedKey hk) {
    Buffer value;
    EXPECT_EQ(Status::Ok, bh.lookup(hk, value));
    helper.call(hk.key(), value.view());
  };

  auto t1 = std::thread(runRead, makeHK("key 1"));
  auto t2 = std::thread(runRead, makeHK("key 2"));
  auto t3 = std::thread(runRead, makeHK("key 3"));
  t1.join();
  t2.join();
  t3.join();
}

TEST(Kangaroo, BloomFilterRecoveryFail) {
  Kangaroo::Config config;
  setLayout(config, 128, 2);
  auto device =
      std::make_unique<StrictMock<MockDevice>>(config.totalSetSize, 128);
  EXPECT_CALL(*device, readImpl(_, _, _)).Times(0);
  config.device = device.get();
  config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);

  Kangaroo bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("100"), value));

  // After a bad recovery, filters will not be affected
  auto ioBuf = folly::IOBuf::createCombined(512);
  std::generate(
      ioBuf->writableData(), ioBuf->writableTail(), std::minstd_rand());

  folly::IOBufQueue queue;
  auto rw = createMemoryRecordWriter(queue);
  rw->writeRecord(std::move(ioBuf));
  auto rr = createMemoryRecordReader(queue);
  ASSERT_FALSE(bh.recover(*rr));

  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("100"), value));
}

TEST(Kangaroo, BloomFilter) {
  Kangaroo::Config config;
  setLayout(config, 128, 2);
  auto device = createMemoryDevice(config.totalSetSize, nullptr);
  config.device = device.get();
  config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);

  MockDestructor helper;
  EXPECT_CALL(helper, call(makeView("100"), _, DestructorEvent::Recycled));
  EXPECT_CALL(helper, call(makeView("101"), _, DestructorEvent::Removed));
  config.destructorCb = toCallback(helper);

  Kangaroo bh(std::move(config));
  BufferGen bg;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("100"), bg.gen(20).view()));

  // Check that eviction triggers BF rebuild. Use the following setup:
  // - Insert "100". BF rejects "101" and accepts "102" and "103".
  // - Insert "101": bucket 0 contains "100" and "101". BF rejects "110".
  // - Insert "110", "100" evicted: bucket contains "101" and "110".
  //
  // Expect that BF rejects "102" and "103". If not rebuilt, it would accept.
  EXPECT_EQ(0, bh.bfRejectCount());

  Buffer value;
  // Rejected by BF
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("101"), value));
  EXPECT_EQ(1, bh.bfRejectCount());
  // False positive, rejected by key
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("102"), value));
  EXPECT_EQ(1, bh.bfRejectCount());
  // False positive, rejected by key
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("103"), value));
  EXPECT_EQ(1, bh.bfRejectCount());

  // Insert "101"
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("101"), bg.gen(20).view()));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("110"), value));
  EXPECT_EQ(2, bh.bfRejectCount());

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("110"), bg.gen(20).view()));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("101"), value));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("110"), value));
  EXPECT_EQ(2, bh.bfRejectCount());

  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("100"), value));
  EXPECT_EQ(3, bh.bfRejectCount());
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("102"), value));
  EXPECT_EQ(4, bh.bfRejectCount());
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("103"), value));
  EXPECT_EQ(5, bh.bfRejectCount());

  // If we remove, BF will be rebuilt and will reject "101":
  EXPECT_EQ(Status::Ok, bh.remove(makeHK("101")));
  EXPECT_EQ(5, bh.bfRejectCount());
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("101"), value));
  EXPECT_EQ(6, bh.bfRejectCount());
}

// Lookup and remove rebuilds BF on first access to the bucket
TEST(Kangaroo, BloomFilterRecoveryRebuild) {
  std::unique_ptr<Device> actual;
  folly::IOBufQueue queue;

  // Add a value to the first bucket and persist.
  {
    Kangaroo::Config config;
    setLayout(config, 128, 2);
    auto device =
        std::make_unique<StrictMock<MockDevice>>(config.totalSetSize, 128);
    EXPECT_CALL(*device, readImpl(0, 128, _));
    EXPECT_CALL(*device, writeImpl(0, 128, _));
    config.device = device.get();
    config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);

    Kangaroo bh(std::move(config));
    EXPECT_EQ(Status::Ok, bh.insert(makeHK("100"), makeView("cat")));
    auto rw = createMemoryRecordWriter(queue);
    bh.persist(*rw);

    actual = device->releaseRealDevice();
  }

  // Recover. BF is also recovered.
  {
    Kangaroo::Config config;
    setLayout(config, 128, 2);
    auto device = std::make_unique<MockDevice>(0, 128);
    device->setRealDevice(std::move(actual));
    EXPECT_CALL(*device, readImpl(0, 128, _)).Times(0);
    EXPECT_CALL(*device, readImpl(128, 128, _)).Times(0);
    config.device = device.get();
    config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);

    Kangaroo bh(std::move(config));
    auto rr = createMemoryRecordReader(queue);
    ASSERT_TRUE(bh.recover(*rr));

    EXPECT_EQ(Status::NotFound, bh.remove(makeHK("200")));
    EXPECT_EQ(1, bh.bfRejectCount());

    EXPECT_EQ(Status::NotFound, bh.remove(makeHK("200")));
    EXPECT_EQ(2, bh.bfRejectCount());
    Buffer value;
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("200"), value));
    EXPECT_EQ(3, bh.bfRejectCount());

    // Test lookup, second bucket
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("201"), value));
    EXPECT_EQ(4, bh.bfRejectCount());
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("201"), value));
    EXPECT_EQ(5, bh.bfRejectCount());

    actual = device->releaseRealDevice();
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
