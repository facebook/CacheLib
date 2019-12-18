#include "cachelib/navy/bighash/BigHash.h"

#include <map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/driver/Driver.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"
#include "cachelib/navy/testing/MockDevice.h"
#include "cachelib/navy/testing/SeqPoints.h"

using testing::_;
using testing::AtLeast;
using testing::InSequence;
using testing::Invoke;
using testing::NiceMock;
using testing::Return;
using testing::StrictMock;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
void setLayout(BigHash::Config& config, uint32_t bs, uint32_t numBuckets) {
  config.bucketSize = bs;
  config.cacheSize = uint64_t{bs} * numBuckets;
}
} // namespace

TEST(BigHash, InsertAndRemove) {
  BigHash::Config config;
  setLayout(config, 128, 2);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 128);
  config.device = device.get();

  BigHash bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  EXPECT_EQ(Status::Ok, bh.remove(makeHK("key")));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(Status::NotFound, bh.remove(makeHK("key")));
}

TEST(BigHash, SimpleStats) {
  BigHash::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 64);
  config.device = device.get();

  BigHash bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
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

TEST(BigHash, EvictionStats) {
  BigHash::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 64);
  config.device = device.get();

  BigHash bh(std::move(config));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key1"), makeView("12345"), {}));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key2"), makeView("123456789"), {}));
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

TEST(BigHash, DeviceErrorStats) {
  BigHash::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 64);
  config.device = device.get();

  BigHash bh(std::move(config));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key1"), makeView("1"), {}));
  EXPECT_CALL(*device, writeImpl(0, 64, _)).WillOnce(Return(false));
  EXPECT_EQ(Status::DeviceError, bh.insert(makeHK("key2"), makeView("1"), {}));
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

TEST(BigHash, DoubleInsert) {
  BigHash::Config config;
  setLayout(config, 128, 2);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 128);
  config.device = device.get();

  MockDestructor helper;
  config.destructorCb = toCallback(helper);

  BigHash bh(std::move(config));

  Buffer value;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  EXPECT_CALL(
      helper,
      call(makeView("key"), makeView("12345"), DestructorEvent::Removed));

  // Insert the same key a second time will overwrite the previous value.
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("45678"), {}));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("45678"), value.view());
}

TEST(BigHash, DestructorCallback) {
  BigHash::Config config;
  setLayout(config, 64, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 64);
  config.device = device.get();

  MockDestructor helper;
  EXPECT_CALL(
      helper,
      call(makeView("key 1"), makeView("value 1"), DestructorEvent::Recycled));
  EXPECT_CALL(
      helper,
      call(makeView("key 2"), makeView("value 2"), DestructorEvent::Removed));
  config.destructorCb = toCallback(helper);

  BigHash bh(std::move(config));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 1"), makeView("value 1"), {}));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 2"), makeView("value 2"), {}));
  EXPECT_EQ(Status::Ok, bh.remove(makeHK("key 2")));
}

TEST(BigHash, Reset) {
  BigHash::Config config;
  size_t bucketCount = 2;
  setLayout(config, 128, bucketCount);

  auto bf = std::make_unique<BloomFilter>(bucketCount, 2, 4);
  config.bloomFilter = std::move(bf);

  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 128);
  auto readFirstBucket = [& dev = device->getRealDeviceRef()] {
    // Read only the bucket after the initial generation and checksum fields
    // since the former can change with reset() while the latter can change
    // with every write.
    Buffer buf{92};
    dev.read(8, 92, buf.data());
    return buf;
  };
  config.device = device.get();

  BigHash bh(std::move(config));

  Buffer value;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());
  auto oldBucketContent = readFirstBucket();

  bh.reset();
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));

  // bh reset should reset the bloom filter as well.
  EXPECT_EQ(1, bh.bfRejectCount());

  // The new bucket content must be identical to that of the old since
  // after a reset, our first write is equivalent to writing to a brand
  // new bucket.
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
  auto newBucketContent = readFirstBucket();
  EXPECT_EQ(oldBucketContent.view(), newBucketContent.view());
}

TEST(BigHash, WriteInTwoBuckets) {
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

  BigHash::Config config;
  config.cacheBaseOffset = 256;
  setLayout(config, 128, 2);
  auto device = std::make_unique<StrictMock<MockDevice>>(
      config.cacheBaseOffset + config.cacheSize, 128);
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

  BigHash bh(std::move(config));

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("A"), makeView("12345"), {}));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("B"), makeView("45678"), {}));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("C"), makeView("67890"), {}));
}

TEST(BigHash, RemoveNotFound) {
  // Remove that is not found will not write to disk, but rebuild bloom filter
  // and second remove would not read the device.
  BigHash::Config config;
  setLayout(config, 128, 1);
  auto device = std::make_unique<StrictMock<MockDevice>>(config.cacheSize, 128);
  {
    InSequence inSeq;
    EXPECT_CALL(*device, readImpl(0, 128, _));
    EXPECT_CALL(*device, writeImpl(0, 128, _));
    EXPECT_CALL(*device, readImpl(0, 128, _));
    EXPECT_CALL(*device, writeImpl(0, 128, _));
    EXPECT_CALL(*device, readImpl(0, 128, _));
  }
  config.device = device.get();

  BigHash bh(std::move(config));

  bh.insert(makeHK("key"), makeView("12345"), {});
  bh.remove(makeHK("key"));
  bh.remove(makeHK("key"));
}

TEST(BigHash, CorruptBucket) {
  // Write a bucket, then corrupt a byte so we won't be able to read it
  BigHash::Config config;
  setLayout(config, 128, 2);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 128);
  config.device = device.get();

  BigHash bh(std::move(config));

  bh.insert(makeHK("key"), makeView("12345"), {});

  Buffer value;
  EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
  EXPECT_EQ(makeView("12345"), value.view());

  // Corrupt data. Use device directly to avoid alignment checks
  uint8_t badBytes[4] = {13, 17, 19, 23};
  device->getRealDeviceRef().write(10, sizeof(badBytes), badBytes);

  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("key"), value));
}

TEST(BigHash, Recovery) {
  BigHash::Config config;
  config.cacheSize = 16 * 1024;
  auto device = createMemoryDevice(config.cacheSize);
  config.device = device.get();

  BigHash bh(std::move(config));

  Buffer value;
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
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

TEST(BigHash, RecoveryBadConfig) {
  folly::IOBufQueue queue;
  {
    BigHash::Config config;
    config.cacheSize = 16 * 1024;
    config.bucketSize = 4 * 1024;
    auto device = createMemoryDevice(config.cacheSize);
    config.device = device.get();

    BigHash bh(std::move(config));

    Buffer value;
    EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHK("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    auto rw = createMemoryRecordWriter(queue);
    bh.persist(*rw);
  }
  {
    BigHash::Config config;
    // Config is different. Number of buckets the same, but size is different.
    config.cacheSize = 32 * 1024;
    config.bucketSize = 8 * 1024;
    auto device = createMemoryDevice(config.cacheSize);
    config.device = device.get();

    BigHash bh(std::move(config));
    auto rr = createMemoryRecordReader(queue);
    ASSERT_FALSE(bh.recover(*rr));
  }
}

TEST(BigHash, RecoveryCorruptedData) {
  BigHash::Config config;
  config.cacheSize = 1024 * 1024;
  auto device = createMemoryDevice(config.cacheSize);
  config.device = device.get();

  BigHash bh(std::move(config));

  Buffer value;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key"), makeView("12345"), {}));
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

TEST(BigHash, ConcurrentRead) {
  // In one bucket, we can have multiple readers at the same time.
  BigHash::Config config;
  setLayout(config, 128, 1);
  auto device = std::make_unique<NiceMock<MockDevice>>(config.cacheSize, 128);
  config.device = device.get();

  BigHash bh(std::move(config));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 1"), makeView("1"), {}));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 2"), makeView("2"), {}));
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("key 3"), makeView("3"), {}));

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

TEST(BigHash, BloomFilterRecoveryFail) {
  BigHash::Config config;
  setLayout(config, 128, 2);
  auto device = std::make_unique<StrictMock<MockDevice>>(config.cacheSize, 128);
  EXPECT_CALL(*device, readImpl(_, _, _)).Times(0);
  config.device = device.get();
  config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);
  auto* bf = config.bloomFilter.get();

  BigHash bh(std::move(config));

  EXPECT_TRUE(bf->getInitBit(0));
  Buffer value;
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("100"), value));
  EXPECT_TRUE(bf->getInitBit(0));

  // After a bad recovery, filters will not be affected
  auto ioBuf = folly::IOBuf::createCombined(512);
  std::generate(
      ioBuf->writableData(), ioBuf->writableTail(), std::minstd_rand());

  folly::IOBufQueue queue;
  auto rw = createMemoryRecordWriter(queue);
  rw->writeRecord(std::move(ioBuf));
  auto rr = createMemoryRecordReader(queue);
  ASSERT_FALSE(bh.recover(*rr));

  EXPECT_TRUE(bf->getInitBit(0));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("100"), value));
  EXPECT_TRUE(bf->getInitBit(0));
}

TEST(BigHash, BloomFilter) {
  BigHash::Config config;
  setLayout(config, 128, 2);
  auto device = createMemoryDevice(config.cacheSize);
  config.device = device.get();
  config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);

  MockDestructor helper;
  EXPECT_CALL(helper, call(makeView("100"), _, DestructorEvent::Recycled));
  EXPECT_CALL(helper, call(makeView("101"), _, DestructorEvent::Removed));
  config.destructorCb = toCallback(helper);

  BigHash bh(std::move(config));
  BufferGen bg;

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("100"), bg.gen(20).view(), {}));

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
  EXPECT_EQ(Status::Ok, bh.insert(makeHK("101"), bg.gen(20).view(), {}));
  EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("110"), value));
  EXPECT_EQ(2, bh.bfRejectCount());

  EXPECT_EQ(Status::Ok, bh.insert(makeHK("110"), bg.gen(20).view(), {}));
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
TEST(BigHash, BloomFilterRecoveryRebuild) {
  std::unique_ptr<Device> actual;
  folly::IOBufQueue queue;

  // Add a value to the first bucket and persist.
  {
    BigHash::Config config;
    setLayout(config, 128, 2);
    auto device =
        std::make_unique<StrictMock<MockDevice>>(config.cacheSize, 128);
    EXPECT_CALL(*device, readImpl(0, 128, _));
    EXPECT_CALL(*device, writeImpl(0, 128, _));
    config.device = device.get();
    config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);

    BigHash bh(std::move(config));
    EXPECT_EQ(Status::Ok, bh.insert(makeHK("100"), makeView("cat"), {}));
    auto rw = createMemoryRecordWriter(queue);
    bh.persist(*rw);

    actual = device->releaseRealDevice();
  }

  // Recover. BF is also recovered.
  {
    BigHash::Config config;
    setLayout(config, 128, 2);
    auto device = std::make_unique<MockDevice>(0, 128);
    device->setRealDevice(std::move(actual));
    EXPECT_CALL(*device, readImpl(0, 128, _)).Times(0);
    EXPECT_CALL(*device, readImpl(128, 128, _)).Times(0);
    config.device = device.get();
    config.bloomFilter = std::make_unique<BloomFilter>(2, 1, 4);
    auto* bf = config.bloomFilter.get();

    BigHash bh(std::move(config));
    auto rr = createMemoryRecordReader(queue);
    ASSERT_TRUE(bh.recover(*rr));

    EXPECT_TRUE(bf->getInitBit(0));
    EXPECT_EQ(Status::NotFound, bh.remove(makeHK("200")));
    EXPECT_EQ(1, bh.bfRejectCount());
    EXPECT_TRUE(bf->getInitBit(0));

    EXPECT_EQ(Status::NotFound, bh.remove(makeHK("200")));
    EXPECT_EQ(2, bh.bfRejectCount());
    Buffer value;
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("200"), value));
    EXPECT_EQ(3, bh.bfRejectCount());

    // Test lookup, second bucket
    EXPECT_TRUE(bf->getInitBit(1));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHK("201"), value));
    EXPECT_TRUE(bf->getInitBit(1));
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
