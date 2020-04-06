#include <thread>

#include <folly/File.h>
#include <folly/ScopeGuard.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/common/Utils.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"
#include "cachelib/navy/testing/MockDevice.h"

using testing::_;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Device, BytesWritten) {
  MockDevice device{100, 1};
  EXPECT_CALL(device, writeImpl(_, _, _))
      .WillOnce(testing::Return(true))
      .WillOnce(testing::Return(true))
      .WillOnce(testing::Return(false));
  EXPECT_TRUE(device.write(0, Buffer{5}));
  EXPECT_TRUE(device.write(0, Buffer{9}));
  EXPECT_EQ(14, device.getBytesWritten());
  EXPECT_FALSE(device.write(0, Buffer{1}));
  EXPECT_EQ(14, device.getBytesWritten());
}

TEST(Device, Encryptor) {
  class MockEncryptor : public DeviceEncryptor {
   public:
    uint32_t encryptionBlockSize() const override { return 1; }

    bool encrypt(folly::MutableByteRange /* value */, uint64_t salt) override {
      return salt != 10;
    }

    bool decrypt(folly::MutableByteRange /* value */, uint64_t salt) override {
      return salt != 15;
    }
  };

  MockDevice device{100, 1, std::make_shared<MockEncryptor>()};

  BufferGen bufGen;
  Buffer testBuffer = bufGen.gen(9);
  device.write(0, testBuffer.copy());
  device.write(10, testBuffer.copy());
  std::array<uint8_t, 9> value;
  device.read(0, 9, value.data());
  EXPECT_EQ(testBuffer.view(), (BufferView{value.size(), value.data()}));
  device.read(15, 9, value.data());

  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(_, _)).WillRepeatedly(testing::Return());
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_encryption_errors"), testing::Eq(1)));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_decryption_errors"), testing::Eq(1)));
  device.getRealDeviceRef().getCounters(toCallback(visitor));
}

TEST(Device, Latency) {
  MockDevice device{0, 1};
  EXPECT_CALL(device, readImpl(0, 1, _))
      .WillOnce(testing::InvokeWithoutArgs([] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        return true;
      }));
  EXPECT_CALL(device, writeImpl(0, 1, _))
      .WillOnce(testing::InvokeWithoutArgs([] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        return true;
      }));

  Buffer buf{1};
  device.read(0, 1, nullptr);
  device.write(0, std::move(buf));

  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(_, _)).WillRepeatedly(testing::Return());
  EXPECT_CALL(
      visitor,
      call(strPiece("navy_device_read_latency_us_p50"), testing::Ge(100'000)));
  EXPECT_CALL(
      visitor,
      call(strPiece("navy_device_write_latency_us_p50"), testing::Ge(100'000)));
  device.getCounters(toCallback(visitor));
}

TEST(Device, IOError) {
  MockDevice device{0, 1};
  EXPECT_CALL(device, readImpl(0, 1, _))
      .WillOnce(testing::InvokeWithoutArgs([] { return false; }));
  EXPECT_CALL(device, writeImpl(0, 1, _))
      .WillOnce(testing::InvokeWithoutArgs([] { return false; }));

  Buffer buf{1};
  device.read(0, 1, nullptr);
  device.write(0, std::move(buf));

  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(_, _)).WillRepeatedly(testing::Return());
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_read_errors"), testing::Eq(1)));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_write_errors"), testing::Eq(1)));
  device.getCounters(toCallback(visitor));
}

TEST(Device, Stats) {
  MockDevice device{0, 1};
  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(strPiece("navy_device_bytes_written"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_errors"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_min"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p5"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p10"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p25"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p50"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p75"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p90"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p95"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p99"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p9999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p99999"), 0));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_read_latency_us_p999999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_max"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_errors"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_min"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p5"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p10"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p25"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p50"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p75"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p90"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p95"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p99"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p9999"), 0));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_write_latency_us_p99999"), 0));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_write_latency_us_p999999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_encryption_errors"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_decryption_errors"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_max"), 0));
  device.getCounters(toCallback(visitor));
}

TEST(Device, RAID0IO) {
  auto filePath = folly::sformat("/tmp/DEVICE_RAID0IO_TEST-{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 4 * 1024 * 1024;
  int blockSize = 4096;
  int stripeSize = 8192;

  std::vector<int> fdvec;
  for (const auto& file : files) {
    auto f = folly::File(file.c_str(), O_RDWR | O_CREAT);
    auto ret = ::fallocate(f.fd(), 0, 0, size);
    EXPECT_EQ(0, ret);
    fdvec.push_back(f.release());
  }

  auto device = createDirectIoRAID0Device(fdvec,
                                          blockSize,
                                          stripeSize,
                                          nullptr /* encryption */,
                                          0 /* max device write size */);

  // Simple IO
  {
    Buffer wbuf = device->makeIOBuffer(stripeSize);
    Buffer rbuf = device->makeIOBuffer(stripeSize);
    std::memset(wbuf.data(), 'A', stripeSize);
    auto ret = device->write(0, wbuf.copy());
    EXPECT_EQ(true, ret);
    ret = device->read(0, stripeSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), stripeSize);
    EXPECT_EQ(0, rc);
  }
  // IO spans two stripes
  {
    Buffer wbuf = device->makeIOBuffer(stripeSize);
    Buffer rbuf = device->makeIOBuffer(stripeSize);
    std::memset(wbuf.data(), 'B', stripeSize);
    auto ret = device->write(1024, wbuf.copy());
    EXPECT_EQ(true, ret);
    ret = device->read(1024, stripeSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), stripeSize);
    EXPECT_EQ(0, rc);
  }
  // IO spans several stripes
  {
    auto ioSize = 10 * stripeSize;
    auto offset = stripeSize * 7 + 2048;
    Buffer wbuf = device->makeIOBuffer(ioSize);
    Buffer rbuf = device->makeIOBuffer(ioSize);
    std::memset(wbuf.data(), 'C', stripeSize);
    auto ret = device->write(offset, wbuf.copy());
    EXPECT_EQ(true, ret);
    ret = device->read(offset, ioSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), ioSize);
    EXPECT_EQ(0, rc);
  }
  // IO size < stripeSize
  {
    auto ioSize = stripeSize / 2;
    auto offset = stripeSize * 22 + 512;
    Buffer wbuf = device->makeIOBuffer(ioSize);
    Buffer rbuf = device->makeIOBuffer(ioSize);
    std::memset(wbuf.data(), 'D', ioSize);
    auto ret = device->write(offset, wbuf.copy());
    EXPECT_EQ(true, ret);
    ret = device->read(offset, ioSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), ioSize);
    EXPECT_EQ(0, rc);
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
