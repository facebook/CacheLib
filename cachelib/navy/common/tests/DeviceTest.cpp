#include "cachelib/navy/common/Device.h"

#include <thread>

#include <folly/File.h>
#include <folly/ScopeGuard.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

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
  EXPECT_TRUE(device.write(0, 5, nullptr));
  EXPECT_TRUE(device.write(0, 9, nullptr));
  EXPECT_EQ(14, device.getBytesWritten());
  EXPECT_FALSE(device.write(0, 1, nullptr));
  EXPECT_EQ(14, device.getBytesWritten());
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
  device.read(0, 1, nullptr);
  device.write(0, 1, nullptr);

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
  device.read(0, 1, nullptr);
  device.write(0, 1, nullptr);

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
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p50"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p90"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p99"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p9999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p99999"), 0));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_read_latency_us_p999999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_errors"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_p100"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p50"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p90"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p99"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p9999"), 0));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_write_latency_us_p99999"), 0));
  EXPECT_CALL(visitor,
              call(strPiece("navy_device_write_latency_us_p999999"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_p100"), 0));
  device.getCounters(toCallback(visitor));
}

TEST(Device, RAID0IO) {
  auto filePath =
      boost::filesystem::unique_path("/tmp/DEVICE_RAID0IO_TEST-%%%%-%%%%-%%%%");
  boost::filesystem::create_directories(filePath);
  SCOPE_EXIT { boost::filesystem::remove_all(filePath); };

  std::vector<std::string> files = {
      filePath.string() + "/CACHE0", filePath.string() + "/CACHE1",
      filePath.string() + "/CACHE2", filePath.string() + "/CACHE3"};

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

  auto device = createDirectIoRAID0Device(fdvec, blockSize, stripeSize);

  // Simple IO
  {
    Buffer wbuf = device->makeIOBuffer(stripeSize);
    Buffer rbuf = device->makeIOBuffer(stripeSize);
    std::memset(wbuf.data(), 'A', stripeSize);
    auto ret = device->write(0, stripeSize, wbuf.data());
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
    auto ret = device->write(1024, stripeSize, wbuf.data());
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
    auto ret = device->write(offset, ioSize, wbuf.data());
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
    auto ret = device->write(offset, ioSize, wbuf.data());
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
