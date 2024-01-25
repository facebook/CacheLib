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
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>

#include "cachelib/common/Utils.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"
#include "cachelib/navy/testing/MockDevice.h"

using testing::_;

namespace facebook::cachelib::navy::tests {
TEST(Device, BytesWritten) {
  MockDevice device{100, 1};
  EXPECT_CALL(device, writeImpl(_, _, _, _))
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
  device.getRealDeviceRef().getCounters({toCallback(visitor)});
}

TEST(Device, EncryptorFail) {
  class MockEncryptor : public DeviceEncryptor {
   public:
    uint32_t encryptionBlockSize() const override { return 512; }

    bool encrypt(folly::MutableByteRange /* value */, uint64_t salt) override {
      return salt != 10;
    }

    bool decrypt(folly::MutableByteRange /* value */, uint64_t salt) override {
      return salt != 15;
    }
  };

  try {
    MockDevice device{100, 1024, std::make_shared<MockEncryptor>()};
  } catch (const std::invalid_argument& e) {
    EXPECT_EQ(
        e.what(),
        std::string("Invalid ioAlignSize 1024 encryption block size 512"));
  }
}

TEST(Device, Latency) {
  // Device size must be at least 1 because we try to write 1 byte to it
  MockDevice device{1, 1};
  EXPECT_CALL(device, readImpl(0, 1, _))
      .WillOnce(testing::InvokeWithoutArgs([] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        return true;
      }));
  EXPECT_CALL(device, writeImpl(0, 1, _, _))
      .WillOnce(testing::InvokeWithoutArgs([] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        return true;
      }));

  Buffer buf{1};
  device.read(0, 1, buf.data());
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
  // Device size must be at least 1 because we try to write 1 byte to it
  MockDevice device{1, 1};
  EXPECT_CALL(device, readImpl(0, 1, _))
      .WillOnce(testing::InvokeWithoutArgs([] { return false; }));
  EXPECT_CALL(device, writeImpl(0, 1, _, _))
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
  device.getCounters({toCallback(visitor)});
}

TEST(Device, Stats) {
  MockDevice device{0, 1};
  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(strPiece("navy_device_bytes_written"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_bytes_read"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_errors"), 0));
  EXPECT_CALL(visitor, call(strPiece("navy_device_read_latency_us_avg"), 0));
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
  EXPECT_CALL(visitor, call(strPiece("navy_device_write_latency_us_avg"), 0));
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
  device.getCounters({toCallback(visitor)});
}

struct DeviceParamTest
    : public testing::TestWithParam<std::tuple<IoEngine, int>> {
  DeviceParamTest()
      : ioEngine_(std::get<0>(GetParam())), qDepth_(std::get<1>(GetParam())) {
    XLOGF(INFO, "DeviceParamTest: ioEngine={}, qDepth={}",
          getIoEngineName(ioEngine_), qDepth_);
  }

 protected:
  std::shared_ptr<Device> createFileDevice(
      std::vector<folly::File> fVec,
      uint64_t fileSize,
      uint32_t blockSize,
      uint32_t stripeSize,
      uint32_t maxDeviceWriteSize,
      std::shared_ptr<DeviceEncryptor> encryptor) {
    device_ = createDirectIoFileDevice(std::move(fVec),
                                       {},
                                       fileSize,
                                       blockSize,
                                       stripeSize,
                                       maxDeviceWriteSize,
                                       ioEngine_,
                                       qDepth_,
                                       false,
                                       std::move(encryptor));
    return device_;
  }

  std::shared_ptr<Device> getDevice() const { return device_; }

  IoEngine ioEngine_;
  uint32_t qDepth_;
  std::shared_ptr<Device> device_;
};

TEST_P(DeviceParamTest, MaxWriteSize) {
  auto filePath = folly::sformat("/tmp/DEVICE_MAXWRITE_TEST-{}", ::getpid());

  int deviceSize = 16 * 1024;
  int ioAlignSize = 1024;
  std::vector<folly::File> fVec;
  fVec.emplace_back(filePath, O_RDWR | O_CREAT, S_IRWXU);

  auto device = createFileDevice(std::move(fVec), deviceSize, ioAlignSize,
                                 ioAlignSize, 1024, nullptr);
  uint32_t bufSize = 4 * 1024;
  Buffer wbuf = device->makeIOBuffer(bufSize);
  Buffer rbuf = device->makeIOBuffer(bufSize);
  auto wdata = wbuf.data();
  for (uint32_t i = 0; i < bufSize; i++) {
    wdata[i] = folly::Random::rand32() % 64;
  }
  auto ret = device->write(0, wbuf.copy(ioAlignSize));
  EXPECT_EQ(true, ret);

  ret = device->read(0, bufSize, rbuf.data());
  EXPECT_EQ(true, ret);
  auto rdata = rbuf.data();
  for (uint32_t i = 0; i < bufSize; i++) {
    EXPECT_EQ(wdata[i], rdata[i]);
  }

  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(_, _)).WillRepeatedly(testing::Return());
  EXPECT_CALL(visitor, call(strPiece("navy_device_bytes_written"), 4096));
  EXPECT_CALL(visitor, call(strPiece("navy_device_bytes_read"), 4096));
  device->getCounters({toCallback(visitor)});
}

TEST_P(DeviceParamTest, RAID0IO) {
  auto filePath = folly::sformat("/tmp/DEVICE_RAID0IO_TEST-{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 4 * 1024 * 1024;
  int ioAlignSize = 4096;
  int stripeSize = 8192;

  std::vector<folly::File> fvec;
  for (const auto& file : files) {
    auto f = folly::File(file.c_str(), O_RDWR | O_CREAT);
    auto ret = ::fallocate(f.fd(), 0, 0, size);
    EXPECT_EQ(0, ret);
    fvec.push_back(std::move(f));
  }
  auto vecSize = fvec.size();
  auto device = createFileDevice(std::move(fvec),
                                 size,
                                 ioAlignSize,
                                 stripeSize,
                                 0 /* max device write size */,
                                 nullptr /* encryption */);

  EXPECT_EQ(vecSize * size, device->getSize());

  // Simple IO
  {
    Buffer wbuf = device->makeIOBuffer(stripeSize);
    Buffer rbuf = device->makeIOBuffer(stripeSize);
    std::memset(wbuf.data(), 'A', stripeSize);
    auto ret = device->write(0, wbuf.copy(ioAlignSize));
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
    auto ret = device->write(ioAlignSize, wbuf.copy(ioAlignSize));
    EXPECT_EQ(true, ret);
    ret = device->read(ioAlignSize, stripeSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), stripeSize);
    EXPECT_EQ(0, rc);
  }
  // IO spans several stripes
  {
    auto ioSize = 10 * stripeSize;
    auto offset = stripeSize * 7 + ioAlignSize;
    Buffer wbuf = device->makeIOBuffer(ioSize);
    Buffer rbuf = device->makeIOBuffer(ioSize);
    std::memset(wbuf.data(), 'C', stripeSize);
    auto ret = device->write(offset, wbuf.copy(ioAlignSize));
    EXPECT_EQ(true, ret);
    ret = device->read(offset, ioSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), ioSize);
    EXPECT_EQ(0, rc);
  }
  // IO size < stripeSize
  {
    auto ioSize = stripeSize / 2;
    auto offset = stripeSize * 22 + ioAlignSize;
    Buffer wbuf = device->makeIOBuffer(ioSize);
    Buffer rbuf = device->makeIOBuffer(ioSize);
    std::memset(wbuf.data(), 'D', ioSize);
    auto ret = device->write(offset, wbuf.copy(ioAlignSize));
    EXPECT_EQ(true, ret);
    ret = device->read(offset, ioSize, rbuf.data());
    EXPECT_EQ(true, ret);
    auto rc = std::memcmp(wbuf.data(), rbuf.data(), ioSize);
    EXPECT_EQ(0, rc);
  }
}

TEST_P(DeviceParamTest, RAID0IOAlignment) {
  // The goal of this test is to ensure we cannot create a RAID0 device
  // if each individual device is not aligned to stripe size. This is to
  // test against a bug that was uncovered in T68874972.
  auto filePath = folly::sformat("/tmp/DEVICE_RAID0IO_TEST-{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 4 * 1024 * 1024;
  int ioAlignSize = 4096;
  int stripeSize = 8192;

  std::vector<folly::File> fvec;
  for (const auto& file : files) {
    auto f = folly::File(file.c_str(), O_RDWR | O_CREAT);
    auto ret = ::fallocate(f.fd(), 0, 0, size);
    EXPECT_EQ(0, ret);
    fvec.push_back(std::move(f));
  }

  // Update individual device size to something smaller but the overall size
  // of all the devices is still aligned on stripe size.
  size = 2 * 1024 * 1024 + stripeSize / fvec.size();
  ASSERT_THROW(createFileDevice(std::move(fvec),
                                size,
                                ioAlignSize,
                                stripeSize,
                                0 /* max device write size */,
                                nullptr /* encryption */),
               std::invalid_argument);
}

INSTANTIATE_TEST_SUITE_P(DeviceParamTestSuite,
                         DeviceParamTest,
                         testing::Values(std::make_tuple(IoEngine::Sync, 0),
                                         std::make_tuple(IoEngine::LibAio, 1),
                                         std::make_tuple(IoEngine::IoUring,
                                                         1)));

} // namespace facebook::cachelib::navy::tests
