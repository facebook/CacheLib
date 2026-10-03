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
#include <folly/io/RecordIO.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/navy/testing/MockDevice.h"

using testing::_;
using testing::NiceMock;

namespace facebook::cachelib::navy::tests {
namespace {
bool ioBufEquals(const folly::IOBuf& ioBuf, const char* expected) {
  folly::StringPiece str{reinterpret_cast<const char*>(ioBuf.data()),
                         ioBuf.length()};
  return folly::StringPiece{expected} == str;
}

constexpr std::string_view kStr1 = "cat";
constexpr std::string_view kStr2 = "frog";
constexpr std::string_view kStr3 = " and ";
constexpr std::string_view kStr4 = "toad";

void writeRecords(RecordWriter& rw) {
  {
    folly::IOBufQueue ioq;
    ioq.append(folly::IOBuf::copyBuffer(kStr1));
    auto ioBuf = ioq.move();
    EXPECT_EQ(1, ioBuf->countChainElements());
    rw.writeRecord(std::move(ioBuf));
    // FileRecordWriter and DeviceMetaDataWriter will write with the header
    // (from folly::recordio_helpers::prependHeader()), while MemoryRecordWriter
    // will just write the payload without header
    EXPECT_TRUE(kStr1.length() + folly::recordio_helpers::headerSize() ==
                    rw.getCurPos() ||
                kStr1.length() == rw.getCurPos());
  }

  {
    folly::IOBufQueue ioq;
    ioq.append(folly::IOBuf::copyBuffer(kStr2));
    ioq.append(folly::IOBuf::copyBuffer(kStr3));
    ioq.append(folly::IOBuf::copyBuffer(kStr4));
    auto ioBuf = ioq.move();
    EXPECT_EQ(3, ioBuf->countChainElements());
    auto prevPos = rw.getCurPos();
    rw.writeRecord(std::move(ioBuf));
    // FileRecordWriter and DeviceMetaDataWriter will write with the header,
    // while MemoryRecordWriter will just write the payload without header
    EXPECT_TRUE(prevPos + kStr2.length() + kStr3.length() + kStr4.length() +
                        folly::recordio_helpers::headerSize() ==
                    rw.getCurPos() ||
                prevPos + kStr2.length() + kStr3.length() + kStr4.length() ==
                    rw.getCurPos());
  }
}

void checkRecords(RecordReader& rr) {
  EXPECT_FALSE(rr.isEnd());
  {
    auto rec = rr.readRecord();
    EXPECT_EQ(1, rec->countChainElements());
    EXPECT_TRUE(ioBufEquals(*rec, kStr1.data()));
  }
  {
    auto rec = rr.readRecord();
    EXPECT_EQ(1, rec->countChainElements());
    std::string expectedStr =
        std::string(kStr2) + std::string(kStr3) + std::string(kStr4);
    EXPECT_TRUE(ioBufEquals(*rec, expectedStr.c_str()));
  }
  EXPECT_TRUE(rr.isEnd());
}
} // namespace

TEST(RecordIO, File) {
  folly::File tmp = folly::File::temporary();
  auto rw = createFileRecordWriter(tmp.fd());
  writeRecords(*rw);
  auto rr = createFileRecordReader(tmp.fd());
  checkRecords(*rr);
}

TEST(RecordIO, Memory) {
  folly::IOBufQueue ioq{folly::IOBufQueue::cacheChainLength()};
  auto rw = createMemoryRecordWriter(ioq);
  writeRecords(*rw);
  auto rr = createMemoryRecordReader(ioq);
  checkRecords(*rr);
}

/**
  MemoryDevice test has each data payload fixed 4k in size; with header size
  included would be larger than 4k.
  DeviceMetaDataWriter/DeviceMetaDataReader capped its capacity size with
  'metadataSize'.
  Each callable 'runTest' intended to write/read two payloads sequentially
  to/from DeviceMetaDataWriter/DeviceMetaDataReader.
*/
TEST(RecordIO, MemoryDevice) {
  constexpr uint32_t ioAlignSize = 4096;
  constexpr uint32_t testSize = 4096;
  constexpr char testChar = testSize % 26 + 'A';
  constexpr int32_t nIter = 2;

  auto runTest = [=](auto metadataSize, bool expectWriteFailed,
                     bool expectReadFailed) {
    int32_t failedIter = -1;
    bool writeFailed = false;
    bool readFailed = false;
    auto dev = createMemoryDevice(10 * metadataSize, nullptr /* encryption */,
                                  ioAlignSize);
    {
      auto rw = createMetadataRecordWriter(*dev, metadataSize);
      for (auto j = 0; j < nIter; j++) {
        auto wbuf = folly::IOBuf::create(testSize);
        wbuf->append(testSize);
        memset(wbuf->writableData(), testChar, testSize);
        try {
          rw->writeRecord(std::move(wbuf));
        } catch (std::logic_error&) {
          writeFailed = true;
          failedIter = j;
          break;
        }
      }
    }
    EXPECT_EQ(expectWriteFailed, writeFailed);

    {
      auto rr = createMetadataRecordReader(*dev, metadataSize);
      for (auto j = 0; j < nIter; j++) {
        try {
          auto rbuf = rr->readRecord();
          auto data = rbuf->data();
          for (uint32_t k = 0; k < testSize; k++) {
            EXPECT_EQ(data[k], testChar);
          }
        } catch (std::logic_error&) {
          readFailed = true;
          EXPECT_EQ(j, failedIter);
          break;
        }
      }
    }
    EXPECT_EQ(expectReadFailed, readFailed);
  };

  // Expecting both write/read to fail in first iteration due to data size
  // (header + payload) is greater than capped size 4k.
  runTest(4096 /* metadataSize */,
          true /* expectWriteFailed */,
          true /* expectReadFailed */);
  // Expecting both write/read to fail in second iteration due to data size
  // (header + payload) * 2 is greater than capped size 8k.
  runTest(8192 /* metadataSize */,
          true /* expectWriteFailed */,
          true /* expectReadFailed */);
  // Expecting both write/read to succeed while (header + payload) * 2 is under
  // capped size 16k.
  runTest(16384 /* metadataSize */,
          false /* expectWriteFailed */,
          false /* expectReadFailed */);
}

TEST(RecordIO, MemoryDeviceVariousPayloads) {
  auto metadataSize = 4 * 1024 * 1024;
  // Test various sizes of ioAlignSize start with 4096 with the number
  // being the power of 2.
  std::array<uint32_t, 3> ioAlignSizes = {4096, 8192, 16384};

  // Test various sizes of records
  std::vector<uint32_t> testSizes = {16,   33,    731,    4095,  4097,
                                     8193, 15977, 121903, 693728};

  for (auto ioAlignSize : ioAlignSizes) {
    for (size_t i = 0; i < testSizes.size(); i++) {
      auto dev = createMemoryDevice(10 * metadataSize, nullptr /* encryption */,
                                    ioAlignSize);
      auto testSize = testSizes[i];
      char testChar = testSize % 26 + 'A';
      uint32_t failedIter = 0;
      uint32_t nIter = 1000;
      {
        auto rw = createMetadataRecordWriter(*dev, metadataSize);
        for (uint32_t j = 0; j < nIter; j++) {
          auto wbuf = folly::IOBuf::create(testSize);
          wbuf->append(testSize);
          memset(wbuf->writableData(), testChar, testSize);
          try {
            rw->writeRecord(std::move(wbuf));
          } catch (std::logic_error&) {
            failedIter = j;
            break;
            /* ignore */
          }
        }
      }
      {
        auto rr = createMetadataRecordReader(*dev, metadataSize);
        for (uint32_t j = 0; j < nIter; j++) {
          try {
            auto rbuf = rr->readRecord();
            auto data = rbuf->data();
            for (uint32_t k = 0; k < testSize; k++) {
              EXPECT_EQ(data[k], testChar);
            }
          } catch (std::logic_error&) {
            // read should fail when we cannot write beyond the metadataSize
            EXPECT_EQ(j, failedIter);
            break;
          }
        }
      }
    }
  }
}

// The writer keeps whole blocks that already fit and drops only the final
// block when the stream reaches the end of the metadata region.
TEST(RecordIO, PartialTailIsDroppedOneBlock) {
  constexpr uint32_t ioAlignSize = 4096;
  constexpr uint64_t metadataSize = 8 * ioAlignSize;
  const uint32_t fullBlock =
      ioAlignSize - folly::recordio_helpers::headerSize();

  auto dev = createMemoryDevice(10 * metadataSize, nullptr, ioAlignSize);
  {
    auto rw = createMetadataRecordWriter(*dev, metadataSize);
    for (int i = 0; i < 7; i++) {
      auto wbuf = folly::IOBuf::create(fullBlock);
      wbuf->append(fullBlock);
      memset(wbuf->writableData(), 'A' + i, fullBlock);
      rw->writeRecord(std::move(wbuf));
    }
    auto tail = folly::IOBuf::create(100);
    tail->append(100);
    memset(tail->writableData(), 'Z', 100);
    rw->writeRecord(std::move(tail));
  }

  auto rr = createMetadataRecordReader(*dev, metadataSize);
  for (int i = 0; i < 7; i++) {
    ASSERT_FALSE(rr->isEnd());
    auto rbuf = rr->readRecord();
    ASSERT_NE(nullptr, rbuf);
    EXPECT_EQ(fullBlock, rbuf->length());
    EXPECT_EQ('A' + i, rbuf->data()[0]);
  }
  EXPECT_TRUE(rr->isEnd());
}

// A larger staging size means fewer, bigger device writes; the bytes written
// are the same either way.
TEST(RecordIO, StagingSizeBatchesWritesWithoutChangingImage) {
  constexpr uint64_t metadataSize = 4 * 1024 * 1024;
  constexpr uint32_t recSize = 3000;
  constexpr int nRecs = 500;

  auto run = [&](uint32_t ioAlignSize, size_t stagingSize) {
    auto dev =
        std::make_unique<NiceMock<MockDevice>>(2 * metadataSize, ioAlignSize);
    uint32_t writes = 0;
    ON_CALL(*dev, writeImpl(_, _, _, _))
        .WillByDefault(
            [&](uint64_t offset, uint32_t size, const void* data, int) {
              writes++;
              auto& real = dev->getRealDeviceRef();
              Buffer buffer = real.makeIOBuffer(size);
              memcpy(buffer.data(), data, size);
              return real.write(offset, std::move(buffer));
            });
    {
      auto rw = createMetadataRecordWriter(*dev, metadataSize, stagingSize);
      for (int i = 0; i < nRecs; i++) {
        auto wbuf = folly::IOBuf::create(recSize);
        wbuf->append(recSize);
        memset(wbuf->writableData(), 'A' + (i % 26), recSize);
        rw->writeRecord(std::move(wbuf));
      }
    }
    Buffer img{metadataSize, ioAlignSize};
    EXPECT_TRUE(dev->getRealDeviceRef().read(0, metadataSize, img.data()));
    auto s = std::string(reinterpret_cast<char*>(img.data()), metadataSize);
    // Guards against the mock swallowing the writes and leaving every run
    // comparing the same empty image.
    EXPECT_NE(std::string(metadataSize, '\0'), s);
    return std::make_pair(s, writes);
  };

  for (uint32_t ioAlignSize : {4096u, 16384u}) {
    const auto oneBlock = run(ioAlignSize, ioAlignSize);
    // Values below a block and values that are not a block multiple are
    // clamped, so they must land on the same image.
    for (size_t stagingSize : {size_t{0}, size_t{1}, size_t{4095},
                               size_t{100000}, size_t{4 * 1024 * 1024}}) {
      EXPECT_EQ(oneBlock.first, run(ioAlignSize, stagingSize).first)
          << "align=" << ioAlignSize << " staging=" << stagingSize;
    }
    EXPECT_LT(run(ioAlignSize, 4 * 1024 * 1024).second, oneBlock.second);
  }
}

} // namespace facebook::cachelib::navy::tests
