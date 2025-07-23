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

} // namespace facebook::cachelib::navy::tests
