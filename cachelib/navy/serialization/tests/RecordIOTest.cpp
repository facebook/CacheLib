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

#include <folly/File.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
bool ioBufEquals(const folly::IOBuf& ioBuf, const char* expected) {
  folly::StringPiece str{reinterpret_cast<const char*>(ioBuf.data()),
                         ioBuf.length()};
  return folly::StringPiece{expected} == str;
}

void writeRecords(RecordWriter& rw) {
  {
    folly::IOBufQueue ioq;
    ioq.append(folly::IOBuf::copyBuffer("cat"));
    auto ioBuf = ioq.move();
    EXPECT_EQ(1, ioBuf->countChainElements());
    rw.writeRecord(std::move(ioBuf));
  }

  {
    folly::IOBufQueue ioq;
    ioq.append(folly::IOBuf::copyBuffer("frog"));
    ioq.append(folly::IOBuf::copyBuffer(" and "));
    ioq.append(folly::IOBuf::copyBuffer("toad"));
    auto ioBuf = ioq.move();
    EXPECT_EQ(3, ioBuf->countChainElements());
    rw.writeRecord(std::move(ioBuf));
  }
}

void checkRecords(RecordReader& rr) {
  EXPECT_FALSE(rr.isEnd());
  {
    auto rec = rr.readRecord();
    EXPECT_EQ(1, rec->countChainElements());
    EXPECT_TRUE(ioBufEquals(*rec, "cat"));
  }
  {
    auto rec = rr.readRecord();
    EXPECT_EQ(1, rec->countChainElements());
    EXPECT_TRUE(ioBufEquals(*rec, "frog and toad"));
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
  folly::IOBufQueue ioq;
  auto rw = createMemoryRecordWriter(ioq);
  writeRecords(*rw);
  auto rr = createMemoryRecordReader(ioq);
  checkRecords(*rr);
}

TEST(RecordIO, MemoryDevice) {
  auto metadataSize = 4 * 1024 * 1024;

  // Test various sizes of records
  std::vector<uint32_t> testSizes = {16,   33,    731,    4095,  4097,
                                     8193, 15977, 121903, 693728};
  for (size_t i = 0; i < testSizes.size(); i++) {
    auto dev = createMemoryDevice(10 * metadataSize, nullptr /* encryption */);
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
        } catch (std::logic_error& e) {
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
        } catch (std::logic_error& e) {
          // read should fail when we cannot write beyond the metadataSize
          EXPECT_EQ(j, failedIter);
          break;
        }
      }
    }
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
