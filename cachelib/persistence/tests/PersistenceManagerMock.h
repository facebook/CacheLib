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

#pragma once

#include <gmock/gmock.h>

#include "cachelib/persistence/PersistenceManager.h"
#include "folly/io/IOBuf.h"
#include "folly/io/IOBufQueue.h"

namespace facebook::cachelib::tests {

using namespace persistence;

using Cache = cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
using CacheConfig = typename Cache::Config;
using CacheKey = typename Cache::Key;
using CacheWriteHandle = typename Cache::WriteHandle;

using ::testing::Action;
using ::testing::An;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;

class MockPersistenceStreamReaderImpl : public PersistenceStreamReader {
 public:
  MockPersistenceStreamReaderImpl(const uint8_t* buf, size_t len)
      : buf_(buf),
        length_(len),
        readAction(Invoke([&](size_t length) { return readImpl(length); })),
        readCharAction(Invoke([&]() { return readImpl(); })) {
    ON_CALL(*this, read(An<size_t>())).WillByDefault(readAction);
    ON_CALL(*this, read()).WillByDefault(readCharAction);
  }

  MOCK_METHOD1(read, folly::IOBuf(size_t));
  MOCK_METHOD0(read, char());

  /* Set expections for read function at the n-th call */
  void ExpectCallAt(const Action<folly::IOBuf(size_t)>& action, int n) {
    auto& expect = EXPECT_CALL(*this, read(An<size_t>()));
    for (int i = 0; i < n - 1; ++i) {
      expect.WillOnce(readAction);
    }
    expect.WillOnce(action).WillRepeatedly(readAction);
  }

  /* Set expections for read function at the n-th call */
  void ExpectCharCallAt(Action<char()> action, int n) {
    auto& expect = EXPECT_CALL(*this, read());
    for (int i = 0; i < n - 1; ++i) {
      expect.WillOnce(readCharAction);
    }
    expect.WillOnce(action).WillRepeatedly(readCharAction);
  }

  folly::IOBuf readImpl(size_t length) {
    if (length > length_) {
      XLOG(ERR, "can't read");
      return {};
    }
    folly::IOBuf buf(folly::IOBuf::CopyBufferOp::COPY_BUFFER, buf_, length);
    buf_ += length;
    length_ -= length;
    return buf;
  }

  char readImpl() {
    if (length_ > 0) {
      char c = static_cast<char>(*buf_);
      ++buf_;
      --length_;
      return c;
    }
    XLOG(ERR, "can't read");
    return char(0);
  }

 private:
  const uint8_t* buf_;
  size_t length_;
  Action<folly::IOBuf(size_t)> readAction;
  Action<char()> readCharAction;
};

class MockPersistenceStreamWriterImpl : public PersistenceStreamWriter {
 public:
  MockPersistenceStreamWriterImpl() = default;

  MockPersistenceStreamWriterImpl(folly::IOBuf* buf, size_t capacity)
      : buf_(buf),
        capacity_(capacity),
        writeAction(Invoke([&](auto&& buf) { writeImpl(std::move(buf)); })),
        writeCharAction(Invoke([&](char c) { writeImpl(c); })) {
    buf_->clear();
    ON_CALL(*this, write(An<folly::IOBuf>())).WillByDefault(writeAction);
    ON_CALL(*this, write(An<char>())).WillByDefault(writeCharAction);
    ON_CALL(*this, flush()).WillByDefault(Invoke([&]() { flushImpl(); }));
  }

  explicit MockPersistenceStreamWriterImpl(folly::IOBuf* buf)
      : MockPersistenceStreamWriterImpl(buf, buf->capacity()) {}

  MOCK_METHOD1(write, void(folly::IOBuf));
  MOCK_METHOD1(write, void(char c));
  MOCK_METHOD0(flush, void());

  /* Set expections for write function at the n-th call */
  void ExpectCallAt(const Action<void(folly::IOBuf)>& action, int n) {
    auto& expect = EXPECT_CALL(*this, write(An<folly::IOBuf>()));
    for (int i = 0; i < n - 1; ++i) {
      expect.WillOnce(writeAction);
    }
    expect.WillOnce(action).WillRepeatedly(writeAction);
  }

  /* Set expections for write function at the n-th call */
  void ExpectCharCallAt(Action<void(char c)> action, int n) {
    auto& expect = EXPECT_CALL(*this, write(An<char>()));
    for (int i = 0; i < n - 1; ++i) {
      expect.WillOnce(writeCharAction);
    }
    expect.WillOnce(action).WillRepeatedly(writeCharAction);
  }

  /* saves the buf arguments in write function to a vector */
  void saveBuffers(std::vector<folly::IOBuf>& bufs) {
    writeAction = Invoke([&](auto&& buf) {
      bufs.emplace_back(folly::IOBuf::CopyBufferOp::COPY_BUFFER, buf.data(),
                        buf.length());
    });
    writeCharAction = Return();
    ON_CALL(*this, write(An<folly::IOBuf>())).WillByDefault(writeAction);
  }

  void writeImpl(folly::IOBuf buffer) {
    CACHELIB_CHECK_THROW(buffer.length() <= capacity_, "over capacity");
    queue_.append(buffer);
    capacity_ -= buffer.length();
  }

  void writeImpl(char c) {
    CACHELIB_CHECK_THROW(capacity_ > 0, "over capacity");
    queue_.append(&c, sizeof(bool));
    --capacity_;
  }

  void flushImpl() {
    auto buf = queue_.move();
    for (auto& br : *buf) {
      std::memcpy(buf_->writableTail(), br.data(), br.size());
      buf_->append(br.size());
    }
  }

 private:
  folly::IOBuf* buf_ = nullptr;
  folly::IOBufQueue queue_;
  size_t capacity_ = 0;
  Action<void(folly::IOBuf)> writeAction;
  Action<void(char)> writeCharAction;
};

// avoid gMock warning messages for uninteresting calls
using MockPersistenceStreamReader = NiceMock<MockPersistenceStreamReaderImpl>;
using MockPersistenceStreamWriter = NiceMock<MockPersistenceStreamWriterImpl>;

} // namespace facebook::cachelib::tests
