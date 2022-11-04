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

#include <folly/Format.h>
#include <gtest/gtest.h>

#include <string>

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
std::string replicate(const std::string& s, size_t times) {
  std::string rv;
  for (size_t i = 0; i < times; i++) {
    rv.append(s);
  }
  return rv;
}
} // namespace

TEST(Buffer, Test) {
  auto b1 = Buffer{makeView("hello world")};
  auto v1 = b1.view().slice(3, 5);
  EXPECT_EQ(makeView("lo wo"), v1);
  auto b2 = Buffer{v1};
  EXPECT_EQ(makeView("lo wo"), b2.view());
  b1 = Buffer{makeView("12345")};
  EXPECT_EQ(makeView("12345"), b1.view());
  auto b3 = std::move(b1);
  EXPECT_TRUE(b1.isNull());
  EXPECT_EQ(makeView("12345"), b3.view());
  b3.shrink(3);
  b1 = std::move(b3);
  EXPECT_EQ(makeView("123"), b1.view());
  auto b1Copy = b1.copy();
  EXPECT_EQ(makeView("123"), b1.view());
  EXPECT_EQ(makeView("123"), b1Copy.view());
}

TEST(Buffer, TestWithTrim) {
  auto b1 = Buffer{makeView("123hello world")};
  b1.trimStart(3);
  auto v1 = b1.view().slice(3, 5);
  EXPECT_EQ(makeView("lo wo"), v1);
  auto b2 = Buffer{v1};
  EXPECT_EQ(makeView("lo wo"), b2.view());
  b1 = Buffer{makeView("12345")};
  EXPECT_EQ(makeView("12345"), b1.view());
  auto b3 = std::move(b1);
  EXPECT_TRUE(b1.isNull());
  EXPECT_EQ(makeView("12345"), b3.view());
  b3.shrink(3);
  b1 = std::move(b3);
  EXPECT_EQ(makeView("123"), b1.view());
  auto b1Copy = b1.copy();
  EXPECT_EQ(makeView("123"), b1.view());
  EXPECT_EQ(makeView("123"), b1Copy.view());
}

TEST(Buffer, ToStringText) {
  auto v = makeView("abc 10");
  auto s = toString(v);
  EXPECT_EQ("BufferView \"abc 10\"", s);
}

TEST(Buffer, ToStringBinary) {
  {
    auto v = makeView("aZ \x08");
    auto s = toString(v);
    EXPECT_EQ("BufferView size=4 <615a2008>", s);
  }
  {
    auto v = makeView("aZ \xfe");
    auto s = toString(v);
    EXPECT_EQ("BufferView size=4 <615a20fe>", s);
  }
}

TEST(Buffer, ToStringBinaryLong) {
  // Test @replicate first:
  EXPECT_EQ("ab", replicate("ab", 1));
  EXPECT_EQ("ababab", replicate("ab", 3));

  Buffer buf(81);
  std::memset(buf.data(), 0x07, buf.size());
  auto v = buf.view();

  const auto formatExpected = [](size_t size, const std::string& data) {
    return folly::sformat("BufferView size={} <{}>", size, data);
  };
  EXPECT_EQ(formatExpected(81, replicate("07", 80) + "..."), toString(v));
  EXPECT_EQ(formatExpected(81, replicate("07", 81)), toString(v, false));
}

TEST(Buffer, Alignment) {
  // Save buffers in vector to not let the system reuse memory
  std::vector<Buffer> buffers;
  for (int i = 0; i < 5; i++) {
    Buffer buf{6 * 1024, 1024};
    EXPECT_EQ(0, reinterpret_cast<uintptr_t>(buf.data()) % 1024);
    buffers.push_back(std::move(buf));
  }
  auto copy = buffers[0].copy(2048);
  EXPECT_EQ(0, reinterpret_cast<uintptr_t>(copy.data()) % 2048);
}

TEST(Buffer, Equals) {
  EXPECT_EQ(makeView("abc"), makeView("abc"));
  EXPECT_NE(makeView("abc"), makeView("abx"));
  EXPECT_NE(BufferView{}, makeView("abc"));
  EXPECT_NE(makeView("abc"), BufferView{});
  EXPECT_EQ(BufferView{}, BufferView{});
}

TEST(Buffer, CopyFrom) {
  Buffer buf{makeView("12345")};
  buf.copyFrom(1, makeView("abc"));
  EXPECT_EQ(makeView("1abc5"), buf.view());
}

TEST(Buffer, CopyFromWithTrim) {
  Buffer buf{makeView("000012345")};
  buf.trimStart(4);
  buf.copyFrom(1, makeView("abc"));
  EXPECT_EQ(makeView("1abc5"), buf.view());
}

TEST(Buffer, MutableView) {
  Buffer buf{makeView("12345")};

  auto mutableView = buf.mutableView();
  std::fill(mutableView.data(), mutableView.dataEnd(), 'b');

  for (size_t i = 0; i < buf.size(); i++) {
    EXPECT_EQ('b', buf.data()[i]) << i;
  }
}

TEST(Buffer, MutableViewWithTrim) {
  Buffer buf{makeView("aa12345")};

  buf.trimStart(2);
  auto mutableView = buf.mutableView();
  std::fill(mutableView.data(), mutableView.dataEnd(), 'c');

  for (size_t i = 0; i < buf.size(); i++) {
    EXPECT_EQ('c', buf.data()[i]) << i;
  }
}

TEST(Buffer, CopyTo) {
  auto view = makeView("12345");
  char dst[]{"hello world."};

  view.copyTo(dst + 6);
  EXPECT_STREQ("hello 12345.", dst);
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
