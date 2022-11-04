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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/nvmcache/NvmItem.h"
namespace facebook {
namespace cachelib {
namespace tests {

namespace {
std::string genRandomStr(size_t len) {
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; i++) {
    char c = static_cast<char>(folly::Random::rand32(1, 256));
    s.push_back(c);
  }
  return s;
}
} // namespace

TEST(NvmItemTest, SingleBlob) {
  folly::StringPiece data{"helloworld"};
  uint32_t origSize = 5;
  Blob blob{origSize, data};
  size_t bufSize = NvmItem::estimateVariableSize(blob);
  auto nvmItem = std::unique_ptr<NvmItem>(new (bufSize) NvmItem(1, 1, 1, blob));
  ASSERT_EQ(1, nvmItem->getNumBlobs());
  ASSERT_EQ(data, nvmItem->getBlob(0).data);
  ASSERT_EQ(origSize, nvmItem->getBlob(0).origAllocSize);
  ASSERT_THROW(nvmItem->getBlob(folly::Random::rand32() + 1),
               std::invalid_argument);
}

TEST(NvmItemTest, MultipleBlobs) {
  int nBlobs = folly::Random::rand32(0, 100);
  std::vector<Blob> blobs;
  std::vector<std::string> strings;
  const uint32_t extra = 10;
  for (int i = 0; i < nBlobs; i++) {
    int len = folly::Random::rand32(100, 10240);
    strings.push_back(genRandomStr(len));
    blobs.push_back(Blob{
        len - extra, folly::StringPiece{strings[i].data(), strings[i].size()}});
  }

  size_t bufSize = NvmItem::estimateVariableSize(blobs);
  auto nvmItem =
      std::unique_ptr<NvmItem>(new (bufSize) NvmItem(1, 1, 1, blobs));

  ASSERT_EQ(nBlobs, nvmItem->getNumBlobs());
  for (int i = 0; i < nBlobs; i++) {
    ASSERT_EQ(blobs[i].data, nvmItem->getBlob(i).data);
    ASSERT_EQ(blobs[i].origAllocSize, nvmItem->getBlob(i).origAllocSize);
  }
  ASSERT_THROW(nvmItem->getBlob(folly::Random::rand32(nBlobs, 1024 * 1024)),
               std::invalid_argument);
}

TEST(NvmItemTest, TotalSize) {
  int nBlobs = folly::Random::rand32(0, 100);
  std::vector<Blob> blobs;
  std::vector<std::string> strings;
  for (int i = 0; i < nBlobs; i++) {
    int len = folly::Random::rand32(100, 10240);
    strings.push_back(genRandomStr(len));
    blobs.push_back(
        Blob{static_cast<uint32_t>(len),
             folly::StringPiece{strings[i].data(), strings[i].size()}});
  }

  size_t bufSize = NvmItem::estimateVariableSize(blobs);
  auto nvmItem =
      std::unique_ptr<NvmItem>(new (bufSize) NvmItem(1, 1, 1, blobs));

  ASSERT_EQ(bufSize + sizeof(NvmItem), nvmItem->totalSize());
}

TEST(NvmItemTest, MultipleBlobsOverFlow) {
  int nBlobs = folly::Random::rand32(0, 100);
  std::vector<Blob> blobs;
  std::vector<std::string> strings;
  const uint32_t extra = 10;
  for (int i = 0; i < nBlobs; i++) {
    int len = folly::Random::rand32(100, 10240);
    strings.push_back(genRandomStr(len));
    blobs.push_back(
        Blob{static_cast<uint32_t>(len - extra),
             folly::StringPiece{strings[i].data(), strings[i].size()}});
  }

  const size_t maxLen = std::numeric_limits<uint32_t>::max();
  auto buf = std::make_unique<char[]>(maxLen);
  blobs.push_back(Blob{static_cast<uint32_t>(maxLen),
                       folly::StringPiece{buf.get(), maxLen}});

  size_t bufSize = NvmItem::estimateVariableSize(blobs);
  ASSERT_THROW(std::unique_ptr<NvmItem>(new (bufSize) NvmItem(1, 1, 1, blobs)),
               std::out_of_range);
}

TEST(NvmItemTest, SingleBlobOverflow) {
  const size_t maxLen = std::numeric_limits<uint32_t>::max() + 10ULL;
  auto buf = std::make_unique<char[]>(maxLen);
  Blob blob{static_cast<uint32_t>(maxLen),
            folly::StringPiece{buf.get(), maxLen}};

  size_t bufSize = NvmItem::estimateVariableSize(blob);
  ASSERT_THROW(std::unique_ptr<NvmItem>(new (bufSize) NvmItem(1, 1, 1, blob)),
               std::out_of_range)
      << maxLen;
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
