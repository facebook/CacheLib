#include <gtest/gtest.h>

#include <folly/Random.h>

#include "cachelib/allocator/nvmcache/DipperItem.h"
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

TEST(DipperItemTest, SingleBlob) {
  folly::StringPiece data{"helloworld"};
  uint32_t origSize = 5;
  Blob blob{origSize, data};
  size_t bufSize = DipperItem::estimateVariableSize(blob);
  auto dItem =
      std::unique_ptr<DipperItem>(new (bufSize) DipperItem(1, 1, 1, blob));
  ASSERT_EQ(1, dItem->getNumBlobs());
  ASSERT_EQ(data, dItem->getBlob(0).data);
  ASSERT_EQ(origSize, dItem->getBlob(0).origAllocSize);
  ASSERT_THROW(dItem->getBlob(folly::Random::rand32() + 1),
               std::invalid_argument);
}

TEST(DipperItemTest, MultipleBlobs) {
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

  size_t bufSize = DipperItem::estimateVariableSize(blobs);
  auto dItem =
      std::unique_ptr<DipperItem>(new (bufSize) DipperItem(1, 1, 1, blobs));

  ASSERT_EQ(nBlobs, dItem->getNumBlobs());
  for (int i = 0; i < nBlobs; i++) {
    ASSERT_EQ(blobs[i].data, dItem->getBlob(i).data);
    ASSERT_EQ(blobs[i].origAllocSize, dItem->getBlob(i).origAllocSize);
  }
  ASSERT_THROW(dItem->getBlob(folly::Random::rand32(nBlobs, 1024 * 1024)),
               std::invalid_argument);
}

TEST(DipperItemTest, TotalSize) {
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

  size_t bufSize = DipperItem::estimateVariableSize(blobs);
  auto dItem =
      std::unique_ptr<DipperItem>(new (bufSize) DipperItem(1, 1, 1, blobs));

  ASSERT_EQ(bufSize + sizeof(DipperItem), dItem->totalSize());
}

TEST(DipperItemTest, MultipleBlobsOverFlow) {
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

  size_t bufSize = DipperItem::estimateVariableSize(blobs);
  ASSERT_THROW(
      std::unique_ptr<DipperItem>(new (bufSize) DipperItem(1, 1, 1, blobs)),
      std::out_of_range);
}

TEST(DipperItemTest, SingleBlobOverflow) {
  const size_t maxLen = std::numeric_limits<uint32_t>::max() + 10ULL;
  auto buf = std::make_unique<char[]>(maxLen);
  Blob blob{static_cast<uint32_t>(maxLen),
            folly::StringPiece{buf.get(), maxLen}};

  size_t bufSize = DipperItem::estimateVariableSize(blob);
  ASSERT_THROW(
      std::unique_ptr<DipperItem>(new (bufSize) DipperItem(1, 1, 1, blob)),
      std::out_of_range)
      << maxLen;
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
