#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/navy/serialization/Serialization.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Serialization, Serialize) {
  serialization::IndexBucket bucket;
  bucket.bucketId = 0;
  bucket.entries.resize(5);
  uint8_t i = 0;
  for (auto& entry : *bucket.entries_ref()) {
    entry.key_ref() = i;
    entry.address_ref() = i * 10;
    entry.sizeHint_ref() = i * 100;
    entry.totalHits_ref() = i + 1;
    entry.currentHits_ref() = i + 2;
    ++i;
  }

  serialization::Region region;
  region.regionId = 1;
  region.classId = 2;
  region.lastEntryEndOffset = 3;

  folly::IOBufQueue ioq;
  auto rw = createMemoryRecordWriter(ioq);

  serializeProto(bucket, *rw);
  serializeProto(region, *rw);

  auto rr = createMemoryRecordReader(ioq);
  auto deserializedBucket = deserializeProto<serialization::IndexBucket>(*rr);
  EXPECT_EQ(deserializedBucket.bucketId, bucket.bucketId);
  EXPECT_EQ(deserializedBucket.entries.size(), 5);

  i = 0;
  for (auto& entry : *deserializedBucket.entries_ref()) {
    EXPECT_EQ(i, *entry.key_ref());
    EXPECT_EQ(i * 10, *entry.address_ref());
    EXPECT_EQ(i * 100, *entry.sizeHint_ref());
    EXPECT_EQ(i + 1, *entry.totalHits_ref());
    EXPECT_EQ(i + 2, *entry.currentHits_ref());
    EXPECT_FALSE(rr->isEnd());
    ++i;
  }

  auto deserializedRegion = deserializeProto<serialization::Region>(*rr);

  EXPECT_EQ(1, deserializedRegion.regionId);
  EXPECT_EQ(2, deserializedRegion.classId);
  EXPECT_EQ(3, deserializedRegion.lastEntryEndOffset);
  EXPECT_TRUE(rr->isEnd());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
