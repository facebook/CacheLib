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
  for (size_t j = 0; j < bucket.entries.size(); j++) {
    bucket.entries[j].key = j;
    bucket.entries[j].value = j * 10;
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
  for (size_t i = 0; i < 5; i++) {
    EXPECT_EQ(i, deserializedBucket.entries[i].key);
    EXPECT_EQ(i * 10, deserializedBucket.entries[i].value);
    EXPECT_FALSE(rr->isEnd());
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
