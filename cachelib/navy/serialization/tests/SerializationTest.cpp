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

#include <gtest/gtest.h>

#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Serialization, Serialize) {
  serialization::IndexBucket bucket;
  *bucket.bucketId_ref() = 0;
  bucket.entries_ref()->resize(5);
  uint8_t i = 0;
  for (auto& entry : *bucket.entries_ref()) {
    entry.key_ref() = i;
    entry.address_ref() = i * 10;
    entry.sizeHint_ref() = static_cast<short>(i * 100);
    entry.totalHits_ref() = static_cast<signed char>(i + 1);
    entry.currentHits_ref() = static_cast<signed char>(i + 2);
    ++i;
  }

  serialization::Region region;
  *region.regionId_ref() = 1;
  *region.classId_ref() = 2;
  *region.lastEntryEndOffset_ref() = 3;

  folly::IOBufQueue ioq;
  auto rw = createMemoryRecordWriter(ioq);

  serializeProto(bucket, *rw);
  serializeProto(region, *rw);

  auto rr = createMemoryRecordReader(ioq);
  auto deserializedBucket = deserializeProto<serialization::IndexBucket>(*rr);
  EXPECT_EQ(*deserializedBucket.bucketId_ref(), *bucket.bucketId_ref());
  EXPECT_EQ(deserializedBucket.entries_ref()->size(), 5);

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

  EXPECT_EQ(1, *deserializedRegion.regionId_ref());
  EXPECT_EQ(2, *deserializedRegion.classId_ref());
  EXPECT_EQ(3, *deserializedRegion.lastEntryEndOffset_ref());
  EXPECT_TRUE(rr->isEnd());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
