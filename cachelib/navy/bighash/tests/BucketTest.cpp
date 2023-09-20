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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/bighash/Bucket.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"

using testing::_;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Bucket, SingleKey) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");
  auto value = makeView("value");

  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk).isNull());

  bucket.insert(hk, value, nullptr, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value, bucket.find(hk));

  MockDestructor helper;
  EXPECT_CALL(helper, call(_, _, _)).Times(0);
  EXPECT_CALL(helper,
              call(makeHK("key"), makeView("value"), DestructorEvent::Removed));
  auto cb = toCallback(helper);
  EXPECT_EQ(1, bucket.remove(hk, cb));
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk).isNull());

  EXPECT_EQ(0, bucket.remove(hk, nullptr));
}

TEST(Bucket, CollisionKeys) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key 1");
  auto value1 = makeView("value 1");

  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk).isNull());

  bucket.insert(hk, value1, nullptr, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk));

  // Simulate a key that collides on the same hash
  const auto collidedHk = HashedKey::precomputed("key 2", hk.keyHash());
  auto value2 = makeView("value 2");
  bucket.insert(collidedHk, value2, nullptr, nullptr);

  EXPECT_EQ(value1, bucket.find(hk));
  EXPECT_EQ(value2, bucket.find(collidedHk));
}

TEST(Bucket, MultipleKeys) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  // Insert all the keys and verify we can find them
  bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
  EXPECT_EQ(1, bucket.size());

  bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
  EXPECT_EQ(2, bucket.size());

  bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);
  EXPECT_EQ(3, bucket.size());

  EXPECT_EQ(makeView("value 1"), bucket.find(hk1));
  EXPECT_EQ(makeView("value 2"), bucket.find(hk2));
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

  // Remove them one by one and verify removing one doesn't affect others
  EXPECT_EQ(1, bucket.remove(hk1, nullptr));
  EXPECT_EQ(2, bucket.size());
  EXPECT_TRUE(bucket.find(hk1).isNull());
  EXPECT_EQ(makeView("value 2"), bucket.find(hk2));
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

  EXPECT_EQ(1, bucket.remove(hk2, nullptr));
  EXPECT_EQ(1, bucket.size());
  EXPECT_TRUE(bucket.find(hk1).isNull());
  EXPECT_TRUE(bucket.find(hk2).isNull());
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

  EXPECT_EQ(1, bucket.remove(hk3, nullptr));
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk1).isNull());
  EXPECT_TRUE(bucket.find(hk2).isNull());
  EXPECT_TRUE(bucket.find(hk3).isNull());
}

TEST(Bucket, DuplicateKeys) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");
  auto value1 = makeView("value 1");
  auto value2 = makeView("value 2");
  auto value3 = makeView("value 3");

  // Bucket does not replace an existing key.
  // New one will be shadowed by the old one, unless it
  // has evicted the old key by chance. Here, we won't
  // allocate enough to trigger evictions.
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk).isNull());

  bucket.insert(hk, value1, nullptr, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk));

  bucket.insert(hk, value2, nullptr, nullptr);
  EXPECT_EQ(2, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk));

  bucket.insert(hk, value3, nullptr, nullptr);
  EXPECT_EQ(3, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk));

  // Now we'll start removing the keys. The order of
  // removing should follow the order of insertion.
  // A key inserted earlier will be removed before
  // the next one is removed.
  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(2, bucket.size());
  EXPECT_EQ(value2, bucket.find(hk));

  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value3, bucket.find(hk));

  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk).isNull());
}

TEST(Bucket, EvictionNone) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  // Insert 3 small key/value just enough not to trigger
  // any evictions.
  MockDestructor helper;
  EXPECT_CALL(helper, call(_, _, _)).Times(0);
  auto cb = toCallback(helper);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  ASSERT_EQ(0, bucket.insert(hk1, makeView("value 1"), nullptr, cb).first);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(makeView("value 1"), bucket.find(hk1));

  ASSERT_EQ(0, bucket.insert(hk2, makeView("value 2"), nullptr, cb).first);
  EXPECT_EQ(2, bucket.size());
  EXPECT_EQ(makeView("value 2"), bucket.find(hk2));

  ASSERT_EQ(0, bucket.insert(hk3, makeView("value 3"), nullptr, cb).first);
  EXPECT_EQ(3, bucket.size());
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

  EXPECT_EQ(3, bucket.size());
}

TEST(Bucket, EvictionOne) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");
  const auto hk4 = makeHK("key 4");

  // Insert 3 small key/value.
  bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
  bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
  bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);

  // Insert one more will evict the very first key
  MockDestructor helper;
  EXPECT_CALL(
      helper,
      call(makeHK("key 1"), makeView("value 1"), DestructorEvent::Recycled));
  auto cb = toCallback(helper);
  ASSERT_EQ(1, bucket.insert(hk4, makeView("value 4"), nullptr, cb).first);

  EXPECT_EQ(makeView("value 4"), bucket.find(hk4));
  EXPECT_EQ(3, bucket.size());
}

TEST(Bucket, EvictionAll) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");
  const auto hkBig = makeHK("big key");

  // Insert 3 small key/value.
  bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
  bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
  bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);
  EXPECT_EQ(3, bucket.size());

  // Inserting this big value will evict all previous keys
  MockDestructor helper;
  EXPECT_CALL(
      helper,
      call(makeHK("key 1"), makeView("value 1"), DestructorEvent::Recycled));
  EXPECT_CALL(
      helper,
      call(makeHK("key 2"), makeView("value 2"), DestructorEvent::Recycled));
  EXPECT_CALL(
      helper,
      call(makeHK("key 3"), makeView("value 3"), DestructorEvent::Recycled));
  auto cb = toCallback(helper);

  Buffer bigValue(50);
  ASSERT_EQ(3, bucket.insert(hkBig, bigValue.view(), nullptr, cb).first);
  EXPECT_EQ(bigValue.view(), bucket.find(hkBig));
  EXPECT_EQ(1, bucket.size());
}

TEST(Bucket, Checksum) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");

  // Setting a checksum does not affect checksum's outcome
  const uint32_t checksum1 = Bucket::computeChecksum(buf.view());
  bucket.setChecksum(checksum1);
  EXPECT_EQ(checksum1, bucket.getChecksum());
  EXPECT_EQ(checksum1, Bucket::computeChecksum(buf.view()));

  // Adding a new key/value will not update the existing checksum member,
  // but it will change the checksum computed.
  bucket.insert(hk, makeView("value"), nullptr, nullptr);
  EXPECT_EQ(checksum1, bucket.getChecksum());
  const uint64_t checksum2 = Bucket::computeChecksum(buf.view());
  EXPECT_NE(checksum1, checksum2);
  bucket.setChecksum(checksum2);
  EXPECT_EQ(checksum2, bucket.getChecksum());

  // Removing a key/value will change the checksum
  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(checksum2, bucket.getChecksum());
  const uint64_t checksum3 = Bucket::computeChecksum(buf.view());
  EXPECT_NE(checksum2, checksum3);
  bucket.setChecksum(checksum3);
  EXPECT_EQ(checksum3, bucket.getChecksum());
}

TEST(Bucket, Iteration) {
  Buffer buf(96 + sizeof(Bucket));
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  // Insert 3 small key/value.
  bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
  bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
  bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);
  EXPECT_EQ(3, bucket.size());

  {
    auto itr = bucket.getFirst();
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk1));
    EXPECT_EQ(makeView("value 1"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk2));
    EXPECT_EQ(makeView("value 2"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk3));
    EXPECT_EQ(makeView("value 3"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_TRUE(itr.done());
  }

  // Remove and retry
  EXPECT_EQ(1, bucket.remove(hk2, nullptr));
  {
    auto itr = bucket.getFirst();
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk1));
    EXPECT_EQ(makeView("value 1"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk3));
    EXPECT_EQ(makeView("value 3"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_TRUE(itr.done());
  }
}

TEST(Bucket, EvictionExpired) {
  constexpr uint32_t bucketSize = 1024;
  const uint32_t itemMinSize =
      details::BucketEntry::computeSize(sizeof("key "), sizeof("value "));
  const uint32_t totalItems = bucketSize / itemMinSize + 1;
  Buffer buf(bucketSize);
  auto& bucket = Bucket::initNew(buf.mutableView(), 0);
  uint32_t numItem = 0;

  char keyStr[64];
  char valueStr[64];

  std::vector<bool> validItems(totalItems, false);
  ExpiredCheck expCb = [&validItems, &numItem](navy::BufferView v) -> bool {
    // expire by 50% of probability
    if (folly::Random::rand32(100) > 50) {
      return false;
    }
    uint32_t idx;
    std::sscanf(reinterpret_cast<const char*>(v.data()), "value %d", &idx);
    EXPECT_LT(idx, numItem);
    validItems[idx] = false;
    return true;
  };

  // insert items until the bucket is full and some items are evicted by
  // expiration
  std::pair<uint32_t, uint32_t> evicted;
  do {
    sprintf(keyStr, "key %d", numItem);
    sprintf(valueStr, "value %d", numItem);

    const auto hk = makeHK(keyStr);
    evicted = bucket.insert(hk, makeView(valueStr), expCb, nullptr);
    // all evicted items should be by expiration if any
    EXPECT_TRUE(!evicted.first || (evicted.first == evicted.second));
    validItems[numItem] = true;
    numItem++;
  } while (!evicted.first && numItem < totalItems);

  EXPECT_EQ(numItem, bucket.size() + evicted.first);

  for (uint32_t i = 0; i < numItem; i++) {
    sprintf(keyStr, "key %d", i);
    const auto hk = makeHK(keyStr);
    auto value = bucket.find(hk);

    if (!validItems[i]) {
      EXPECT_TRUE(value.isNull());
    } else {
      sprintf(valueStr, "value %d", i);
      EXPECT_EQ(makeView(valueStr), bucket.find(hk));
    }
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
