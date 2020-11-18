#include "cachelib/navy/kangaroo/RripBucket.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"

using testing::_;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {

void updateEmpty(uint32_t idx) {
  return;
}

TEST(RripBucket, SingleKey) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");
  auto value = makeView("value");

  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());
  
  std::cout << "After find" << std::endl;

  bucket.insert(hk, value, 0, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value, bucket.find(hk, updateEmpty));
  
  std::cout << "Insert and another find" << std::endl;

  MockDestructor helper;
  EXPECT_CALL(helper, call(_, _, _)).Times(0);
  EXPECT_CALL(
      helper,
      call(makeView("key"), makeView("value"), DestructorEvent::Removed));
  auto cb = toCallback(helper);
  EXPECT_EQ(1, bucket.remove(hk, cb));
  std::cout << "After remove" << std::endl;
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());
  std::cout << "After find" << std::endl;

  EXPECT_EQ(0, bucket.remove(hk, nullptr));
  std::cout << "After empty remove" << std::endl;
}

TEST(RripBucket, SingleKeyRrip) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");
  auto value = makeView("value");

  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());

  bucket.insert(hk, value, 2, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value, bucket.find(hk, updateEmpty));

  MockDestructor helper;
  EXPECT_CALL(helper, call(_, _, _)).Times(0);
  EXPECT_CALL(
      helper,
      call(makeView("key"), makeView("value"), DestructorEvent::Removed));
  auto cb = toCallback(helper);
  EXPECT_EQ(1, bucket.remove(hk, cb));
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());

  EXPECT_EQ(0, bucket.remove(hk, nullptr));
}

TEST(RripBucket, CollisionKeys) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key 1");
  auto value1 = makeView("value 1");

  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());

  bucket.insert(hk, value1, 0, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk, updateEmpty));

  // Simulate a key that collides on the same hash
  const auto collidedHk =
      HashedKey::precomputed(makeView("key 2"), hk.keyHash());
  auto value2 = makeView("value 2");
  bucket.insert(collidedHk, value2, 0, nullptr);

  EXPECT_EQ(value1, bucket.find(hk, updateEmpty));
  EXPECT_EQ(value2, bucket.find(collidedHk, updateEmpty));
}

TEST(RripBucket, MultipleKeys) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  // Insert all the keys and verify we can find them
  bucket.insert(hk1, makeView("value 1"), 0, nullptr);
  EXPECT_EQ(1, bucket.size());

  bucket.insert(hk2, makeView("value 2"), 0, nullptr);
  EXPECT_EQ(2, bucket.size());

  bucket.insert(hk3, makeView("value 3"), 0, nullptr);
  EXPECT_EQ(3, bucket.size());

  EXPECT_EQ(makeView("value 1"), bucket.find(hk1, updateEmpty));
  EXPECT_EQ(makeView("value 2"), bucket.find(hk2, updateEmpty));
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3, updateEmpty));

  // Remove them one by one and verify removing one doesn't affect others
  EXPECT_EQ(1, bucket.remove(hk1, nullptr));
  EXPECT_EQ(2, bucket.size());
  EXPECT_TRUE(bucket.find(hk1, updateEmpty).isNull());
  EXPECT_EQ(makeView("value 2"), bucket.find(hk2, updateEmpty));
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3, updateEmpty));

  EXPECT_EQ(1, bucket.remove(hk2, nullptr));
  EXPECT_EQ(1, bucket.size());
  EXPECT_TRUE(bucket.find(hk1, updateEmpty).isNull());
  EXPECT_TRUE(bucket.find(hk2, updateEmpty).isNull());
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3, updateEmpty));

  EXPECT_EQ(1, bucket.remove(hk3, nullptr));
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk1, updateEmpty).isNull());
  EXPECT_TRUE(bucket.find(hk2, updateEmpty).isNull());
  EXPECT_TRUE(bucket.find(hk3, updateEmpty).isNull());
}

TEST(RripBucket, DuplicateKeys) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");
  auto value1 = makeView("value 1");
  auto value2 = makeView("value 2");
  auto value3 = makeView("value 3");

  // RripBucket does not replace an existing key.
  // New one will be shadowed by the old one, unless it
  // has evicted the old key by chance. Here, we won't
  // allocate enough to trigger evictions.
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());

  bucket.insert(hk, value1, 0, nullptr);
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk, updateEmpty));

  bucket.insert(hk, value2, 0, nullptr);
  EXPECT_EQ(2, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk, updateEmpty));

  bucket.insert(hk, value3, 0, nullptr);
  EXPECT_EQ(3, bucket.size());
  EXPECT_EQ(value1, bucket.find(hk, updateEmpty));

  // Now we'll start removing the keys. The order of
  // removing should follow the order of insertion.
  // A key inserted earlier will be removed before
  // the next one is removed.
  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(2, bucket.size());
  EXPECT_EQ(value2, bucket.find(hk, updateEmpty));

  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(value3, bucket.find(hk, updateEmpty));

  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(0, bucket.size());
  EXPECT_TRUE(bucket.find(hk, updateEmpty).isNull());
}

TEST(RripBucket, EvictionNone) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  // Insert 3 small key/value just enough not to trigger
  // any evictions.
  MockDestructor helper;
  EXPECT_CALL(helper, call(_, _, _)).Times(0);
  auto cb = toCallback(helper);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  ASSERT_EQ(0, bucket.insert(hk1, makeView("value 1"), 0, cb));
  EXPECT_EQ(1, bucket.size());
  EXPECT_EQ(makeView("value 1"), bucket.find(hk1, updateEmpty));

  ASSERT_EQ(0, bucket.insert(hk2, makeView("value 2"), 0, cb));
  EXPECT_EQ(2, bucket.size());
  EXPECT_EQ(makeView("value 2"), bucket.find(hk2, updateEmpty));

  ASSERT_EQ(0, bucket.insert(hk3, makeView("value 3"), 0, cb));
  EXPECT_EQ(3, bucket.size());
  EXPECT_EQ(makeView("value 3"), bucket.find(hk3, updateEmpty));

  EXPECT_EQ(3, bucket.size());
}

TEST(RripBucket, EvictionOne) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");
  const auto hk4 = makeHK("key 4");

  // Insert 3 small key/value.
  bucket.insert(hk1, makeView("value 1"), 0, nullptr);
  bucket.insert(hk2, makeView("value 2"), 0, nullptr);
  bucket.insert(hk3, makeView("value 3"), 0, nullptr);

  // Insert one more will evict the very first key
  MockDestructor helper;
  EXPECT_CALL(
      helper,
      call(makeView("key 1"), makeView("value 1"), DestructorEvent::Recycled));
  auto cb = toCallback(helper);
  ASSERT_EQ(1, bucket.insert(hk4, makeView("value 4"), 0, cb));

  EXPECT_EQ(makeView("value 4"), bucket.find(hk4, updateEmpty));
  EXPECT_EQ(3, bucket.size());
}

TEST(RripBucket, EvictionAll) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");
  const auto hkBig = makeHK("big key");

  // Insert 3 small key/value.
  bucket.insert(hk1, makeView("value 1"), 0, nullptr);
  bucket.insert(hk2, makeView("value 2"), 0, nullptr);
  bucket.insert(hk3, makeView("value 3"), 0, nullptr);
  EXPECT_EQ(3, bucket.size());

  // Inserting this big value will evict all previous keys
  MockDestructor helper;
  EXPECT_CALL(
      helper,
      call(makeView("key 1"), makeView("value 1"), DestructorEvent::Recycled));
  EXPECT_CALL(
      helper,
      call(makeView("key 2"), makeView("value 2"), DestructorEvent::Recycled));
  EXPECT_CALL(
      helper,
      call(makeView("key 3"), makeView("value 3"), DestructorEvent::Recycled));
  auto cb = toCallback(helper);

  Buffer bigValue(50);
  ASSERT_EQ(3, bucket.insert(hkBig, bigValue.view(), 0, cb));
  EXPECT_EQ(bigValue.view(), bucket.find(hkBig, updateEmpty));
  EXPECT_EQ(1, bucket.size());
}

TEST(RripBucket, Checksum) {
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk = makeHK("key");

  // Setting a checksum does not affect checksum's outcome
  const uint32_t checksum1 = RripBucket::computeChecksum(buf.view());
  bucket.setChecksum(checksum1);
  EXPECT_EQ(checksum1, bucket.getChecksum());
  EXPECT_EQ(checksum1, RripBucket::computeChecksum(buf.view()));

  // Adding a new key/value will not update the existing checksum member,
  // but it will change the checksum computed.
  bucket.insert(hk, makeView("value"), 0, nullptr);
  EXPECT_EQ(checksum1, bucket.getChecksum());
  const uint64_t checksum2 = RripBucket::computeChecksum(buf.view());
  EXPECT_NE(checksum1, checksum2);
  bucket.setChecksum(checksum2);
  EXPECT_EQ(checksum2, bucket.getChecksum());

  // Removing a key/value will change the checksum
  EXPECT_EQ(1, bucket.remove(hk, nullptr));
  EXPECT_EQ(checksum2, bucket.getChecksum());
  const uint64_t checksum3 = RripBucket::computeChecksum(buf.view());
  EXPECT_NE(checksum2, checksum3);
  bucket.setChecksum(checksum3);
  EXPECT_EQ(checksum3, bucket.getChecksum());
}

TEST(RripBucket, Iteration) {
  EXPECT_EQ(1,1);
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  // Insert 3 small key/value.
  bucket.insert(hk1, makeView("value 1"), 0, nullptr);
  bucket.insert(hk2, makeView("value 2"), 0, nullptr);
  bucket.insert(hk3, makeView("value 3"), 0, nullptr);
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

TEST(RripBucket, Reorder) {
  EXPECT_EQ(1,1);
  Buffer buf(96 + sizeof(RripBucket));
  auto& bucket = RripBucket::initNew(buf.mutableView(), 0);

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  // Insert 3 small key/value with different # of hits
  bucket.insert(hk1, makeView("value 1"), 0, nullptr);
  bucket.insert(hk2, makeView("value 2"), 5, nullptr);
  bucket.insert(hk3, makeView("value 3"), 0, nullptr);
  EXPECT_EQ(3, bucket.size());

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
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk2));
    EXPECT_EQ(makeView("value 2"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_TRUE(itr.done());
  }

  BitVectorReadVisitor visitor = [](uint32_t test){ return !(test % 2); };
  bucket.reorder(visitor);
  
  {
    auto itr = bucket.getFirst();
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk3));
    EXPECT_EQ(makeView("value 3"), itr.value());

    itr = bucket.getNext(itr);
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk1));
    EXPECT_EQ(makeView("value 1"), itr.value());
    
    itr = bucket.getNext(itr);
    EXPECT_FALSE(itr.done());
    EXPECT_TRUE(itr.keyEqualsTo(hk2));
    EXPECT_EQ(makeView("value 2"), itr.value());


    itr = bucket.getNext(itr);
    EXPECT_TRUE(itr.done());
  }

}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
