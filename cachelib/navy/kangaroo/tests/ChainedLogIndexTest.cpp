#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/kangaroo/ChainedLogIndex.h"
#include "cachelib/navy/testing/BufferGen.h"

using testing::_;

namespace facebook {
namespace cachelib {
namespace navy {

TEST(ChainedLogIndex, BasicOps) {
  auto testFn = [](uint64_t hash) { return KangarooBucketId(hash % 2); };
  ChainedLogIndex li{6, 3, testFn};
  uint32_t hits = 0;

  const auto hk1 = makeHK("key1");
  li.insert(hk1, PartitionOffset(1, true));
  auto ret = li.lookup(hk1, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(1, true));
  EXPECT_EQ(hits, 1);

  const auto hk2 = makeHK("key2");
  li.insert(hk2, PartitionOffset(2, true));
  ret = li.lookup(hk2, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(2, true));
  EXPECT_EQ(hits, 1);

  auto status = li.remove(hk1, PartitionOffset(1, true));
  EXPECT_EQ(status, Status::Ok);
  ret = li.lookup(hk2, false, &hits);
  EXPECT_EQ(ret, PartitionOffset(2, true));
  EXPECT_EQ(hits, 1);
  ret = li.lookup(hk1, false, &hits);
  EXPECT_EQ(ret, PartitionOffset(0, false));
}

TEST(ChainedLogIndex, Chaining) {
  auto testFn = [](uint64_t hash) { return KangarooBucketId(hash % 2); };
  ChainedLogIndex li{1, 3, testFn};
  uint32_t hits = 0;

  const auto hk0 = makeHK("key0");
  li.insert(hk0, PartitionOffset(0, true));
  auto ret = li.lookup(hk0, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(0, true));
  EXPECT_EQ(hits, 1);

  const auto hk1 = makeHK("key1");
  li.insert(hk1, PartitionOffset(1, true));
  ret = li.lookup(hk1, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(1, true));
  EXPECT_EQ(hits, 1);

  const auto hk2 = makeHK("key2");
  li.insert(hk2, PartitionOffset(2, true));
  ret = li.lookup(hk2, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(2, true));
  EXPECT_EQ(hits, 1);

  const auto hk3 = makeHK("key3");
  li.insert(hk3, PartitionOffset(3, true));
  ret = li.lookup(hk3, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(3, true));
  EXPECT_EQ(hits, 1);
  ret = li.lookup(hk3, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(3, true));
  EXPECT_EQ(hits, 2);

  auto status = li.remove(hk1, PartitionOffset(1, true));
  EXPECT_EQ(status, Status::Ok);
  ret = li.lookup(hk0, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(0, true));
  EXPECT_EQ(hits, 2);
  ret = li.lookup(hk1, false, &hits);
  EXPECT_EQ(ret, PartitionOffset(0, false));
  ret = li.lookup(hk3, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(3, true));
  EXPECT_EQ(hits, 3);

  EXPECT_EQ(li.find(testFn(hk2.keyHash()), createTag(hk2)),
            PartitionOffset(2, true));
  EXPECT_EQ(li.remove(createTag(hk2), testFn(hk2.keyHash()),
                      PartitionOffset(2, true)),
            Status::Ok);
  ret = li.lookup(hk0, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(0, true));
  EXPECT_EQ(hits, 3);
  ret = li.lookup(hk2, false, &hits);
  EXPECT_EQ(ret, PartitionOffset(0, false));
  ret = li.lookup(hk3, true, &hits);
  EXPECT_EQ(ret, PartitionOffset(3, true));
  EXPECT_EQ(hits, 4);
}

TEST(ChainedLogIndex, MergeOps) {
  auto testFn = [](uint64_t hash) { return KangarooBucketId(hash % 2 * 5); };
  ChainedLogIndex li{7, 3, testFn};

  const auto hk1 = makeHK("key1");
  li.insert(hk1, PartitionOffset(1, true));
  li.insert(makeHK("key2"), PartitionOffset(2, true));
  li.insert(makeHK("key3"), PartitionOffset(3, true));
  li.insert(makeHK("key4"), PartitionOffset(4, true));
  li.insert(makeHK("key5"), PartitionOffset(5, true));
  li.insert(makeHK("key6"), PartitionOffset(6, true));

  uint64_t count = li.countBucket(hk1);
  EXPECT_EQ(count, 3);

  auto itr = li.getHashBucketIterator(hk1);
  EXPECT_EQ(itr.offset(), PartitionOffset(1, true));
  EXPECT_EQ(itr.hits(), 0);
  EXPECT_EQ(itr.tag(), createTag(HashedKey(hk1.key())));
  uint64_t i = 0;
  while (!itr.done()) {
    i++;
    itr = li.getNext(itr);
  }
  EXPECT_EQ(i, count);

  auto offset = li.find(KangarooBucketId(0), createTag(HashedKey(hk1.key())));
  EXPECT_EQ(offset, PartitionOffset(1, true));
  li.remove(createTag(HashedKey(hk1.key())), KangarooBucketId(0),
            PartitionOffset(1, true));
  offset = li.find(KangarooBucketId(1), createTag(HashedKey(hk1.key())));
  EXPECT_EQ(offset, PartitionOffset(0, false));
}

TEST(ChainedLogIndex, Intermixed) {
  auto testFn = [](uint64_t hash) { return KangarooBucketId(hash % 3); };
  ChainedLogIndex li{3, 2, testFn};
  uint32_t hits = 0;

  li.insert(makeHK("key0"), PartitionOffset(0, true));
  li.insert(makeHK("key1"), PartitionOffset(1, true));
  li.insert(makeHK("key2"), PartitionOffset(2, true));
  li.insert(makeHK("key3"), PartitionOffset(3, true));
  li.insert(makeHK("key4"), PartitionOffset(4, true));
  li.insert(makeHK("key5"), PartitionOffset(5, true));
  li.insert(makeHK("key6"), PartitionOffset(6, true));

  EXPECT_EQ(li.lookup(makeHK("key0"), false, nullptr),
            PartitionOffset(0, true));
  EXPECT_EQ(li.lookup(makeHK("key1"), false, nullptr),
            PartitionOffset(1, true));
  EXPECT_EQ(li.lookup(makeHK("key2"), false, nullptr),
            PartitionOffset(2, true));
  EXPECT_EQ(li.lookup(makeHK("key3"), false, nullptr),
            PartitionOffset(3, true));
  EXPECT_EQ(li.lookup(makeHK("key4"), false, nullptr),
            PartitionOffset(4, true));
  EXPECT_EQ(li.lookup(makeHK("key5"), false, nullptr),
            PartitionOffset(5, true));
  EXPECT_EQ(li.lookup(makeHK("key6"), false, nullptr),
            PartitionOffset(6, true));

  EXPECT_EQ(li.remove(makeHK("key0"), PartitionOffset(0, true)), Status::Ok);
  EXPECT_EQ(li.remove(makeHK("key3"), PartitionOffset(3, true)), Status::Ok);
  EXPECT_EQ(li.remove(makeHK("key6"), PartitionOffset(6, true)), Status::Ok);
  EXPECT_EQ(li.remove(makeHK("key0"), PartitionOffset(0, true)),
            Status::NotFound);

  EXPECT_EQ(li.lookup(makeHK("key0"), false, nullptr),
            PartitionOffset(0, false));
  EXPECT_EQ(li.lookup(makeHK("key1"), false, nullptr),
            PartitionOffset(1, true));
  EXPECT_EQ(li.lookup(makeHK("key2"), false, nullptr),
            PartitionOffset(2, true));
  EXPECT_EQ(li.lookup(makeHK("key3"), false, nullptr),
            PartitionOffset(3, false));
  EXPECT_EQ(li.lookup(makeHK("key4"), false, nullptr),
            PartitionOffset(4, true));
  EXPECT_EQ(li.lookup(makeHK("key5"), false, &hits), PartitionOffset(5, true));
  EXPECT_EQ(li.lookup(makeHK("key6"), false, nullptr),
            PartitionOffset(6, false));
  EXPECT_EQ(hits, 0);

  li.insert(makeHK("key7"), PartitionOffset(7, true));
  li.insert(makeHK("key8"), PartitionOffset(8, true));
  EXPECT_EQ(li.remove(makeHK("key5"), PartitionOffset(5, true)), Status::Ok);

  EXPECT_EQ(li.lookup(makeHK("key0"), false, nullptr),
            PartitionOffset(0, false));
  EXPECT_EQ(li.lookup(makeHK("key1"), false, nullptr),
            PartitionOffset(1, true));
  EXPECT_EQ(li.lookup(makeHK("key2"), false, nullptr),
            PartitionOffset(2, true));
  EXPECT_EQ(li.lookup(makeHK("key3"), false, nullptr),
            PartitionOffset(3, false));
  EXPECT_EQ(li.lookup(makeHK("key4"), false, nullptr),
            PartitionOffset(4, true));
  EXPECT_EQ(li.lookup(makeHK("key5"), false, nullptr),
            PartitionOffset(5, false));
  EXPECT_EQ(li.lookup(makeHK("key6"), false, nullptr),
            PartitionOffset(6, false));
  EXPECT_EQ(li.lookup(makeHK("key7"), false, nullptr),
            PartitionOffset(7, true));
  EXPECT_EQ(li.lookup(makeHK("key8"), false, nullptr),
            PartitionOffset(8, true));

  uint64_t count1 = li.countBucket(makeHK("key1"));
  uint64_t count2 = li.countBucket(makeHK("key2"));
  uint64_t count4 = li.countBucket(makeHK("key4"));
  uint64_t count7 = li.countBucket(makeHK("key7"));
  uint64_t count8 = li.countBucket(makeHK("key8"));

  auto itr = li.getHashBucketIterator(makeHK("key2"));
  EXPECT_EQ(itr.offset(), PartitionOffset(2, true));
  itr = li.getNext(itr);
  EXPECT_EQ(itr.offset(), PartitionOffset(4, true));
  itr = li.getNext(itr);
  EXPECT_EQ(itr.offset(), PartitionOffset(7, true));

  EXPECT_EQ(count1 + count2 + count4 + count7 + count8, 11);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
