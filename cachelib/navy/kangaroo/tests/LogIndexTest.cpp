#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/kangaroo/LogIndex.h"
#include "cachelib/navy/common/Types.h"

#include "cachelib/navy/testing/BufferGen.h"

using testing::_;

namespace facebook {
namespace cachelib {
namespace navy {

TEST(LogIndex, BasicOps) {
  auto testFn = [](uint64_t hash) {return KangarooBucketId(hash % 2); };
  LogIndex li{11, testFn};

  const auto hk1 = makeHK("key1");
  li.insert(hk1, LogPageId(1, true));
  auto ret = li.lookup(hk1, true);
  EXPECT_EQ(ret, LogPageId(1, true));

  const auto hk2 = makeHK("key2");
  li.insert(hk2, LogPageId(2, true));
  ret = li.lookup(hk2, true);
  EXPECT_EQ(ret, LogPageId(2, true));

  auto status = li.remove(hk1);
  EXPECT_EQ(status, Status::Ok);
  ret = li.lookup(hk2, false);
  EXPECT_EQ(ret, LogPageId(2, true));
  ret = li.lookup(hk1, false);
  EXPECT_EQ(ret, LogPageId(0, false));
}

TEST(LogIndex, MergeOps) {
  auto testFn = [](uint64_t hash) {
    std::cout << "Hash: " << hash % 2 * 5 << std::endl;
    return KangarooBucketId(hash % 2 * 5); 
  };
  LogIndex li{11, testFn};

  const auto hk1 = makeHK("key1");
  li.insert(hk1, LogPageId(1, true));
  li.insert(makeHK("key2"), LogPageId(2, true));
  li.insert(makeHK("key3"), LogPageId(3, true));
  li.insert(makeHK("key4"), LogPageId(4, true));
  li.insert(makeHK("key5"), LogPageId(5, true));
  li.insert(makeHK("key6"), LogPageId(6, true));

  uint64_t count = li.countBucket(hk1);
  EXPECT_EQ(count, 3);

  auto itr = li.getHashBucketIterator(hk1);
  EXPECT_EQ(itr.page(), LogPageId(1, true));
  EXPECT_EQ(itr.hits(), 0);
  EXPECT_EQ(itr.tag(), createTag(HashedKey(hk1.key())));
  uint64_t i = 0;
  while (!itr.done()) {
    i++;
    itr = li.getNext(itr);
  }
  EXPECT_EQ(i, count);

  auto page = li.findAndRemove(KangarooBucketId(1), createTag(HashedKey(hk1.key())));
  EXPECT_EQ(page, LogPageId(1, true));
  page = li.findAndRemove(KangarooBucketId(1), createTag(HashedKey(hk1.key())));
  EXPECT_EQ(page, LogPageId(0, false));
}


} // namespace navy
} // namespace cachelib
} // namespace facebook
