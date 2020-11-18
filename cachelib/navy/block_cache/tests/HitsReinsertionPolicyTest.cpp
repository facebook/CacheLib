#include <thread>

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/serialization/RecordIO.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(HitsReinsertionPolicy, Simple) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);

  auto hk1 = makeHK("test_key_1");

  // lookup before inserting has no effect
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  // lookup after inserting has effect
  index.insert(hk1.keyHash(), 0, 0);
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  index.lookup(hk1.keyHash());
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(1, access.totalHits);
    EXPECT_EQ(1, access.currHits);
  }

  EXPECT_TRUE(tracker.shouldReinsert(hk1));
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(1, access.totalHits);
    EXPECT_EQ(1, access.currHits);
  }

  // lookup again
  index.lookup(hk1.keyHash());
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(2, access.totalHits);
    EXPECT_EQ(2, access.currHits);
  }

  index.remove(hk1.keyHash());
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  // removing a second time is fine. Just no-op
  index.remove(hk1.keyHash());
}

TEST(HitsReinsertionPolicy, UpperBound) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);
  auto hk1 = makeHK("test_key_1");

  index.insert(hk1.keyHash(), 0, 0);
  for (int i = 0; i < 1000; i++) {
    index.lookup(hk1.keyHash());
  }
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }
}

TEST(HitsReinsertionPolicy, ThreadSafe) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);
  auto hk1 = makeHK("test_key_1");

  index.insert(hk1.keyHash(), 0, 0);

  auto lookup = [&]() { index.lookup(hk1.keyHash()); };

  std::vector<std::thread> threads;
  for (int i = 0; i < 159; i++) {
    threads.emplace_back(std::thread(lookup));
  }

  for (auto& t : threads) {
    t.join();
  }

  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(159, access.totalHits);
    EXPECT_EQ(159, access.currHits);
  }
}

TEST(HitsReinsertionPolicy, Recovery) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);
  auto hk1 = makeHK("test_key_1");

  index.insert(hk1.keyHash(), 0, 0);
  for (int i = 0; i < 1000; i++) {
    index.lookup(hk1.keyHash());
  }
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }

  // persist to memory then recover from it
  folly::IOBufQueue buf;
  auto rw = createMemoryRecordWriter(buf);
  index.persist(*rw);
  index.reset();

  auto rr = createMemoryRecordReader(buf);
  index.recover(*rr);

  // access stats should be the same
  {
    auto access = tracker.getAccessStats(hk1);
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
