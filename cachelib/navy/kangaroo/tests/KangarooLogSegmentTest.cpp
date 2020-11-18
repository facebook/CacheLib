#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/kangaroo/KangarooLogSegment.h"

#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/Callbacks.h"

using testing::_;

namespace facebook {
namespace cachelib {
namespace navy {

TEST(KangarooLogSegment, BasicOps) {
  uint64_t bucketSize = 32 + sizeof(LogBucket);
  Buffer buf(2 * bucketSize);
  auto seg = KangarooLogSegment(bucketSize * 2, bucketSize, 
    LogSegmentId(1, 1), 8, buf.mutableView(), true); 
  
  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");
  
  LogPageId lpid = seg.insert(hk1, makeView("value 1"));
  EXPECT_EQ(lpid.index(), 10);
  EXPECT_EQ(lpid.isValid(), true);
  lpid = seg.insert(hk2, makeView("value 2"));
  EXPECT_EQ(lpid, LogPageId(11, true));
  
  lpid = seg.insert(hk3, makeView("value 3"));
  EXPECT_EQ(lpid, LogPageId(14, false));

  EXPECT_EQ(makeView("value 1"), seg.find(hk1, LogPageId(2, true)));
  EXPECT_EQ(makeView(""), seg.find(hk2, LogPageId(2, true)));
  EXPECT_EQ(makeView("value 2"), seg.find(hk2, LogPageId(3, true)));

  EXPECT_EQ(LogSegmentId(1, 1), seg.getLogSegmentId());

  auto it = seg.getFirst();
  EXPECT_EQ(it.key(), hk1);
  it = seg.getNext(it);
  EXPECT_EQ(it.key(), hk2);
  it = seg.getNext(it);
  EXPECT_TRUE(it.done());

  seg.clear(LogSegmentId(2, 0));
  EXPECT_EQ(LogSegmentId(2, 0), seg.getLogSegmentId());
  EXPECT_EQ(makeView(""), seg.find(hk1, LogPageId(2, true)));
  EXPECT_EQ(makeView(""), seg.find(hk2, LogPageId(3, true)));
  
  lpid = seg.insert(hk3, makeView("value 3"));
  EXPECT_EQ(lpid, LogPageId(4, true));
  EXPECT_EQ(makeView("value 3"), seg.find(hk3, lpid));
}


} // namespace navy
} // namespace cachelib
} // namespace facebook
