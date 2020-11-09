#include <folly/portability/GTest.h>

#include "cachelib/common/piecewise/RequestRange.h"

using facebook::cachelib::RequestRange;

TEST(RequestRangeTests, Parsing) {
  RequestRange hr(0, 499);
  EXPECT_TRUE(hr.getRequestRange().has_value());
  EXPECT_EQ(0, hr.getRequestRange()->first);
  EXPECT_TRUE(hr.getRequestRange()->second.has_value());
  EXPECT_EQ(499, hr.getRequestRange()->second.value());

  hr = RequestRange(500, 999);
  EXPECT_TRUE(hr.getRequestRange().has_value());
  EXPECT_EQ(500, hr.getRequestRange()->first);
  EXPECT_TRUE(hr.getRequestRange()->second.has_value());
  EXPECT_EQ(999, hr.getRequestRange()->second.value());

  hr = RequestRange(folly::none, 500);
  EXPECT_FALSE(hr.getRequestRange().has_value());

  hr = RequestRange(999, 500);
  EXPECT_FALSE(hr.getRequestRange().has_value());

  hr = RequestRange(9500, folly::none);
  EXPECT_TRUE(hr.getRequestRange().has_value());
  EXPECT_EQ(9500, hr.getRequestRange()->first);
  EXPECT_FALSE(hr.getRequestRange()->second.has_value());

  hr = RequestRange(folly::none, folly::none);
  EXPECT_FALSE(hr.getRequestRange().has_value());

  hr = RequestRange(folly::none);
  EXPECT_FALSE(hr.getRequestRange().has_value());
}
