#include <folly/portability/GTest.h>

#include "cachelib/common/piecewise/GenericPieces.h"

using facebook::cachelib::GenericPieces;
using facebook::cachelib::RequestRange;

TEST(GenericPiecesTests, Normal) {
  RequestRange hr(folly::none);
  EXPECT_FALSE(hr.getRequestRange().has_value());
  GenericPieces cp("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(4, cp.getNumPiecesTotal());
  EXPECT_EQ(0, cp.getRequestedStartByte());
  EXPECT_EQ(999, cp.getRequestedEndByte());
  EXPECT_EQ(0, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());

  // Spanning multiple pieces
  hr = RequestRange(260, 759);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(4, cp.getNumPiecesTotal());
  EXPECT_EQ(260, cp.getRequestedStartByte());
  EXPECT_EQ(759, cp.getRequestedEndByte());
  EXPECT_EQ(1, cp.getStartPieceIndex());
  EXPECT_EQ(2, cp.getEndPieceIndex());

  // All in one piece
  hr = RequestRange(800, 995);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(800, cp.getRequestedStartByte());
  EXPECT_EQ(995, cp.getRequestedEndByte());
  EXPECT_EQ(3, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());

  // No end specified
  hr = RequestRange(400, folly::none);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(400, cp.getRequestedStartByte());
  EXPECT_EQ(999, cp.getRequestedEndByte());
  EXPECT_EQ(1, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());

  // Specified end is way past actual end
  hr = RequestRange(400, 56789);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(400, cp.getRequestedStartByte());
  EXPECT_EQ(999, cp.getRequestedEndByte());
  EXPECT_EQ(1, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());
}
