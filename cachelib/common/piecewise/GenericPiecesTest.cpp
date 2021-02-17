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
  EXPECT_EQ(0, cp.getCurFetchingPieceIndex());
  EXPECT_EQ(999, cp.getLastByteOffsetOfLastPiece());
  EXPECT_EQ(256, cp.getSizeOfAPiece(0));
  EXPECT_EQ(256, cp.getRequestedSizeOfAPiece(0));
  EXPECT_EQ(232, cp.getSizeOfAPiece(3));
  EXPECT_EQ(232, cp.getRequestedSizeOfAPiece(3));
  EXPECT_EQ(1000, cp.getTotalSize());
  EXPECT_EQ(0, cp.getBytesToTrimAtStart());
  EXPECT_EQ(0, cp.getBytesToTrimAtEnd());
  EXPECT_EQ(1000, cp.getRequestedSize());
  EXPECT_EQ(1000, cp.getRemainingBytes());
  cp.updateFetchIndex();
  EXPECT_EQ(744, cp.getRemainingBytes());

  // Spanning multiple pieces
  hr = RequestRange(260, 759);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(4, cp.getNumPiecesTotal());
  EXPECT_EQ(260, cp.getRequestedStartByte());
  EXPECT_EQ(759, cp.getRequestedEndByte());
  EXPECT_EQ(1, cp.getStartPieceIndex());
  EXPECT_EQ(2, cp.getEndPieceIndex());
  EXPECT_EQ(1, cp.getCurFetchingPieceIndex());
  EXPECT_EQ(767, cp.getLastByteOffsetOfLastPiece());
  EXPECT_EQ(256, cp.getSizeOfAPiece(1));
  EXPECT_EQ(252, cp.getRequestedSizeOfAPiece(1));
  EXPECT_EQ(256, cp.getSizeOfAPiece(2));
  EXPECT_EQ(248, cp.getRequestedSizeOfAPiece(2));
  EXPECT_EQ(512, cp.getTotalSize());
  EXPECT_EQ(500, cp.getRequestedSize());
  EXPECT_EQ(512, cp.getRemainingBytes());
  EXPECT_EQ(4, cp.getBytesToTrimAtStart());
  EXPECT_EQ(8, cp.getBytesToTrimAtEnd());
  cp.updateFetchIndex();
  EXPECT_EQ(256, cp.getRemainingBytes());

  // All in one piece
  hr = RequestRange(800, 995);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(800, cp.getRequestedStartByte());
  EXPECT_EQ(995, cp.getRequestedEndByte());
  EXPECT_EQ(3, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());
  EXPECT_EQ(3, cp.getCurFetchingPieceIndex());
  EXPECT_EQ(999, cp.getLastByteOffsetOfLastPiece());
  EXPECT_EQ(232, cp.getSizeOfAPiece(3));
  EXPECT_EQ(196, cp.getRequestedSizeOfAPiece(3));
  EXPECT_EQ(232, cp.getTotalSize());
  EXPECT_EQ(196, cp.getRequestedSize());
  EXPECT_EQ(232, cp.getRemainingBytes());
  EXPECT_EQ(32, cp.getBytesToTrimAtStart());
  EXPECT_EQ(4, cp.getBytesToTrimAtEnd());
  cp.updateFetchIndex();
  EXPECT_EQ(0, cp.getRemainingBytes());

  // No end specified
  hr = RequestRange(400, folly::none);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(400, cp.getRequestedStartByte());
  EXPECT_EQ(999, cp.getRequestedEndByte());
  EXPECT_EQ(1, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());
  EXPECT_EQ(1, cp.getCurFetchingPieceIndex());
  EXPECT_EQ(999, cp.getLastByteOffsetOfLastPiece());
  EXPECT_EQ(256, cp.getSizeOfAPiece(1));
  EXPECT_EQ(112, cp.getRequestedSizeOfAPiece(1));
  EXPECT_EQ(232, cp.getSizeOfAPiece(3));
  EXPECT_EQ(232, cp.getRequestedSizeOfAPiece(3));
  EXPECT_EQ(744, cp.getTotalSize());
  EXPECT_EQ(600, cp.getRequestedSize());
  EXPECT_EQ(744, cp.getRemainingBytes());
  EXPECT_EQ(144, cp.getBytesToTrimAtStart());
  EXPECT_EQ(0, cp.getBytesToTrimAtEnd());
  cp.updateFetchIndex();
  EXPECT_EQ(488, cp.getRemainingBytes());

  // Specified end is way past actual end
  hr = RequestRange(400, 56789);
  EXPECT_TRUE(hr.getRequestRange().has_value()); // Sanity check
  cp = GenericPieces("/mykey", 256, 0, 1000, &hr);
  EXPECT_EQ(400, cp.getRequestedStartByte());
  EXPECT_EQ(999, cp.getRequestedEndByte());
  EXPECT_EQ(1, cp.getStartPieceIndex());
  EXPECT_EQ(3, cp.getEndPieceIndex());
  EXPECT_EQ(1, cp.getCurFetchingPieceIndex());
  EXPECT_EQ(999, cp.getLastByteOffsetOfLastPiece());
  EXPECT_EQ(256, cp.getSizeOfAPiece(1));
  EXPECT_EQ(112, cp.getRequestedSizeOfAPiece(1));
  EXPECT_EQ(232, cp.getSizeOfAPiece(3));
  EXPECT_EQ(232, cp.getRequestedSizeOfAPiece(3));
  EXPECT_EQ(744, cp.getTotalSize());
  EXPECT_EQ(600, cp.getRequestedSize());
  EXPECT_EQ(744, cp.getRemainingBytes());
  EXPECT_EQ(144, cp.getBytesToTrimAtStart());
  EXPECT_EQ(0, cp.getBytesToTrimAtEnd());
  cp.updateFetchIndex();
  EXPECT_EQ(488, cp.getRemainingBytes());
}
