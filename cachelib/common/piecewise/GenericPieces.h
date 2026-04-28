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

#pragma once

#include <folly/logging/xlog.h>

#include <string>

#include "cachelib/common/piecewise/GenericPiecesBase.h"
#include "cachelib/common/piecewise/RequestRange.h"

namespace facebook {
namespace cachelib {

/**
 * A content can be split and stored in multiple pieces in cache. The class
 * provides the utility to map content object to pieces.
 *
 * This class handles single contiguous range requests.
 */
class GenericPieces : public GenericPiecesBase {
 public:
  /**
   * @param baseKey: base key of the request. we will generate piece key from
   * the base key.
   * @param pieceSize: byte size for each cache piece.
   * @param piecesPerGroup: # of pieces we put into a single group. we may want
   * to put each group onto a separate machine, depending on application.
   * @param fullBodyLen: the length of the full content (without the response
   * header), in regardless of the range.
   * @param range: define the range request.
   */
  GenericPieces(const std::string& baseKey,
                uint64_t pieceSize,
                uint64_t piecesPerGroup,
                uint64_t fullBodyLen,
                const RequestRange* range);

  /**
   * Overrides from GenericPiecesBase
   */
  uint64_t getStartPieceIndex() const override { return startPieceIndex_; }
  uint64_t getEndPieceIndex() const override { return endPieceIndex_; }

  uint64_t getTargetNumPieces() const override {
    return getEndPieceIndex() - getStartPieceIndex() + 1;
  }

  uint64_t getNumFetchedPieces() const override {
    return curFetchingPieceIndex_ - startPieceIndex_;
  }

  void updateFetchIndex() override { curFetchingPieceIndex_ += 1; }

  /**
   * Reset piece range based on the given request range.
   * If range is invalid, don't modify the piece range. Only reset the fetch
   * index.
   */
  void resetFromRequestRange(const RequestRange& range);

  void setFetchIndex(uint64_t pieceIndex) {
    curFetchingPieceIndex_ = pieceIndex;
  }

  uint64_t getRemainingBytes() const {
    if (curFetchingPieceIndex_ > getEndPieceIndex()) {
      return 0;
    }
    return getLastByteOffsetOfLastPiece() - getFirstByteOffsetOfCurPiece() + 1;
  }

  bool isPieceWithinBound(uint64_t pieceIndex) const {
    return pieceIndex <= getEndPieceIndex();
  }

  // Return the byte size of all pieces
  uint64_t getTotalSize() const {
    return getLastByteOffsetOfLastPiece() -
           getStartPieceIndex() * getPieceSize() + 1;
  }

 protected:
  // Start piece index of the request content (or range)
  uint64_t startPieceIndex_;
  // End piece index of the request content (or range)
  uint64_t endPieceIndex_;
};

} // namespace cachelib
} // namespace facebook
