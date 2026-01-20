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
   * Reset piece range based on the given request range.
   * If range is invalid, don't modify the piece range. Only reset the fetch
   * index.
   */
  void resetFromRequestRange(const RequestRange& range);

  bool morePiecesToFetch() const {
    return curFetchingPieceIndex_ < endPieceIndex_;
  }

  /**
   * Indicates we finished fetching a piece and are ready to fetch the
   * next one.
   */
  void updateFetchIndex() { curFetchingPieceIndex_ += 1; }

  void setFetchIndex(uint64_t pieceIndex) {
    curFetchingPieceIndex_ = pieceIndex;
  }

  uint64_t getFirstByteOffsetOfCurPiece() const {
    return curFetchingPieceIndex_ * getPieceSize();
  }

  uint64_t getLastByteOffsetOfLastPiece() const {
    return std::min((endPieceIndex_ + 1) * pieceSize_ - 1, fullBodyLen_ - 1);
  }

  uint64_t getRemainingBytes() const {
    if (curFetchingPieceIndex_ > endPieceIndex_) {
      return 0;
    }
    return getLastByteOffsetOfLastPiece() - getFirstByteOffsetOfCurPiece() + 1;
  }

  bool isPieceWithinBound(uint64_t pieceIndex) const {
    return pieceIndex <= endPieceIndex_;
  }

  uint64_t getSizeOfAPiece(uint64_t pieceIndex) const {
    // The size is full piece size when it's not the last piece
    if (pieceIndex < endPieceIndex_) {
      return pieceSize_;
    } else {
      return getLastByteOffsetOfLastPiece() % pieceSize_ + 1;
    }
  }

  // Return the byte size of all pieces
  uint64_t getTotalSize() const {
    return getLastByteOffsetOfLastPiece() - startPieceIndex_ * getPieceSize() +
           1;
  }

  uint64_t getStartPieceIndex() const { return startPieceIndex_; }
  uint64_t getEndPieceIndex() const { return endPieceIndex_; }
  uint64_t getFirstByteOffsetToFetch() const {
    return startPieceIndex_ * getPieceSize();
  }

  /**
   * Get the number of pieces we need to fetch (excluding the header piece)
   */
  uint64_t getTargetNumPieces() const;

 protected:
  // Start piece index of the request content (or range)
  uint64_t startPieceIndex_;
  // End piece index of the request content (or range)
  uint64_t endPieceIndex_;
};

} // namespace cachelib
} // namespace facebook
