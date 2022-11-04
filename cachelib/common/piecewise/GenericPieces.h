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

#include <folly/dynamic.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "cachelib/common/piecewise/RequestRange.h"

namespace facebook {
namespace cachelib {

const std::string kCacheGroupSeparator = "|@|";
const std::string kCachePieceSeparator = "|#|";

/**
 * A content can be split and stored in multiple pieces in cache. The class
 * provides the utility to map content object to pieces.
 *
 */
class GenericPieces {
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

  void resetFromRequestRange(const RequestRange& range);

  /**
   * We fetch one piece at a time and keep track of that piece
   * number here.
   */
  uint64_t getCurFetchingPieceIndex() const { return curFetchingPieceIndex_; }

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

  uint64_t getPieceSize() const { return pieceSize_; }
  uint64_t getPiecesPerGroup() const { return numPiecesPerGroup_; }

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

  uint64_t getRequestedSizeOfAPiece(uint64_t pieceIndex) const {
    if (startPieceIndex_ == endPieceIndex_) {
      XDCHECK_EQ(pieceIndex, startPieceIndex_);
      return requestedEndByte_ - requestedStartByte_ + 1;
    }

    if (pieceIndex == startPieceIndex_) {
      // For the first piece, trim bytes before requestedStartByte_
      return pieceSize_ - requestedStartByte_ % pieceSize_;
    } else if (pieceIndex == endPieceIndex_) {
      // For the last piece, trim bytes after requestedEndByte_
      return requestedEndByte_ % pieceSize_ + 1;
    } else {
      return pieceSize_;
    }
  }

  uint64_t getBytesToTrimAtStart() const {
    return requestedStartByte_ - startPieceIndex_ * pieceSize_;
  }

  uint64_t getBytesToTrimAtEnd() const {
    return getLastByteOffsetOfLastPiece() - requestedEndByte_;
  }

  uint64_t getRequestedSize() const {
    return (requestedEndByte_ - requestedStartByte_ + 1);
  }

  /**
   * Returns the body-length of the *full* blob (e.g. if there is a
   * 1000000-byte blob and 6400 bytes are requested in a range request, this
   * will still return 1000000)
   */
  uint64_t getFullBodyLength() const { return fullBodyLen_; }
  uint64_t getStartPieceIndex() const { return startPieceIndex_; }
  [[nodiscard]] uint64_t getStartPieceOffset() const {
    return startPieceIndex_ * pieceSize_;
  }
  uint64_t getEndPieceIndex() const { return endPieceIndex_; }
  uint64_t getNumPiecesTotal() const { return numPiecesTotal_; }
  uint64_t getRequestedStartByte() const { return requestedStartByte_; }
  uint64_t getRequestedEndByte() const { return requestedEndByte_; }
  uint64_t getFirstByteOffsetToFetch() const { return firstByteOffsetToFetch_; }

  /**
   * Get the number of pieces we need to fetch (excluding the header piece)
   */
  uint64_t getTargetNumPieces() const;

  /**
   * We use "|#|" as the separator between the actual cachekey and meta
   * key information (Is it a header?  Is it a piece?  Which piece?)  So
   * we want to make sure this separator is escaped in the main key.  Do this
   * by doubling it whenever we see it.  If we ever see the single separator
   * string by itself, we know it's actually the separator.
   */
  static std::string escapeCacheKey(const std::string& key);

  /**
   * Get the basekey from the given pieceKey (full key). The returned value
   * shares the same lifetime as the passed in pieceKey.
   */
  static folly::StringPiece getBaseKey(folly::StringPiece pieceKey);

  /**
   * @param versionID: unique identifer of the content's version, e.g.,
   * hash of the content.
   */
  static std::string createPieceHeaderKey(const std::string& baseKey,
                                          uint64_t versionID = 0);

  /**
   * Keys used to store each piece of the response.  We include the pieceSize
   * in the key in case we change pieceSize at some point, so we can
   * distinguish between the different values.
   */
  static std::string createPieceKey(const std::string& baseKey,
                                    size_t pieceNum,
                                    uint64_t piecesPerGroup,
                                    uint64_t versionID = 0);

  static uint64_t calculateNumPiecesTotal(const uint64_t fullBodyLen,
                                          const uint64_t pieceSize) {
    return ((fullBodyLen - 1) / pieceSize) + 1;
  }

 protected:
  std::string baseKey_;
  uint64_t pieceSize_;
  uint64_t numPiecesPerGroup_;
  uint64_t fullBodyLen_;

  uint64_t curFetchingPieceIndex_;

  // Calculated values
  // Total number of pieces for the full content
  uint64_t numPiecesTotal_;
  // Start byte of the request content (or range)
  uint64_t requestedStartByte_;
  // End byte of the request content (or range)
  uint64_t requestedEndByte_;
  // Start piece index of he request content (or range)
  uint64_t startPieceIndex_;
  // End piece index of he request content (or range)
  uint64_t endPieceIndex_;
  // Starting byte offset of the first piece
  uint64_t firstByteOffsetToFetch_;
};

} // namespace cachelib
} // namespace facebook
