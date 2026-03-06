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

#include <folly/Range.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace facebook {
namespace cachelib {

extern const std::string kCacheGroupSeparator;
extern const std::string kCachePieceSeparator;

/**
 * Base class for managing content split into multiple pieces in cache.
 * Provides non-request-specific utilities for piece management.
 */
class GenericPiecesBase {
 public:
  /**
   * @param baseKey: base key of the request. we will generate piece key from
   * the base key.
   * @param pieceSize: byte size for each cache piece.
   * @param piecesPerGroup: # of pieces we put into a single group. we may want
   * to put each group onto a separate machine, depending on application.
   * @param fullBodyLen: the length of the full content (without the response
   * header), in regardless of the range.
   */
  GenericPiecesBase(const std::string& baseKey,
                    uint64_t pieceSize,
                    uint64_t piecesPerGroup,
                    uint64_t fullBodyLen);

  virtual ~GenericPiecesBase() = default;

  // ========================================================================
  // Pure virtual methods for piece range.
  // These behave differently depending on whether the requested range is
  // single-range or multi-range.
  // ========================================================================

  virtual uint64_t getStartPieceIndex() const = 0;
  virtual uint64_t getEndPieceIndex() const = 0;

  // Get the number of pieces we need to fetch (excluding the header piece).
  virtual uint64_t getTargetNumPieces() const = 0;

  // Get the number of pieces fetched so far (before the current fetch index).
  virtual uint64_t getNumFetchedPieces() const = 0;

  // Indicates we finished fetching a piece and are ready to fetch the next one.
  virtual void updateFetchIndex() = 0;

  // ========================================================================
  // Basic getters for piece and body info.
  // ========================================================================

  uint64_t getCurFetchingPieceIndex() const { return curFetchingPieceIndex_; }
  uint64_t getPieceSize() const { return pieceSize_; }
  uint64_t getPiecesPerGroup() const { return numPiecesPerGroup_; }
  /**
   * Returns the body-length of the *full* blob (e.g. if there is a
   * 1000000-byte blob and 6400 bytes are requested in a range request, this
   * will still return 1000000)
   */
  uint64_t getFullBodyLength() const { return fullBodyLen_; }
  uint64_t getNumPiecesTotal() const { return numPiecesTotal_; }

  // ========================================================================
  // Common piece iteration methods.
  // These use virtual getters and work for both single-range and multi-range.
  // ========================================================================

  bool morePiecesToFetch() const {
    return curFetchingPieceIndex_ < getEndPieceIndex();
  }

  uint64_t getFirstByteOffsetOfCurPiece() const {
    return curFetchingPieceIndex_ * getPieceSize();
  }

  uint64_t getLastByteOffsetOfLastPiece() const {
    return std::min((getEndPieceIndex() + 1) * getPieceSize() - 1,
                    getFullBodyLength() - 1);
  }

  uint64_t getSizeOfAPiece(uint64_t pieceIndex) const {
    // The size is full piece size when it's not the last piece
    if (pieceIndex < getEndPieceIndex()) {
      return getPieceSize();
    } else {
      return getLastByteOffsetOfLastPiece() % getPieceSize() + 1;
    }
  }

  uint64_t getFirstByteOffsetToFetch() const {
    return getStartPieceIndex() * getPieceSize();
  }

  // ========================================================================
  // Static utility methods for cache key management.
  // ========================================================================

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
   * @param versionID: unique identifier of the content's version, e.g.,
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

  uint64_t curFetchingPieceIndex_{0};

  // Calculated values
  // Total number of pieces for the full content
  uint64_t numPiecesTotal_;
};

} // namespace cachelib
} // namespace facebook
