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

  /**
   * We fetch one piece at a time and keep track of that piece
   * number here.
   */
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
