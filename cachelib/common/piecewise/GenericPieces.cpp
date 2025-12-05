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

#include "cachelib/common/piecewise/GenericPieces.h"

#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {

GenericPieces::GenericPieces(const std::string& baseKey,
                             uint64_t pieceSize,
                             uint64_t piecesPerGroup,
                             uint64_t fullBodyLen,
                             const RequestRange* range)
    : GenericPiecesBase(baseKey, pieceSize, piecesPerGroup, fullBodyLen),
      requestedStartByte_{0},
      requestedEndByte_{fullBodyLen - 1},
      startPieceIndex_{0},
      endPieceIndex_{numPiecesTotal_ - 1} {
  if (range) {
    resetFromRequestRange(*range);
  }
}

void GenericPieces::resetFromRequestRange(const RequestRange& range) {
  const auto& requestRange = range.getRequestRange();
  if (requestRange.has_value()) {
    // Range request, might not need to fetch all the pieces
    requestedStartByte_ = requestRange->first;
    startPieceIndex_ = requestedStartByte_ / pieceSize_;
    if (requestRange->second.has_value()) {
      uint64_t requestedEndByte = requestRange->second.value();
      if (requestedEndByte < fullBodyLen_) {
        requestedEndByte_ = requestedEndByte;
        endPieceIndex_ = requestedEndByte_ / pieceSize_;
      }
    }
  }
  XCHECK_GE(endPieceIndex_, startPieceIndex_);

  curFetchingPieceIndex_ = startPieceIndex_;

  firstByteOffsetToFetch_ = startPieceIndex_ * pieceSize_;
}

uint64_t GenericPieces::getTargetNumPieces() const {
  XDCHECK_GE(endPieceIndex_, startPieceIndex_);
  return (endPieceIndex_ - startPieceIndex_) + 1;
}

} // namespace cachelib
} // namespace facebook
