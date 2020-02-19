#include "cachelib/common/piecewise/GenericPieces.h"

#include <vector>

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {

GenericPieces::GenericPieces(const std::string& baseKey,
                             uint64_t pieceSize,
                             uint64_t piecesPerGroup,
                             uint64_t fullBodyLen,
                             const RequestRange* range)
    : baseKey_(baseKey),
      pieceSize_(pieceSize),
      numPiecesPerGroup_(piecesPerGroup),
      fullBodyLen_(fullBodyLen) {
  XCHECK(fullBodyLen > 0);

  numPiecesTotal_ = calculateNumPiecesTotal(fullBodyLen_, pieceSize_);

  // Figure out the first and last bytes/pieces we need to fetch
  requestedStartByte_ = 0;
  requestedEndByte_ = fullBodyLen_ - 1;
  startPieceIndex_ = 0;
  endPieceIndex_ = numPiecesTotal_ - 1;

  if (range) {
    const auto& requestRange = range->getRequestRange();
    if (requestRange.hasValue()) {
      // Range request, might not need to fetch all the pieces
      requestedStartByte_ = requestRange->first;
      startPieceIndex_ = requestedStartByte_ / pieceSize_;
      if (requestRange->second.hasValue()) {
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
}

std::string GenericPieces::escapeCacheKey(const std::string& key) {
  std::vector<folly::StringPiece> parts;
  folly::split(kCachePieceSeparator, key, parts);
  if (parts.size() == 1) {
    return key;
  }
  std::string escapeSeparator =
      folly::to<std::string>(kCachePieceSeparator, kCachePieceSeparator);

  std::string ret;
  folly::join(escapeSeparator, parts, ret);
  return ret;
}

uint64_t GenericPieces::getTargetNumPieces() const {
  XDCHECK_GE(endPieceIndex_, startPieceIndex_);
  return (endPieceIndex_ - startPieceIndex_) + 1;
}

std::string GenericPieces::createPieceHeaderKey(const std::string& baseKey,
                                                uint64_t versionID) {
  return folly::to<std::string>(
      baseKey, kCachePieceSeparator, "header-", versionID);
}

std::string GenericPieces::createPieceKey(const std::string& baseKey,
                                          size_t pieceNum,
                                          uint64_t piecesPerGroup,
                                          uint64_t versionID) {
  uint64_t groupNum = 0;
  if (piecesPerGroup > 0) {
    groupNum = pieceNum / piecesPerGroup;
  }
  if (groupNum == 0) {
    return folly::to<std::string>(
        baseKey, kCachePieceSeparator, "body-", versionID, "-", pieceNum);
  } else {
    return folly::to<std::string>(baseKey,
                                  kCacheGroupSeparator,
                                  groupNum,
                                  kCachePieceSeparator,
                                  "body-",
                                  versionID,
                                  "-",
                                  pieceNum);
  }
}

} // namespace cachelib
} // namespace facebook
