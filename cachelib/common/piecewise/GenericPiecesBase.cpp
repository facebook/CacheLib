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

#include "cachelib/common/piecewise/GenericPiecesBase.h"

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>

#include <vector>

namespace facebook {
namespace cachelib {

const std::string kCacheGroupSeparator = "|@|";
const std::string kCachePieceSeparator = "|#|";

GenericPiecesBase::GenericPiecesBase(const std::string& baseKey,
                                     uint64_t pieceSize,
                                     uint64_t piecesPerGroup,
                                     uint64_t fullBodyLen)
    : baseKey_{baseKey},
      pieceSize_{pieceSize},
      numPiecesPerGroup_{piecesPerGroup},
      fullBodyLen_{fullBodyLen} {
  XCHECK(fullBodyLen > 0);
  numPiecesTotal_ = calculateNumPiecesTotal(fullBodyLen_, pieceSize_);
}

std::string GenericPiecesBase::escapeCacheKey(const std::string& key) {
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

std::string GenericPiecesBase::createPieceHeaderKey(const std::string& baseKey,
                                                    uint64_t versionID) {
  return folly::to<std::string>(
      baseKey, kCachePieceSeparator, "header-", versionID);
}

std::string GenericPiecesBase::createPieceKey(const std::string& baseKey,
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

folly::StringPiece GenericPiecesBase::getBaseKey(folly::StringPiece key) {
  auto found = key.find(kCacheGroupSeparator);
  if (found == folly::StringPiece::npos) {
    found = key.find(kCachePieceSeparator);
  }

  if (found != folly::StringPiece::npos) {
    return key.subpiece(0, found);
  }
  return key;
}

} // namespace cachelib
} // namespace facebook
