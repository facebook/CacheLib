#include "folly/String.h"

#include "cachelib/cachebench/util/Exceptions.h"

namespace {
constexpr uint32_t kTraceNumFields = 10;
} // namespace

namespace facebook {
namespace cachelib {
namespace cachebench {

const Request& PieceWiseReplayGenerator::getReq(
    uint8_t, std::mt19937&, std::optional<uint64_t> lastRequestId) {
  {
    std::lock_guard<std::mutex> lock(lock_);
    if (lastRequestId && activeReqM_.count(*lastRequestId) > 0) {
      return activeReqM_.find(*lastRequestId)->second.req;
    }
  }

  return getReqFromTrace();
}

OpType PieceWiseReplayGenerator::getOp(uint8_t,
                                       std::mt19937&,
                                       std::optional<uint64_t> requestId) {
  std::lock_guard<std::mutex> lock(lock_);
  if (requestId && activeReqM_.count(*requestId) > 0) {
    return activeReqM_.find(*requestId)->second.op;
  } else {
    return OpType::kGet;
  }
}

void PieceWiseReplayGenerator::notifyResult(uint64_t requestId,
                                            OpResultType result) {
  std::lock_guard<std::mutex> lock(lock_);
  auto it = activeReqM_.find(requestId);

  if (it == activeReqM_.end()) {
    XLOG(INFO) << "Request id not found: " << requestId;
    return;
  }

  if (it->second.cachePieces) {
    updatePieceProcessing(it, result);
  } else {
    if (result == OpResultType::kGetHit ||
        result == OpResultType::kSetSuccess ||
        result == OpResultType::kSetFailure) {
      // Record the cache hit stats
      if (result == OpResultType::kGetHit) {
        // We trim the fetched bytes if it's range request
        if (it->second.requestRange.getRequestRange()) {
          auto range = it->second.requestRange.getRequestRange();
          size_t rangeSize = range->second
                                 ? (*range->second - range->first + 1)
                                 : (it->second.sizes[0] -
                                    it->second.headerSize - range->first);
          stats_.getHitBytes += rangeSize + it->second.headerSize;
          stats_.getHitBodyBytes += rangeSize;
        } else {
          stats_.getHitBytes += it->second.sizes[0];
          stats_.getHitBodyBytes += it->second.sizes[0] - it->second.headerSize;
        }

        stats_.objGetHits += 1;
      }

      activeReqM_.erase(it);
    } else if (result == OpResultType::kGetMiss) {
      // Perform set operation next
      it->second.op = OpType::kSet;
    } else {
      XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
    }
  }
}

void PieceWiseReplayGenerator::updatePieceProcessing(
    std::unordered_map<uint64_t, ReqWrapper>::iterator it,
    OpResultType result) {
  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure) {
    // The piece index we need to fetch next
    auto nextPieceIndex = it->second.cachePieces->getCurFetchingPieceIndex();

    // Record the cache hit stats
    if (result == OpResultType::kGetHit) {
      if (it->second.isHeaderPiece) {
        stats_.getHitBytes += it->second.sizes[0];
      } else {
        auto resultPieceIndex = nextPieceIndex - 1;
        auto requestedSize =
            it->second.cachePieces->getRequestedSizeOfAPiece(resultPieceIndex);
        stats_.getHitBytes += requestedSize;
        stats_.getHitBodyBytes += requestedSize;
      }
    }

    // For pieces that are beyond pieces number limit, we don't store them
    if (it->second.cachePieces->isPieceWithinBound(nextPieceIndex) &&
        nextPieceIndex < config_.maxCachePieces) {
      // first set the correct key. Header piece has already been fetched,
      // this is now a body piece.
      it->second.pieceKey = GenericPieces::createPieceKey(
          it->second.baseKey,
          nextPieceIndex,
          it->second.cachePieces->getPiecesPerGroup());

      // Set the size of the piece
      it->second.sizes[0] =
          it->second.cachePieces->getSizeOfAPiece(nextPieceIndex);

      // Set the operation type
      if (result == OpResultType::kGetHit) {
        it->second.op = OpType::kGet;
      } else {
        // Once we start to set a piece, we set all subsequent pieces
        it->second.op = OpType::kSet;
      }

      // Update the piece fetch index
      it->second.isHeaderPiece = false;
      it->second.cachePieces->updateFetchIndex();
    } else {
      // Record the cache hit stats
      if (result == OpResultType::kGetHit) {
        stats_.objGetHits += 1;
      }

      activeReqM_.erase(it);
    }
  } else if (result == OpResultType::kGetMiss) {
    // Perform set operation next for the current piece
    it->second.op = OpType::kSet;
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
  }
}

void PieceWiseReplayGenerator::renderStats(uint64_t elapsedTimeNs,
                                           std::ostream& out) const {
  out << std::endl << "== PieceWiseReplayGenerator Stats ==" << std::endl;
  PieceWiseReplayGeneratorStats curStats;

  {
    std::lock_guard<std::mutex> lock(lock_);
    curStats = stats_;
  }

  // Output the stats
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);
  const uint64_t getBytesPerSec = curStats.getBytes / 1024 / elapsedSecs;
  const double getBytesSuccessRate =
      curStats.getBytes == 0 ? 0.0
                             : 100.0 * curStats.getHitBytes / curStats.getBytes;

  const uint64_t getBodyBytesPerSec =
      curStats.getBodyBytes / 1024 / elapsedSecs;
  const double getBodyBytesSuccessRate =
      curStats.getBodyBytes == 0
          ? 0.0
          : 100.0 * curStats.getHitBodyBytes / curStats.getBodyBytes;

  const uint64_t getPerSec = curStats.objGets / elapsedSecs;
  const double getSuccessRate =
      curStats.objGets == 0 ? 0.0
                            : 100.0 * curStats.objGetHits / curStats.objGets;

  auto outFn = [&out](folly::StringPiece k1, uint64_t v1, folly::StringPiece k2,
                      double v2) {
    out << folly::sformat("{:10}: {:9,}/s, {:10}: {:6.2f}%", k1, v1, k2, v2)
        << std::endl;
  };
  outFn("getBytes(KB)", getBytesPerSec, "success", getBytesSuccessRate);
  outFn("getBodyBytes(KB)", getBodyBytesPerSec, "success",
        getBodyBytesSuccessRate);
  outFn("objectGet", getPerSec, "success", getSuccessRate);
}

const Request& PieceWiseReplayGenerator::getReqFromTrace() {
  std::string line;
  std::lock_guard<std::mutex> lock(lock_);
  while (std::getline(infile_, line)) {
    try {
      std::vector<folly::StringPiece> fields;
      // Line format:
      // timestamp, cacheKey, OpType, objectSize, responseSize,
      // responseHeaderSize, rangeStart, rangeEnd, TTL, samplingRate
      folly::split(",", line, fields);
      if (fields.size() == kTraceNumFields) {
        // Invalid sample: cacheKey is empty, objectSize is not positive
        if (!fields[1].compare("-") || !fields[1].compare("") ||
            folly::to<int64_t>(fields[3].str()) <= 0) {
          ++invalidSamples_;
          continue;
        }

        auto parseRangeField = [](folly::StringPiece range,
                                  size_t contentSize) {
          folly::Optional<uint64_t> result;
          // Negative value means it's not range request
          auto val = folly::to<int64_t>(range);
          if (val >= 0) {
            // range index can not be larger than content size
            result = std::min(static_cast<size_t>(val), contentSize - 1);
          } else {
            result = folly::none;
          }

          return result;
        };

        auto fullContentSize = folly::to<size_t>(fields[3].str());
        auto responseHeaderSize = folly::to<size_t>(fields[5].str());
        auto rangeStart = parseRangeField(fields[6], fullContentSize);
        auto rangeEnd = parseRangeField(fields[7], fullContentSize);

        // Record the byte wise and object wise stats that we will egress
        if (rangeStart) {
          size_t rangeSize = rangeEnd ? (*rangeEnd - *rangeStart + 1)
                                      : (fullContentSize - *rangeStart);
          stats_.getBytes += rangeSize + responseHeaderSize;
          stats_.getBodyBytes += rangeSize;
        } else {
          stats_.getBytes += fullContentSize + responseHeaderSize;
          stats_.getBodyBytes += fullContentSize;
        }
        stats_.objGets += 1;

        auto reqId = nextReqId_++;
        activeReqM_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(reqId),
                            std::forward_as_tuple(config_,
                                                  reqId,
                                                  fields[1],
                                                  fullContentSize,
                                                  responseHeaderSize,
                                                  rangeStart,
                                                  rangeEnd));
        return activeReqM_.find(reqId)->second.req;
      }
    } catch (const std::exception& e) {
      XLOG(ERR) << "Processing line: " << line
                << ", causes exception: " << e.what();
    }
  }

  throw cachelib::cachebench::EndOfTrace("");
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
