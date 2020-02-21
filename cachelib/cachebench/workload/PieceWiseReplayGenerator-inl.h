#include "folly/String.h"

#include "cachelib/cachebench/util/Exceptions.h"

namespace {
// Line format: timestamp, cacheKey, OpType, size, TTL
constexpr uint32_t kTraceNumFields = 9;
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
    auto pieceIndex = it->second.cachePieces->getCurFetchingPieceIndex();

    // For pieces that are beyond pieces number limit, we don't store them
    if (it->second.cachePieces->isPieceWithinBound(pieceIndex) &&
        pieceIndex < config_.maxCachePieces) {
      // first set the correct key. Header piece has already been fetched,
      // this is now a body piece.
      it->second.pieceKey = GenericPieces::createPieceKey(
          it->second.baseKey,
          pieceIndex,
          it->second.cachePieces->getPiecesPerGroup());

      // Set the size of the piece
      it->second.sizes[0] = it->second.cachePieces->getSizeOfAPiece(pieceIndex);

      // Set the operation type
      if (result == OpResultType::kGetHit) {
        it->second.op = OpType::kGet;
      } else {
        // Once we start to set a piece, we set all subsequent pieces
        it->second.op = OpType::kSet;
      }

      // Update the piece fetch index
      it->second.cachePieces->updateFetchIndex();
    } else {
      activeReqM_.erase(it);
    }
  } else if (result == OpResultType::kGetMiss) {
    // Perform set operation next for the current piece
    it->second.op = OpType::kSet;
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
  }
}

const Request& PieceWiseReplayGenerator::getReqFromTrace() {
  std::string line;
  std::lock_guard<std::mutex> lock(lock_);
  while (std::getline(infile_, line)) {
    std::vector<folly::StringPiece> fields;
    // Line format:
    // timestamp, cacheKey, OpType, objectSize, responseSize, rangeStart,
    // rangeEnd, TTL, samplingRate
    folly::split(",", line, fields);
    if (fields.size() == kTraceNumFields) {
      auto reqId = nextReqId_++;

      auto parseRangeField = [](folly::StringPiece p) {
        auto val = folly::to<int64_t>(p);
        return val >= 0 ? folly::Optional<uint64_t>(val) : folly::none;
      };

      auto rangeStart = parseRangeField(fields[5]);
      auto rangeEnd = parseRangeField(fields[6]);
      // TODO: set correct response header size after T62193035.
      size_t responseHeaderSize = 500;
      activeReqM_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(reqId),
          std::forward_as_tuple(config_,
                                reqId,
                                fields[1],
                                folly::to<size_t>(fields[3].str()),
                                responseHeaderSize,
                                rangeStart,
                                rangeEnd));
      return activeReqM_.find(reqId)->second.req;
    }
  }

  throw cachelib::cachebench::EndOfTrace("");
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
