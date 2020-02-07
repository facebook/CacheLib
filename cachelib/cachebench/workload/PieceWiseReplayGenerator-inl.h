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
  // TODO: implement piece-wise caching and range request logic
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
  // TODO: implement piece-wise caching and range request logic
  std::lock_guard<std::mutex> lock(lock_);
  auto it = activeReqM_.find(requestId);

  if (it == activeReqM_.end()) {
    XLOG(INFO) << "Request id not found: " << requestId;
    return;
  }

  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure) {
    activeReqM_.erase(it);
  } else if (result == OpResultType::kGetMiss) {
    // Perform set operation next
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
      // TODO: support range request
      activeReqM_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(reqId),
                          std::forward_as_tuple(fields[1], fields[4], reqId));
      return activeReqM_.find(reqId)->second.req;
    }
  }

  throw cachelib::cachebench::EndOfTrace("");
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
