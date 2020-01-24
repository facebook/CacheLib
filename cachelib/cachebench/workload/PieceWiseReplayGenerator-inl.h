#include "folly/String.h"

namespace {
// Line format: timestamp, cacheKey, OpType, size, TTL
constexpr uint32_t kTraceNumFields = 5;
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
      // without piece wise caching, this must be a miss, perform set
      // operation next
      auto it = activeReqM_.find(*lastRequestId);
      it->second.op = OpType::kSet;
      return it->second.req;
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
  if (result != OpResultType::kGetMiss && result != OpResultType::kSetFailure) {
    std::lock_guard<std::mutex> lock(lock_);
    auto it = activeReqM_.find(requestId);
    if (it != activeReqM_.end()) {
      activeReqM_.erase(it);
    } else {
      XLOG(INFO) << "request id not found: " << requestId;
    }
  }
}

const Request& PieceWiseReplayGenerator::getReqFromTrace() {
  std::string line;
  std::lock_guard<std::mutex> lock(lock_);
  while (std::getline(infile_, line)) {
    XLOG(INFO) << "Read line: " << line;
    std::vector<folly::StringPiece> fields;
    folly::split(",", line, fields);
    if (fields.size() == kTraceNumFields) {
      auto reqId = nextReqId_++;
      activeReqM_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(reqId),
                          std::forward_as_tuple(fields[1], fields[3], reqId));
      return activeReqM_.find(reqId)->second.req;
    }
  }

  // Return an invalid request
  return activeReqM_.find(kInvalidRequestId)->second.req;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
