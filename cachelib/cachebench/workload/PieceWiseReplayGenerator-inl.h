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
  if (lastRequestId) {
    auto shard = getShard(*lastRequestId);
    LockHolder lock(activeReqLock_[shard]);
    auto it = activeReqM_[shard].find(*lastRequestId);
    if (it != activeReqM_[shard].end()) {
      return it->second.req;
    }
  }

  return getReqFromTrace();
}

void PieceWiseReplayGenerator::notifyResult(uint64_t requestId,
                                            OpResultType result) {
  auto shard = getShard(requestId);
  LockHolder lock(activeReqLock_[shard]);
  auto it = activeReqM_[shard].find(requestId);

  if (it == activeReqM_[shard].end()) {
    XLOG(INFO) << "Request id not found: " << requestId;
    return;
  }

  ReqWrapper& rw = it->second;
  if (rw.cachePieces) {
    bool done = updatePieceProcessing(rw, result);
    if (done) {
      activeReqM_[shard].erase(it);
    }
    return;
  }

  // Now we make sure the object is not stored in pieces, and it should be
  // stored along with the response header
  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure) {
    // Record the cache hit stats
    if (!isPrepopulate() && result == OpResultType::kGetHit) {
      // We trim the fetched bytes if it's range request
      if (rw.requestRange.getRequestRange()) {
        auto range = rw.requestRange.getRequestRange();
        size_t rangeSize = range->second
                               ? (*range->second - range->first + 1)
                               : (rw.sizes[0] - rw.headerSize - range->first);
        stats_.getHitBytes.add(rangeSize + rw.headerSize);
        stats_.getFullHitBytes.add(rangeSize + rw.headerSize);
        stats_.getHitBodyBytes.add(rangeSize);
        stats_.getFullHitBodyBytes.add(rangeSize);
      } else {
        stats_.getHitBytes.add(rw.sizes[0]);
        stats_.getFullHitBytes.add(rw.sizes[0]);
        stats_.getHitBodyBytes.add(rw.sizes[0] - rw.headerSize);
        stats_.getFullHitBodyBytes.add(rw.sizes[0] - rw.headerSize);
      }
      stats_.objGetHits.inc();
      stats_.objGetFullHits.inc();
    }
    activeReqM_[shard].erase(it);
  } else if (result == OpResultType::kGetMiss) {
    // Perform set operation next
    rw.req.setOp(OpType::kSet);
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
  }
}

bool PieceWiseReplayGenerator::updatePieceProcessing(ReqWrapper& rw,
                                                     OpResultType result) {
  // we are only done if we got everything.
  bool done = false;
  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure) {
    // The piece index we need to fetch next
    auto nextPieceIndex = rw.cachePieces->getCurFetchingPieceIndex();

    // Record the cache hit stats
    if (!isPrepopulate() && result == OpResultType::kGetHit) {
      if (rw.isHeaderPiece) {
        stats_.getHitBytes.add(rw.sizes[0]);
        stats_.objGetHits.inc();
      } else {
        auto resultPieceIndex = nextPieceIndex - 1;
        // getRequestedSizeOfAPiece() takes care of trim if needed
        auto requestedSize =
            rw.cachePieces->getRequestedSizeOfAPiece(resultPieceIndex);
        stats_.getHitBytes.add(requestedSize);
        stats_.getHitBodyBytes.add(requestedSize);
      }
    }

    // For pieces that are beyond pieces number limit, we don't store them
    if (rw.cachePieces->isPieceWithinBound(nextPieceIndex) &&
        nextPieceIndex < config_.maxCachePieces) {
      // first set the correct key. Header piece has already been fetched,
      // this is now a body piece.
      rw.pieceKey = GenericPieces::createPieceKey(
          rw.baseKey, nextPieceIndex, rw.cachePieces->getPiecesPerGroup());

      // Set the size of the piece
      rw.sizes[0] = rw.cachePieces->getSizeOfAPiece(nextPieceIndex);

      if (result == OpResultType::kGetHit) {
        rw.req.setOp(OpType::kGet); // fetch next piece
      } else {
        // Once we start to set a piece, we set all subsequent pieces
        rw.req.setOp(OpType::kSet);
      }

      // Update the piece fetch index
      rw.isHeaderPiece = false;
      rw.cachePieces->updateFetchIndex();
    } else {
      // Record the cache hit stats: we got all the pieces that were requested
      if (!isPrepopulate() && result == OpResultType::kGetHit) {
        auto requestedSize = rw.cachePieces->getRequestedSize();
        stats_.getFullHitBytes.add(requestedSize + rw.headerSize);
        stats_.getFullHitBodyBytes.add(requestedSize);
        stats_.objGetFullHits.inc();
      }
      // we are done
      done = true;
    }
  } else if (result == OpResultType::kGetMiss) {
    // Perform set operation next for the current piece
    rw.req.setOp(OpType::kSet);
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
  }
  return done;
}

void PieceWiseReplayGenerator::renderStats(uint64_t elapsedTimeNs,
                                           std::ostream& out) const {
  out << std::endl << "== PieceWiseReplayGenerator Stats ==" << std::endl;

  // Output the stats
  out << folly::sformat("{:10}: {:.2f} million", "Total Processed Samples",
                        stats_.objGets.get() / 1e6)
      << std::endl;

  auto safeDiv = [](auto nr, auto dr) {
    return dr == 0 ? 0.0 : 100.0 * nr / dr;
  };

  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);
  const uint64_t getBytesPerSec = stats_.getBytes.get() / 1024 / elapsedSecs;
  const double getBytesSuccessRate =
      safeDiv(stats_.getHitBytes.get(), stats_.getBytes.get());
  const double getBytesFullSuccessRate =
      safeDiv(stats_.getFullHitBytes.get(), stats_.getBytes.get());

  const uint64_t getBodyBytesPerSec =
      stats_.getBodyBytes.get() / 1024 / elapsedSecs;
  const double getBodyBytesSuccessRate =
      safeDiv(stats_.getHitBodyBytes.get(), stats_.getBodyBytes.get());
  const double getBodyBytesFullSuccessRate =
      safeDiv(stats_.getFullHitBodyBytes.get(), stats_.getBodyBytes.get());

  const uint64_t getPerSec = stats_.objGets.get() / elapsedSecs;
  const double getSuccessRate =
      safeDiv(stats_.objGetHits.get(), stats_.objGets.get());
  const double getFullSuccessRate =
      safeDiv(stats_.objGetFullHits.get(), stats_.objGets.get());

  auto outFn = [&out](folly::StringPiece k1, uint64_t v1, folly::StringPiece k2,
                      double v2, folly::StringPiece k3, double v3) {
    out << folly::sformat("{:10}: {:9,}/s, {:10}: {:6.2f}%, {:10}: {:6.2f}%",
                          k1, v1, k2, v2, k3, v3)
        << std::endl;
  };
  outFn("getBytes(KB)", getBytesPerSec, "success", getBytesSuccessRate,
        "full success", getBytesFullSuccessRate);
  outFn("getBodyBytes(KB)", getBodyBytesPerSec, "success",
        getBodyBytesSuccessRate, "full success", getBodyBytesFullSuccessRate);
  outFn("objectGet", getPerSec, "success", getSuccessRate, "full success",
        getFullSuccessRate);
}

const Request& PieceWiseReplayGenerator::getReqFromTrace() {
  std::string line;
  while (true) {
    {
      LockHolder lock(getLineLock_);
      if (!std::getline(infile_, line)) {
        throw cachelib::cachebench::EndOfTrace("");
      }
    }

    try {
      std::vector<folly::StringPiece> fields;
      // Line format:
      // timestamp, cacheKey, OpType, objectSize, responseSize,
      // responseHeaderSize, rangeStart, rangeEnd, TTL, samplingRate
      folly::split(",", line, fields);
      if (fields.size() != kTraceNumFields) {
        invalidSamples_.inc();
        continue;
      }
      // Invalid sample: cacheKey is empty, objectSize is not positive,
      // responseSize is not positive
      if (!fields[1].compare("-") || !fields[1].compare("") ||
          folly::to<int64_t>(fields[3].str()) <= 0 ||
          folly::to<int64_t>(fields[4].str()) <= 0) {
        invalidSamples_.inc();
        continue;
      }

      auto parseRangeField = [](folly::StringPiece range, size_t contentSize) {
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
      auto responseSize = folly::to<size_t>(fields[4].str());
      auto responseHeaderSize = folly::to<size_t>(fields[5].str());
      auto rangeStart = parseRangeField(fields[6], fullContentSize);
      auto rangeEnd = parseRangeField(fields[7], fullContentSize);

      // The client connection could be terminated early because of reasons
      // like slow client, user stops in the middle, etc.
      // This normally happens when client sends out request for very large
      // object without range request. Rectify such trace sample here.
      auto responseBodySize = responseSize - responseHeaderSize;
      if (!rangeStart.has_value() && responseBodySize < fullContentSize) {
        // No range request setting, but responseBodySize is smaller than
        // fullContentSize. convert the sample to range request
        rangeStart = 0;
        rangeEnd = responseBodySize - 1;
      }

      // Record the byte wise and object wise stats that we will egress
      if (!isPrepopulate()) {
        if (rangeStart) {
          size_t rangeSize = rangeEnd ? (*rangeEnd - *rangeStart + 1)
                                      : (fullContentSize - *rangeStart);
          stats_.getBytes.add(rangeSize + responseHeaderSize);
          stats_.getBodyBytes.add(rangeSize);
        } else {
          stats_.getBytes.add(fullContentSize + responseHeaderSize);
          stats_.getBodyBytes.add(fullContentSize);
        }
        stats_.objGets.inc();
        postpopulateSamples_.inc();
      } else {
        prepopulateSamples_.inc();
      }

      auto reqId = nextReqId_++;
      auto shard = getShard(reqId);
      LockHolder l(activeReqLock_[shard]);
      activeReqM_[shard].emplace(std::piecewise_construct,
                                 std::forward_as_tuple(reqId),
                                 std::forward_as_tuple(config_,
                                                       reqId,
                                                       fields[1],
                                                       fullContentSize,
                                                       responseHeaderSize,
                                                       rangeStart,
                                                       rangeEnd));
      return activeReqM_[shard].find(reqId)->second.req;
    } catch (const std::exception& e) {
      XLOG(ERR) << "Processing line: " << line
                << ", causes exception: " << e.what();
    }
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
