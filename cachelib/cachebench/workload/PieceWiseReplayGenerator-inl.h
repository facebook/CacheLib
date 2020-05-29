#include "folly/String.h"

#include "cachelib/cachebench/util/Exceptions.h"

namespace {
constexpr uint32_t kTraceNumFields = 10;
constexpr uint32_t kProducerConsumerWaitTimeUs = 5;
} // namespace

namespace facebook {
namespace cachelib {
namespace cachebench {

const Request& PieceWiseReplayGenerator::getReq(
    uint8_t, std::mt19937&, std::optional<uint64_t> lastRequestId) {
  auto& activeReqQ = getTLReqQueue();

  // Spin until the queue has a value
  while (activeReqQ.isEmpty()) {
    if (isEndOfFile_.load(std::memory_order_relaxed) || shouldShutdown()) {
      throw cachelib::cachebench::EndOfTrace("");
    } else {
      // Wait a while to allow traceGenThread_ to process new samples.
      queueConsumerWaitCounts_.inc();
      std::this_thread::sleep_for(
          std::chrono::microseconds(kProducerConsumerWaitTimeUs));
    }
  }

  auto reqWrapper = activeReqQ.frontPtr();
  bool isNewReq = true;
  if (lastRequestId) {
    XCHECK_LE(*lastRequestId, reqWrapper->req.requestId.value());
    if (*lastRequestId == reqWrapper->req.requestId.value()) {
      isNewReq = false;
    }
  }

  // Record the byte wise and object wise stats that we will egress
  // when it's a new request
  if (isNewReq) {
    if (reqWrapper->requestRange.getRequestRange()) {
      auto rangeStart = reqWrapper->requestRange.getRequestRange()->first;
      auto rangeEnd = reqWrapper->requestRange.getRequestRange()->second;
      size_t rangeSize = rangeEnd ? (*rangeEnd - rangeStart + 1)
                                  : (reqWrapper->fullObjectSize - rangeStart);
      stats_.getBytes.add(rangeSize + reqWrapper->headerSize);
      stats_.getBodyBytes.add(rangeSize);
    } else {
      stats_.getBytes.add(reqWrapper->fullObjectSize + reqWrapper->headerSize);
      stats_.getBodyBytes.add(reqWrapper->fullObjectSize);
    }
    stats_.objGets.inc();
  }

  return reqWrapper->req;
}

void PieceWiseReplayGenerator::notifyResult(uint64_t requestId,
                                            OpResultType result) {
  auto& activeReqQ = getTLReqQueue();
  auto& rw = *(activeReqQ.frontPtr());
  XCHECK_EQ(rw.req.requestId.value(), requestId);

  // Object is stored in pieces
  if (rw.cachePieces) {
    bool done = updatePieceProcessing(rw, result);
    if (done) {
      activeReqQ.popFront();
    }
    return;
  }

  // Now we know the object is not stored in pieces, and it should be stored
  // along with the response header
  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure) {
    // Record the cache hit stats
    if (result == OpResultType::kGetHit) {
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
    activeReqQ.popFront();
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
    if (result == OpResultType::kGetHit) {
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
      if (result == OpResultType::kGetHit) {
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

void PieceWiseReplayGenerator::getReqFromTrace() {
  std::string line;
  while (true) {
    if (!std::getline(infile_, line)) {
      if (repeatTraceReplay_) {
        XLOG_EVERY_MS(
            INFO, 100'000,
            "Reached the end of trace file. Restarting from beginning.");
        resetTraceFileToBeginning();
        continue;
      }
      isEndOfFile_.store(true, std::memory_order_relaxed);
      break;
    }
    samples_.inc();

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

      auto fullContentSizeT = folly::tryTo<size_t>(fields[3]);
      auto responseSizeT = folly::tryTo<size_t>(fields[4]);
      auto ttlT = folly::tryTo<uint32_t>(fields[8]);
      // Invalid sample: cacheKey is empty, objectSize is not positive,
      // responseSize is not positive, ttl is not positive
      if (!fields[1].compare("-") || !fields[1].compare("") ||
          !fullContentSizeT.hasValue() || fullContentSizeT.value() == 0 ||
          !responseSizeT.hasValue() || responseSizeT.value() == 0 ||
          !ttlT.hasValue() || ttlT.value() == 0) {
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

      auto fullContentSize = fullContentSizeT.value();
      auto responseSize = responseSizeT.value();
      auto ttl = ttlT.value();
      auto responseHeaderSize = folly::to<size_t>(fields[5]);
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

      auto shard = getShard(fields[1]);
      // Spin until the queue has room
      while (!activeReqQ_[shard]->write(config_,
                                        nextReqId_,
                                        fields[1],
                                        fullContentSize,
                                        responseHeaderSize,
                                        rangeStart,
                                        rangeEnd,
                                        ttl)) {
        if (shouldShutdown()) {
          LOG(INFO) << "Forced to stop, terminate reading trace file!";
          return;
        }

        queueProducerWaitCounts_.inc();
        std::this_thread::sleep_for(
            std::chrono::microseconds(kProducerConsumerWaitTimeUs));
      }

      ++nextReqId_;
    } catch (const std::exception& e) {
      XLOG(ERR) << "Processing line: " << line
                << ", causes exception: " << e.what();
    }
  }
}

uint32_t PieceWiseReplayGenerator::getShard(folly::StringPiece key) {
  if (mode_ == ReplayGeneratorConfig::SerializeMode::strict) {
    return folly::hash::SpookyHashV2::Hash32(key.begin(), key.size(), 0) %
           numShards_;
  } else {
    // TODO: implement the relaxed mode
    return folly::Random::rand32(numShards_);
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
