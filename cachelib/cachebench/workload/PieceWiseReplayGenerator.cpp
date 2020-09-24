#include "folly/String.h"

#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/workload/PieceWiseReplayGenerator.h"

namespace {
constexpr uint32_t kProducerConsumerWaitTimeUs = 5;
} // namespace

namespace facebook {
namespace cachelib {
namespace cachebench {

void PieceWiseReplayGeneratorStats::recordAccess(
    size_t getBytes,
    size_t getBodyBytes,
    const std::vector<std::string>& extraFields) {
  recordStats(recordAccessInternal, extraFields, getBytes, getBodyBytes);
}

void PieceWiseReplayGeneratorStats::recordAccessInternal(InternalStats& stats,
                                                         size_t getBytes,
                                                         size_t getBodyBytes) {
  stats.getBytes.add(getBytes);
  stats.getBodyBytes.add(getBodyBytes);
  stats.objGets.inc();
}

void PieceWiseReplayGeneratorStats::recordNonPieceHit(
    size_t hitBytes,
    size_t hitBodyBytes,
    const std::vector<std::string>& extraFields) {
  recordStats(recordNonPieceHitInternal, extraFields, hitBytes, hitBodyBytes);
}

void PieceWiseReplayGeneratorStats::recordNonPieceHitInternal(
    InternalStats& stats, size_t hitBytes, size_t hitBodyBytes) {
  stats.getHitBytes.add(hitBytes);
  stats.getFullHitBytes.add(hitBytes);
  stats.getHitBodyBytes.add(hitBodyBytes);
  stats.getFullHitBodyBytes.add(hitBodyBytes);
  stats.objGetHits.inc();
  stats.objGetFullHits.inc();
}

void PieceWiseReplayGeneratorStats::recordPieceHeaderHit(
    size_t pieceBytes, const std::vector<std::string>& extraFields) {
  recordStats(recordPieceHeaderHitInternal, extraFields, pieceBytes);
}

void PieceWiseReplayGeneratorStats::recordPieceHeaderHitInternal(
    InternalStats& stats, size_t pieceBytes) {
  stats.getHitBytes.add(pieceBytes);
  stats.objGetHits.inc();
}

void PieceWiseReplayGeneratorStats::recordPieceBodyHit(
    size_t pieceBytes, const std::vector<std::string>& extraFields) {
  recordStats(recordPieceBodyHitInternal, extraFields, pieceBytes);
}

void PieceWiseReplayGeneratorStats::recordPieceBodyHitInternal(
    InternalStats& stats, size_t pieceBytes) {
  stats.getHitBytes.add(pieceBytes);
  stats.getHitBodyBytes.add(pieceBytes);
}

void PieceWiseReplayGeneratorStats::recordPieceFullHit(
    size_t headerBytes,
    size_t bodyBytes,
    const std::vector<std::string>& extraFields) {
  recordStats(recordPieceFullHitInternal, extraFields, headerBytes, bodyBytes);
}

void PieceWiseReplayGeneratorStats::recordPieceFullHitInternal(
    InternalStats& stats, size_t headerBytes, size_t bodyBytes) {
  stats.getFullHitBytes.add(headerBytes + bodyBytes);
  stats.getFullHitBodyBytes.add(bodyBytes);
  stats.objGetFullHits.inc();
}

util::LatencyTracker PieceWiseReplayGeneratorStats::makeLatencyTracker() {
  return util::LatencyTracker(reqLatencyStats_);
}

void PieceWiseReplayGeneratorStats::renderStats(uint64_t elapsedTimeNs,
                                                std::ostream& out) const {
  out << std::endl << "== PieceWiseReplayGenerator Stats ==" << std::endl;

  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  // Output the overall stats
  out << "= Overall stats =" << std::endl;
  renderStatsInternal(stats_, elapsedSecs, out);

  // request latency
  out << "= Request Latency =" << std::endl;
  folly::StringPiece latCat = "Total Request Latency";

  auto fmtLatency = [&](folly::StringPiece cat, folly::StringPiece pct,
                        uint64_t diffNanos) {
    double diffUs = static_cast<double>(diffNanos) / 1000.0;
    out << folly::sformat("{:20} {:8} : {:>10.2f} us\n", cat, pct, diffUs);
  };

  auto ret = reqLatencyStats_.estimate();

  fmtLatency(latCat, "avg", ret.avg);
  fmtLatency(latCat, "p50", ret.p50);
  fmtLatency(latCat, "p90", ret.p90);
  fmtLatency(latCat, "p99", ret.p99);
  fmtLatency(latCat, "p999", ret.p999);
  fmtLatency(latCat, "p9999", ret.p9999);
  fmtLatency(latCat, "p99999", ret.p99999);
  fmtLatency(latCat, "p999999", ret.p999999);
  fmtLatency(latCat, "p100", ret.p100);

  // Output stats broken down by extra field
  for (const auto& [fieldNum, fieldValues] : extraStatsIndexM_) {
    out << "= Breakdown stats for extra field " << fieldNum << " ="
        << std::endl;
    for (const auto& [fieldValue, fieldStatIdx] : fieldValues) {
      out << "Stats for field value " << fieldValue << ": " << std::endl;
      renderStatsInternal(extraStatsV_[fieldStatIdx], elapsedSecs, out);
    }
  }
}

void PieceWiseReplayGeneratorStats::renderStatsInternal(
    const InternalStats& stats, double elapsedSecs, std::ostream& out) {
  out << folly::sformat("{:10}: {:.2f} million", "Total Processed Samples",
                        stats.objGets.get() / 1e6)
      << std::endl;

  auto safeDiv = [](auto nr, auto dr) {
    return dr == 0 ? 0.0 : 100.0 * nr / dr;
  };

  const uint64_t getBytesPerSec =
      util::narrow_cast<uint64_t>(stats.getBytes.get() / 1024 / elapsedSecs);
  const double getBytesSuccessRate =
      safeDiv(stats.getHitBytes.get(), stats.getBytes.get());
  const double getBytesFullSuccessRate =
      safeDiv(stats.getFullHitBytes.get(), stats.getBytes.get());

  const uint64_t getBodyBytesPerSec = util::narrow_cast<uint64_t>(
      stats.getBodyBytes.get() / 1024 / elapsedSecs);
  const double getBodyBytesSuccessRate =
      safeDiv(stats.getHitBodyBytes.get(), stats.getBodyBytes.get());
  const double getBodyBytesFullSuccessRate =
      safeDiv(stats.getFullHitBodyBytes.get(), stats.getBodyBytes.get());

  const uint64_t getPerSec =
      util::narrow_cast<uint64_t>(stats.objGets.get() / elapsedSecs);
  const double getSuccessRate =
      safeDiv(stats.objGetHits.get(), stats.objGets.get());
  const double getFullSuccessRate =
      safeDiv(stats.objGetFullHits.get(), stats.objGets.get());

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

  // Record the byte wise and object wise stats that we will fetch
  // when it's a new request
  if (isNewReq) {
    // Start tracking request latency
    reqWrapper->latencyTracker_ = stats_.makeLatencyTracker();
    size_t getBytes;
    size_t getBodyBytes;
    if (reqWrapper->cachePieces) {
      // Fetch all relevant pieces, e.g., for range request of 5-150k, we
      // will fetch 3 pieces (assuming 64k piece): 0-64k, 64-128k, 128-192k
      getBodyBytes = reqWrapper->cachePieces->getTotalSize();
    } else {
      // We fetch the whole object no matter it's range request or not.
      getBodyBytes = reqWrapper->fullObjectSize;
    }
    getBytes = getBodyBytes + reqWrapper->headerSize;

    stats_.recordAccess(getBytes, getBodyBytes, reqWrapper->extraFields);
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
      size_t hitBytes = rw.sizes[0];
      size_t hitBodyBytes = rw.sizes[0] - rw.headerSize;
      stats_.recordNonPieceHit(hitBytes, hitBodyBytes, rw.extraFields);
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
        stats_.recordPieceHeaderHit(rw.sizes[0], rw.extraFields);
      } else {
        auto resultPieceIndex = nextPieceIndex - 1;
        // We always fetch a complete piece.
        auto pieceSize = rw.cachePieces->getSizeOfAPiece(resultPieceIndex);
        stats_.recordPieceBodyHit(pieceSize, rw.extraFields);
      }
    }

    // For pieces that are beyond pieces number limit (config_.maxCachePieces),
    // we don't store them
    if (rw.cachePieces->isPieceWithinBound(nextPieceIndex) &&
        nextPieceIndex < config_.maxCachePieces) {
      // first set the correct key. Header piece has already been fetched,
      // this is now a body piece.
      rw.pieceKey = GenericPieces::createPieceKey(
          rw.baseKey, nextPieceIndex, rw.cachePieces->getPiecesPerGroup());

      // Set the size of the piece
      rw.sizes[0] = rw.cachePieces->getSizeOfAPiece(nextPieceIndex);

      if (result == OpResultType::kGetHit) {
        // Fetch next piece
        rw.req.setOp(OpType::kGet);
      } else {
        // Once we start to set a piece, we set all subsequent pieces
        rw.req.setOp(OpType::kSet);
      }

      // Update the piece fetch index
      rw.isHeaderPiece = false;
      rw.cachePieces->updateFetchIndex();
    } else {
      // Record the cache hit stats: we got all the pieces that were requested
      // TODO: for pieces beyond config_.maxCachePieces, we still record as
      // full hit bytes, may need to change in the future.
      if (result == OpResultType::kGetHit) {
        auto totalSize = rw.cachePieces->getTotalSize();
        stats_.recordPieceFullHit(rw.headerSize, totalSize, rw.extraFields);
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
      folly::split(",", line, fields);
      if (fields.size() !=
          SampleFields::TOTAL_FIELDS +
              config_.replayGeneratorConfig.numAggregationFields) {
        invalidSamples_.inc();
        continue;
      }

      auto fullContentSizeT =
          folly::tryTo<size_t>(fields[SampleFields::OBJECT_SIZE]);
      auto responseSizeT =
          folly::tryTo<size_t>(fields[SampleFields::RESPONSE_SIZE]);
      auto responseHeaderSizeT =
          folly::tryTo<size_t>(fields[SampleFields::RESPONSE_HEADER_SIZE]);
      auto ttlT = folly::tryTo<uint32_t>(fields[SampleFields::TTL]);
      // Invalid sample: cacheKey is empty, objectSize is not positive,
      // responseSize is not positive, responseHeaderSize is not positive,
      // ttl is not positive
      if (!fields[1].compare("-") || !fields[1].compare("") ||
          !fullContentSizeT.hasValue() || fullContentSizeT.value() == 0 ||
          !responseSizeT.hasValue() || responseSizeT.value() == 0 ||
          !responseHeaderSizeT.hasValue() || responseHeaderSizeT.value() == 0 ||
          !ttlT.hasValue() || ttlT.value() == 0) {
        invalidSamples_.inc();
        continue;
      }

      auto fullContentSize = fullContentSizeT.value();
      auto responseSize = responseSizeT.value();
      auto responseHeaderSize = responseHeaderSizeT.value();
      auto ttl = ttlT.value();
      // When responseSize and responseHeaderSize is equal, responseBodySize
      // becomes 0 which can make range calculation incorrect. Simply ignore
      // such requests for now.
      // TODO: better handling non-GET requests
      if (responseSize == responseHeaderSize) {
        nonGetSamples_.inc();
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
      auto rangeStart =
          parseRangeField(fields[SampleFields::RANGE_START], fullContentSize);
      auto rangeEnd =
          parseRangeField(fields[SampleFields::RANGE_END], fullContentSize);

      // Perform range size check, and rectify the range when responseBodySize
      // is obviously too small.
      auto responseBodySize = responseSize - responseHeaderSize;
      if (!rangeStart.has_value()) {
        // No range request setting, but responseBodySize is smaller than
        // fullContentSize. Convert the sample to range request.
        if (responseBodySize < fullContentSize) {
          rangeStart = 0;
          rangeEnd = responseBodySize - 1;
        }
      } else {
        // The sample is range request, but range size is larger than
        // responseBodySize. Rectify the range end.
        size_t rangeSize = rangeEnd ? (*rangeEnd - *rangeStart + 1)
                                    : (fullContentSize - *rangeStart);
        if (responseBodySize < rangeSize) {
          rangeEnd = responseBodySize + *rangeStart - 1;
        }
      }

      std::vector<std::string> extraFields;
      for (size_t i = SampleFields::TOTAL_FIELDS; i < fields.size(); ++i) {
        extraFields.push_back(fields[i].str());
      }

      auto shard = getShard(fields[SampleFields::CACHE_KEY]);
      // Spin until the queue has room
      while (!activeReqQ_[shard]->write(config_,
                                        nextReqId_,
                                        fields[SampleFields::CACHE_KEY],
                                        fullContentSize,
                                        responseHeaderSize,
                                        rangeStart,
                                        rangeEnd,
                                        ttl,
                                        std::move(extraFields))) {
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
