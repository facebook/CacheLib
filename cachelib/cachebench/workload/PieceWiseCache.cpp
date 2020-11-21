#include "cachelib/cachebench/workload/PieceWiseCache.h"

namespace {
constexpr double kGB = 1024.0 * 1024 * 1024;
}

namespace facebook {
namespace cachelib {
namespace cachebench {

PieceWiseCacheStats::PieceWiseCacheStats(
    uint32_t numAggregationFields,
    const std::unordered_map<uint32_t, std::vector<std::string>>&
        statsPerAggField) {
  // Doing a pre-allocation for extraStatsIndexM_ and extraStatsV_ here,
  // so we can use them afterwards without lock.
  // c++ guarantees thread safety for its const functions in the absence
  // of any non-const access.
  uint32_t extraStatsCount = 0;
  for (const auto& kv : statsPerAggField) {
    XCHECK_LT(kv.first, numAggregationFields);
    std::map<std::string, uint32_t> stat;
    for (const auto& fieldValue : kv.second) {
      stat[fieldValue] = extraStatsCount++;
    }
    extraStatsIndexM_[kv.first] = std::move(stat);
  }
  extraStatsV_ = std::vector<InternalStats>(extraStatsCount);
}

void PieceWiseCacheStats::recordAccess(
    size_t getBytes,
    size_t getBodyBytes,
    size_t egressBytes,
    const std::vector<std::string>& extraFields) {
  // TODO: support egress bytes counting for agg fields
  stats_.totalEgressBytes.add(egressBytes);

  recordStats(recordAccessInternal, extraFields, getBytes, getBodyBytes);
}

void PieceWiseCacheStats::recordAccessInternal(InternalStats& stats,
                                               size_t getBytes,
                                               size_t getBodyBytes) {
  stats.getBytes.add(getBytes);
  stats.getBodyBytes.add(getBodyBytes);
  stats.objGets.inc();
}

void PieceWiseCacheStats::recordNonPieceHit(
    size_t hitBytes,
    size_t hitBodyBytes,
    const std::vector<std::string>& extraFields) {
  recordStats(recordNonPieceHitInternal, extraFields, hitBytes, hitBodyBytes);
}

void PieceWiseCacheStats::recordNonPieceHitInternal(InternalStats& stats,
                                                    size_t hitBytes,
                                                    size_t hitBodyBytes) {
  stats.getHitBytes.add(hitBytes);
  stats.getFullHitBytes.add(hitBytes);
  stats.getHitBodyBytes.add(hitBodyBytes);
  stats.getFullHitBodyBytes.add(hitBodyBytes);
  stats.objGetHits.inc();
  stats.objGetFullHits.inc();
}

void PieceWiseCacheStats::recordPieceHeaderHit(
    size_t pieceBytes, const std::vector<std::string>& extraFields) {
  recordStats(recordPieceHeaderHitInternal, extraFields, pieceBytes);
}

void PieceWiseCacheStats::recordPieceHeaderHitInternal(InternalStats& stats,
                                                       size_t pieceBytes) {
  stats.getHitBytes.add(pieceBytes);
  stats.objGetHits.inc();
}

void PieceWiseCacheStats::recordPieceBodyHit(
    size_t pieceBytes, const std::vector<std::string>& extraFields) {
  recordStats(recordPieceBodyHitInternal, extraFields, pieceBytes);
}

void PieceWiseCacheStats::recordPieceBodyHitInternal(InternalStats& stats,
                                                     size_t pieceBytes) {
  stats.getHitBytes.add(pieceBytes);
  stats.getHitBodyBytes.add(pieceBytes);
}

void PieceWiseCacheStats::recordPieceFullHit(
    size_t headerBytes,
    size_t bodyBytes,
    const std::vector<std::string>& extraFields) {
  recordStats(recordPieceFullHitInternal, extraFields, headerBytes, bodyBytes);
}

void PieceWiseCacheStats::recordPieceFullHitInternal(InternalStats& stats,
                                                     size_t headerBytes,
                                                     size_t bodyBytes) {
  stats.getFullHitBytes.add(headerBytes + bodyBytes);
  stats.getFullHitBodyBytes.add(bodyBytes);
  stats.objGetFullHits.inc();
}

void PieceWiseCacheStats::recordBytesIngress(size_t bytesIngress) {
  stats_.totalIngressBytes.add(bytesIngress);
}

util::PercentileStats& PieceWiseCacheStats::getLatencyStatsObject() {
  return reqLatencyStats_;
}

void PieceWiseCacheStats::renderStats(uint64_t elapsedTimeNs,
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

void PieceWiseCacheStats::renderStatsInternal(const InternalStats& stats,
                                              double elapsedSecs,
                                              std::ostream& out) {
  out << folly::sformat("{:10}: {:.2f} million", "Total Processed Samples",
                        stats.objGets.get() / 1e6)
      << std::endl;

  auto safeDiv = [](auto nr, auto dr) {
    return dr == 0 ? 0.0 : 100.0 * nr / dr;
  };

  const double getBytesGB = stats.getBytes.get() / kGB;
  const double getBytesGBPerSec = getBytesGB / elapsedSecs;
  const double getBytesSuccessRate =
      safeDiv(stats.getHitBytes.get(), stats.getBytes.get());
  const double getBytesFullSuccessRate =
      safeDiv(stats.getFullHitBytes.get(), stats.getBytes.get());

  const double getBodyBytesGB = stats.getBodyBytes.get() / kGB;
  const double getBodyBytesGBPerSec = getBodyBytesGB / elapsedSecs;
  const double getBodyBytesSuccessRate =
      safeDiv(stats.getHitBodyBytes.get(), stats.getBodyBytes.get());
  const double getBodyBytesFullSuccessRate =
      safeDiv(stats.getFullHitBodyBytes.get(), stats.getBodyBytes.get());

  const uint64_t get = stats.objGets.get();
  const uint64_t getPerSec =
      util::narrow_cast<uint64_t>(stats.objGets.get() / elapsedSecs);
  const double getSuccessRate =
      safeDiv(stats.objGetHits.get(), stats.objGets.get());
  const double getFullSuccessRate =
      safeDiv(stats.objGetFullHits.get(), stats.objGets.get());

  const double egressBytesGB = stats.totalEgressBytes.get() / kGB;
  const double egressBytesGBPerSec = egressBytesGB / elapsedSecs;

  const double ingressBytesGB = stats.totalIngressBytes.get() / kGB;
  const double ingressBytesGBPerSec = ingressBytesGB / elapsedSecs;

  const double successRateByTotalTraffic =
      safeDiv(stats.totalEgressBytes.get() - stats.totalIngressBytes.get(),
              stats.totalEgressBytes.get());

  auto outFn = [&out](folly::StringPiece k0, double v0, folly::StringPiece k1,
                      double v1, folly::StringPiece k2, double v2,
                      folly::StringPiece k3, double v3) {
    out << folly::sformat(
               "{:12}: {:6.2f} GB, {:18}: {:6.2f} GB/s, {:8}: {:6.2f}%, {:10}: "
               "{:6.2f}%",
               k0, v0, k1, v1, k2, v2, k3, v3)
        << std::endl;
  };
  outFn("getBytes", getBytesGB, "getBytesPerSec", getBytesGBPerSec, "success",
        getBytesSuccessRate, "full success", getBytesFullSuccessRate);
  outFn("getBodyBytes", getBodyBytesGB, "getBodyBytesPerSec",
        getBodyBytesGBPerSec, "success", getBodyBytesSuccessRate,
        "full success", getBodyBytesFullSuccessRate);
  out << folly::sformat(
             "{:12}: {:6.2f} GB, {:12}: {:6.2f} GB, {:18}: {:6.2f} GB/s, "
             "{:18}: {:6.2f} GB/s, {:8}: {:6.2f}%",
             "egressBytes", egressBytesGB, "ingressBytes", ingressBytesGB,
             "egressBytesPerSec", egressBytesGBPerSec, "ingressBytesPerSec",
             ingressBytesGBPerSec, "success", successRateByTotalTraffic)
      << std::endl;
  out << folly::sformat(
             "{:12}: {:9,}, {:18}: {:8,} /s, {:8}: {:6.2f}%, {:10}: {:6.2f}%",
             "objectGet", get, "objectGetPerSec", getPerSec, "success",
             getSuccessRate, "full success", getFullSuccessRate)
      << std::endl;
}

PieceWiseReqWrapper::PieceWiseReqWrapper(uint64_t cachePieceSize,
                                         uint64_t reqId,
                                         OpType opType,
                                         folly::StringPiece key,
                                         size_t fullContentSize,
                                         size_t responseHeaderSize,
                                         folly::Optional<uint64_t> rangeStart,
                                         folly::Optional<uint64_t> rangeEnd,
                                         uint32_t ttl,
                                         std::vector<std::string>&& extraFieldV)
    : baseKey(GenericPieces::escapeCacheKey(key.str())),
      pieceKey(baseKey),
      sizes(1),
      req(pieceKey, sizes.begin(), sizes.end(), opType, ttl, reqId),
      requestRange(rangeStart, rangeEnd),
      headerSize(responseHeaderSize),
      fullObjectSize(fullContentSize),
      extraFields(extraFieldV) {
  if (fullContentSize < cachePieceSize) {
    // The entire object is stored along with the response header.
    // We always fetch the full content first, then trim the
    // response if it's range request
    sizes[0] = fullContentSize + responseHeaderSize;
  } else {
    // Piecewise caching
    cachePieces =
        std::make_unique<GenericPieces>(baseKey,
                                        cachePieceSize,
                                        kCachePieceGroupSize / cachePieceSize,
                                        fullContentSize,
                                        &requestRange);

    // Header piece is the first piece
    pieceKey = GenericPieces::createPieceHeaderKey(baseKey);
    sizes[0] = responseHeaderSize;
    isHeaderPiece = true;
  }
}

PieceWiseReqWrapper::PieceWiseReqWrapper(const PieceWiseReqWrapper& other)
    : baseKey(other.baseKey),
      pieceKey(other.pieceKey),
      sizes(other.sizes),
      req(pieceKey,
          sizes.begin(),
          sizes.end(),
          other.req.getOp(),
          other.req.ttlSecs,
          other.req.requestId.value()),
      requestRange(other.requestRange),
      isHeaderPiece(other.isHeaderPiece),
      headerSize(other.headerSize),
      fullObjectSize(other.fullObjectSize),
      extraFields(other.extraFields) {
  if (other.cachePieces) {
    cachePieces = std::make_unique<GenericPieces>(
        baseKey,
        other.cachePieces->getPieceSize(),
        kCachePieceGroupSize / other.cachePieces->getPieceSize(),
        fullObjectSize,
        &requestRange);
    cachePieces->setFetchIndex(other.cachePieces->getCurFetchingPieceIndex());
  }
}

void PieceWiseCacheAdapter::recordNewReq(PieceWiseReqWrapper& rw) {
  // Start tracking request latency
  rw.latencyTracker_ =
      std::make_unique<util::LatencyTracker>(stats_.getLatencyStatsObject());

  // Record the bytes that we are going to fetch, and to egress.
  // getBytes and getBodyBytes are what we will fetch from either cache or
  // upstream. egressBytes are what we will egress to client.
  size_t getBytes;
  size_t getBodyBytes;
  size_t egressBytes;

  // Calculate getBytes and getBodyBytes.
  // We always fetch complete piece or object regardless of range boundary
  if (rw.cachePieces) {
    // Fetch all relevant pieces, e.g., for range request of 5-150k, we
    // will fetch 3 pieces (assuming 64k piece): 0-64k, 64-128k, 128-192k
    getBodyBytes = rw.cachePieces->getTotalSize();
  } else {
    // We fetch the whole object no matter it's range request or not.
    getBodyBytes = rw.fullObjectSize;
  }
  getBytes = getBodyBytes + rw.headerSize;

  // Calculate egressBytes.
  if (rw.requestRange.getRequestRange()) {
    auto rangeStart = rw.requestRange.getRequestRange()->first;
    auto rangeEnd = rw.requestRange.getRequestRange()->second;
    auto rangeSize = rangeEnd ? (*rangeEnd - rangeStart + 1)
                              : (rw.fullObjectSize - rangeStart);

    egressBytes = rangeSize + rw.headerSize;
  } else {
    egressBytes = rw.fullObjectSize + rw.headerSize;
  }
  stats_.recordAccess(getBytes, getBodyBytes, egressBytes, rw.extraFields);
}

bool PieceWiseCacheAdapter::processReq(PieceWiseReqWrapper& rw,
                                       OpResultType result) {
  if (rw.cachePieces) {
    // Object is stored in pieces
    return updatePieceProcessing(rw, result);
  } else {
    // Object is not stored in pieces, and it should be stored along
    // with the response header
    return updateNonPieceProcessing(rw, result);
  }
}

bool PieceWiseCacheAdapter::updatePieceProcessing(PieceWiseReqWrapper& rw,
                                                  OpResultType result) {
  // we are only done if we have got everything.
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

    // For pieces that are beyond pieces number limit (maxCachePieces_),
    // we don't store them
    if (rw.cachePieces->isPieceWithinBound(nextPieceIndex) &&
        nextPieceIndex < maxCachePieces_) {
      // First set the correct key. Header piece has already been fetched,
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
      if (result == OpResultType::kGetHit) {
        if (!rw.cachePieces->isPieceWithinBound(nextPieceIndex)) {
          // We have got all the pieces that are requested, record the full
          // cache hit stats
          auto totalSize = rw.cachePieces->getTotalSize();
          stats_.recordPieceFullHit(rw.headerSize, totalSize, rw.extraFields);
        } else {
          // The remaining pieces are beyond maxCachePieces_, we don't store
          // them in cache and fetch them from upstream directly
          if (nextPieceIndex >= maxCachePieces_) {
            stats_.recordBytesIngress(rw.headerSize +
                                      rw.cachePieces->getRemainingBytes());
          }
        }
      }

      // we are done
      done = true;
    }
  } else if (result == OpResultType::kGetMiss) {
    // Record ingress bytes since we will fetch the bytes from upstream.
    size_t bytesIngress;
    if (rw.isHeaderPiece) {
      bytesIngress = rw.headerSize + rw.cachePieces->getRemainingBytes();
    } else {
      // Note we advance the piece index ahead of time, so
      // getCurFetchingPieceIndex() returns the next piece index
      auto missPieceIndex = rw.cachePieces->getCurFetchingPieceIndex() - 1;
      bytesIngress = rw.headerSize +
                     rw.cachePieces->getSizeOfAPiece(missPieceIndex) +
                     rw.cachePieces->getRemainingBytes();
    }
    stats_.recordBytesIngress(bytesIngress);

    // Perform set operation next for the current piece
    rw.req.setOp(OpType::kSet);
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
  }
  return done;
}

bool PieceWiseCacheAdapter::updateNonPieceProcessing(PieceWiseReqWrapper& rw,
                                                     OpResultType result) {
  // we are only done if we got everything.
  bool done = false;

  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure) {
    // Record the cache hit stats
    if (result == OpResultType::kGetHit) {
      size_t hitBytes = rw.sizes[0];
      size_t hitBodyBytes = rw.sizes[0] - rw.headerSize;
      stats_.recordNonPieceHit(hitBytes, hitBodyBytes, rw.extraFields);
    }

    // we are done
    done = true;
  } else if (result == OpResultType::kGetMiss) {
    // Record ingress bytes since we will fetch the bytes from upstream.
    stats_.recordBytesIngress(rw.sizes[0]);

    // Perform set operation next
    rw.req.setOp(OpType::kSet);
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
  }

  return done;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
