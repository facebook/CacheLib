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

#include "cachelib/cachebench/workload/BlockChunkCache.h"

namespace {
constexpr double kGB = 1024.0 * 1024 * 1024;
}

namespace facebook {
namespace cachelib {
namespace cachebench {

void BlockChunkCacheStats::InternalStats::updateTimestamp(uint64_t timestamp) {
  std::lock_guard<std::mutex> lck(tsMutex_);
  if (startTimestamp_ > timestamp || startTimestamp_ == 0) {
    startTimestamp_ = timestamp;
  }

  if (endTimestamp_ < timestamp) {
    endTimestamp_ = timestamp;
  }
}

std::pair<uint64_t, uint64_t>
BlockChunkCacheStats::InternalStats::getTimestamps() {
  std::pair<uint64_t, uint64_t> result;
  {
    std::lock_guard<std::mutex> lck(tsMutex_);
    result = {startTimestamp_, endTimestamp_};
  }
  return result;
}

void BlockChunkCacheStats::recordAccess(uint64_t timestamp,
                                        size_t getBytes,
                                        size_t egressBytes,
                                        folly::Optional<bool> isHit) {
  // Adjust the timestamp given current sample.
  stats_.updateTimestamp(timestamp);
  lastWindowStats_.updateTimestamp(timestamp);

  recordStats(recordAccessInternal, getBytes, egressBytes);
  if (isHit.hasValue()) {
    recordBenchmark(statsBenchmark_, getBytes, egressBytes, isHit.value());
  }
}

void BlockChunkCacheStats::recordAccessInternal(InternalStats& stats,
                                                size_t getBytes,
                                                size_t egressBytes) {
  stats.getBytes.add(getBytes);
  stats.objGets.inc();
  stats.totalEgressBytes.add(egressBytes);
}

void BlockChunkCacheStats::recordBenchmark(InternalStats& stats,
                                           size_t getBytes,
                                           size_t egressBytes,
                                           bool isHit) {
  stats.getBytes.add(getBytes);
  stats.objGets.inc();
  stats.totalEgressBytes.add(egressBytes);
  if (isHit) {
    stats.getHitBytes.add(getBytes);
    stats.getFullHitBytes.add(getBytes);
    stats.objGetHits.inc();
    stats.objGetFullHits.inc();
  }
}

void BlockChunkCacheStats::recordPieceBodyHit(size_t pieceBytes) {
  recordStats(recordPieceBodyHitInternal, pieceBytes);
}

void BlockChunkCacheStats::recordPieceBodyHitInternal(InternalStats& stats,
                                                      size_t pieceBytes) {
  stats.getHitBytes.add(pieceBytes);
}

void BlockChunkCacheStats::recordRequestHit(size_t fullHitBytes) {
  recordStats(recordPieceFullHitInternal, fullHitBytes);
}

void BlockChunkCacheStats::recordPieceFullHitInternal(InternalStats& stats,
                                                      size_t fullHitBytes) {
  stats.objGetHits.inc();
  if (fullHitBytes) {
    stats.getFullHitBytes.add(fullHitBytes);
    stats.objGetFullHits.inc();
  }
}

void BlockChunkCacheStats::recordIngressBytesInternal(InternalStats& stats,
                                                      size_t ingressBytes) {
  stats.totalIngressBytes.add(ingressBytes);
}

void BlockChunkCacheStats::recordIngressBytes(size_t ingressBytes) {
  recordStats(recordIngressBytesInternal, ingressBytes);
}

util::PercentileStats& BlockChunkCacheStats::getLatencyStatsObject() {
  return reqLatencyStats_;
}

void BlockChunkCacheStats::renderStats(uint64_t elapsedTimeNs,
                                       std::ostream& out) const {
  out << std::endl << "== BlockChunkReplayGenerator Stats ==" << std::endl;

  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  // Output the overall stats
  out << "= Overall stats =" << std::endl;
  renderStatsInternal(stats_, elapsedSecs, out);

  out << "= Benchmark stats =" << std::endl;
  renderStatsInternal(statsBenchmark_, elapsedSecs, out);

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

  if (hasNvmCacheWarmedUp_) {
    // TODO: convert seconds to human readable timestamp
    out << "= Overall stats after warmup [" << nvmCacheWarmupTimestamp_
        << "] =" << std::endl;
    renderStatsInternal(statsAfterWarmUp_, elapsedSecs, out);
  }
}

void BlockChunkCacheStats::renderStats(uint64_t /* elapsedTimeNs */,
                                       folly::UserCounters& counters) const {
  auto ret = reqLatencyStats_.estimate();
  counters["request_latency_p99"] = ret.p99;
}

void BlockChunkCacheStats::renderWindowStats(double elapsedSecs,
                                             std::ostream& out) const {
  auto windowTs = lastWindowStats_.getTimestamps();
  out << std::endl
      << "== BlockChunkReplayGenerator Stats in Recent Time Window ("
      << windowTs.first << " - " << windowTs.second << ") ==" << std::endl;

  renderStatsInternal(lastWindowStats_, elapsedSecs, out);

  lastWindowStats_.reset();
}

void BlockChunkCacheStats::renderStatsInternal(const InternalStats& stats,
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

  const double ingressEgressRatio =
      safeDiv(static_cast<int64_t>(stats.totalEgressBytes.get()) -
                  static_cast<int64_t>(stats.totalIngressBytes.get()),
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
  out << folly::sformat(
             "{:12}: {:6.2f} GB, {:12}: {:6.2f} GB, {:18}: {:6.2f} GB/s, "
             "{:18}: {:6.2f} GB/s, {:8}: {:6.2f}%",
             "egressBytes", egressBytesGB, "ingressBytes", ingressBytesGB,
             "egressBytesPerSec", egressBytesGBPerSec, "ingressBytesPerSec",
             ingressBytesGBPerSec, "ingressEgressratio", ingressEgressRatio)
      << std::endl;
  out << folly::sformat(
             "{:12}: {:10,}, {:18}: {:8,} /s, {:8}: {:6.2f}%, {:10}: {:6.2f}%",
             "objectGet", get, "objectGetPerSec", getPerSec, "success",
             getSuccessRate, "full success", getFullSuccessRate)
      << std::endl;
}

BlockReqWrapper::BlockReqWrapper(BlockChunkCacheAdapter& cache,
                                 uint64_t timestamp,
                                 uint64_t reqId,
                                 OpType opType,
                                 folly::StringPiece key,
                                 uint64_t rangeStart,
                                 uint64_t rangeEnd)
    : cache(cache),
      baseKey(key.str()),
      pieceKey(baseKey),
      sizes(1),
      op(opType),
      req(pieceKey, sizes.begin(), sizes.end(), opType, reqId),
      requestRange(rangeStart, rangeEnd) {
  req.timestamp = timestamp;

  cachePieces = std::make_unique<ChunkPieces>(baseKey, cache.chunkSize_,
                                              cache.blockSize_, &requestRange);

  pieceKey = ChunkPieces::createPieceKey(
      baseKey, cachePieces->getCurFetchingPieceIndex());

  sizes[0] =
      cachePieces->getSizeOfAPiece(cachePieces->getCurFetchingPieceIndex());
}

BlockReqWrapper::BlockReqWrapper(const BlockReqWrapper& other)
    : cache(other.cache),
      baseKey(other.baseKey),
      pieceKey(other.pieceKey),
      sizes(other.sizes),
      op(other.op),
      req(pieceKey,
          sizes.begin(),
          sizes.end(),
          other.req.getOp(),
          other.req.requestId.value()),
      requestRange(other.requestRange),
      isHit(other.isHit) {
  cachePieces = std::make_unique<ChunkPieces>(baseKey, cache.chunkSize_,
                                              cache.blockSize_, &requestRange);
  cachePieces->setFetchIndex(other.cachePieces->getCurFetchingPieceIndex());
}

void BlockChunkCacheAdapter::recordNewReq(BlockReqWrapper& rw) {
  // Start tracking request latency
  rw.latencyTracker_ =
      std::make_unique<util::LatencyTracker>(stats_.getLatencyStatsObject());

  // Record the bytes that we are going to fetch, and to egress.
  // getBytes and getBodyBytes are what we will fetch from either cache or
  // upstream. egressBytes are what we will egress to client.
  size_t getBytes;
  size_t egressBytes;

  // Calculate getBytes, i.e., the total bytes we fetch from cache or upstream.
  // We always fetch complete piece or block regardless of range boundary.
  // E.g., for range request of [5, 150k), we will fetch 3 pieces
  // (assuming 64k piece): [0, 64k), [64k, 128k), [128k, 192k)
  getBytes = rw.cachePieces->getTotalSize();

  // Calculate egressBytes.
  auto rangeStart = rw.requestRange.getRequestRange()->first;
  auto rangeEnd = rw.requestRange.getRequestRange()->second;
  auto rangeSize =
      rangeEnd ? (*rangeEnd - rangeStart + 1) : (blockSize_ - rangeStart);

  egressBytes = rangeSize;
  stats_.recordAccess(rw.req.timestamp, getBytes, egressBytes, rw.isHit);
}

bool BlockChunkCacheAdapter::processReq(BlockReqWrapper& rw,
                                        OpResultType result) {
  // We are only done if we have got everything.
  bool done = false;
  if (result == OpResultType::kGetHit || result == OpResultType::kSetSuccess ||
      result == OpResultType::kSetFailure || result == OpResultType::kSetSkip) {
    // Update the piece fetch index
    rw.cachePieces->updateFetchIndex();

    // The piece index we need to fetch next
    auto nextPieceIndex = rw.cachePieces->getCurFetchingPieceIndex();

    // Record the cache hit stats
    if (result == OpResultType::kGetHit) {
      auto resultPieceIndex = nextPieceIndex - 1;
      // We always fetch a complete piece.
      auto pieceSize = rw.cachePieces->getSizeOfAPiece(resultPieceIndex);
      stats_.recordPieceBodyHit(pieceSize);
    }

    // Check if we have got all the pieces we need to fetch
    if (rw.cachePieces->isPieceWithinBound(nextPieceIndex)) {
      XDCHECK_LT(nextPieceIndex, maxCachePieces_);
      // Reset op since it could have been changed to kSet
      rw.req.setOp(rw.op);
      // Update the piece key and the size
      rw.pieceKey = ChunkPieces::createPieceKey(rw.baseKey, nextPieceIndex);
      XDCHECK_EQ(rw.sizes.size(), 1u);
      rw.sizes[0] = rw.cachePieces->getSizeOfAPiece(nextPieceIndex);
    } else {
      // we are done
      done = true;
    }
  } else if (result == OpResultType::kGetMiss) {
    // We do the lookaside always, so perform set operation next
    // for the current piece
    rw.req.setOp(OpType::kSet);
    auto curIndex = rw.cachePieces->getCurFetchingPieceIndex() - 1;
    // Update the missing piece index accordingly
    if (rw.missPieceRange.first < 0) {
      rw.missPieceRange.first = curIndex;
    }
    rw.missPieceRange.second = curIndex;
  } else {
    XLOG(INFO) << "Unsupported OpResultType: " << (int)result;
    done = true;
  }

  if (done && rw.op == OpType::kGet) {
    // We have got all the pieces that are requested, record the full
    if (rw.missPieceRange.first < 0) {
      // No missing pieces, record full hit
      stats_.recordRequestHit(rw.cachePieces->getTotalSize());
    } else {
      // Record ingress bytes since we will fetch the bytes from upstream.
      // Note that we always fetch all the missing pieces as one operation,
      // meaning any hit pieces in the middle will also be fetched again
      size_t ingressPieces =
          rw.missPieceRange.second - rw.missPieceRange.first + 1;
      size_t totalPieces = rw.cachePieces->getEndPieceIndex() -
                           rw.cachePieces->getStartPieceIndex() + 1;
      XDCHECK_GT(ingressPieces, 0u);
      XDCHECK_LE(ingressPieces, maxCachePieces_);
      XDCHECK_GE(totalPieces, ingressPieces);
      if (totalPieces > ingressPieces) {
        // Record partial hit
        stats_.recordRequestHit(0);
      }
      stats_.recordIngressBytes(ingressPieces * chunkSize_);
    }
  }
  return done;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
