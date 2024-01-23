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

#pragma once

#include <folly/Benchmark.h>
#include <folly/Optional.h>
#include <folly/Range.h>

#include <cstdint>
#include <mutex>

#include "cachelib/cachebench/util/Request.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/common/piecewise/GenericPieces.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// forward declaration
class BlockChunkCacheAdapter;

class BlockChunkCacheStats {
 public:
  struct InternalStats {
    // Byte wise stats:
    // getBytes: record bytes we fetch from cache and upstream;
    // getHitBytes: record bytes we fetch from cache;
    // getFullHitBytes: similar to getHitBytes, but only if all bytes for the
    //                  request are cache hits, i.e., excluding partial hits
    // totalIngressBytes: record bytes we fetch from upstream
    // totalEgressBytes: record bytes we send out to downstream.
    //                   Note we trim the bytes fetched from cache and/or
    //                   upstream to for egress match request range
    //
    // For example, for range request of 0-50k (assuming 64k piece size), we
    // will fetch the first complete piece, but egress only 0-50k bytes; so
    // getBytes = 64k, getBodyBytes = 64k, totalEgressBytes = 50001
    AtomicCounter getBytes{0};
    AtomicCounter getHitBytes{0};
    AtomicCounter getFullHitBytes{0};
    AtomicCounter totalIngressBytes{0};
    AtomicCounter totalEgressBytes{0};

    // Object wise stats: for an object get, objGetFullHits is incremented when
    // all pieces are cache hits, while objGetHits is incremented for partial
    // hits as well.
    AtomicCounter objGets{0};
    AtomicCounter objGetHits{0};
    AtomicCounter objGetFullHits{0};

    void reset() {
      getBytes.set(0);
      getHitBytes.set(0);
      getFullHitBytes.set(0);
      totalIngressBytes.set(0);
      totalEgressBytes.set(0);
      objGets.set(0);
      objGetHits.set(0);
      objGetFullHits.set(0);

      {
        std::lock_guard<std::mutex> lck(tsMutex_);
        startTimestamp_ = 0;
        endTimestamp_ = 0;
      }
    }

    void updateTimestamp(uint64_t timestamp);
    std::pair<uint64_t, uint64_t> getTimestamps();

   private:
    // The starting time and end time for counting the stats.
    std::mutex tsMutex_;
    uint64_t startTimestamp_{0};
    uint64_t endTimestamp_{0};
  };

  BlockChunkCacheStats() = default;

  // Record both byte-wise and object-wise stats for a get request access
  // isHit is used to record stats provided in the trace
  void recordAccess(uint64_t timestamp,
                    size_t getBytes,
                    size_t bytesEgress,
                    folly::Optional<bool> isHit);

  // Record cache hit for a body piece
  void recordPieceBodyHit(size_t pieceBytes);

  // Record stats for partial or full hit for the request access
  // @param fullHitBytes: the number of bytes for the full hit
  //                      0 if partial hit
  void recordRequestHit(size_t fullHitBytes);

  // Record bytes ingress from upstream
  void recordIngressBytes(size_t ingressBytes);

  util::PercentileStats& getLatencyStatsObject();

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const;

  void renderStats(uint64_t /* elapsedTimeNs */,
                   folly::UserCounters& counters) const;

  const InternalStats& getInternalStats() const { return stats_; }

  void renderWindowStats(double elapsedSecs, std::ostream& out) const;

  void setNvmCacheWarmedUp(uint64_t timestamp) {
    hasNvmCacheWarmedUp_ = true;
    nvmCacheWarmupTimestamp_ = timestamp;
  }

 private:
  // Overall hit rate stats
  InternalStats stats_;

  // Overall hit rate stats for benchmark. It is based on the hit status
  // provided in the traces.
  InternalStats statsBenchmark_;

  // Stats after cache has been warmed up
  InternalStats statsAfterWarmUp_;

  // The stats for the data since the last time we rendered.
  mutable InternalStats lastWindowStats_;

  // Latency stats
  mutable util::PercentileStats reqLatencyStats_;

  bool hasNvmCacheWarmedUp_{false};
  uint64_t nvmCacheWarmupTimestamp_{0};

  template <typename F, typename... Args>
  void recordStats(F& func, Args... args) {
    func(stats_, args...);
    func(lastWindowStats_, args...);
    if (hasNvmCacheWarmedUp_) {
      func(statsAfterWarmUp_, args...);
    }
  }

  static void recordAccessInternal(InternalStats& stats,
                                   size_t getBytes,
                                   size_t egressBytes);
  static void recordBenchmark(InternalStats& stats,
                              size_t getBytes,
                              size_t egressBytes,
                              bool isHit);
  static void recordPieceBodyHitInternal(InternalStats& stats,
                                         size_t pieceBytes);
  static void recordPieceFullHitInternal(InternalStats& stats,
                                         size_t bodyBytes);
  static void recordIngressBytesInternal(InternalStats& stats,
                                         size_t ingressBytes);
  static void renderStatsInternal(const InternalStats& stats,
                                  double elapsedSecs,
                                  std::ostream& out);
};

class ChunkPieces : public GenericPieces {
 public:
  /**
   * @param baseKey: base key of the request. we will generate piece key from
   * the base key.
   * @param chunkSize: byte size of each chunk used for caching
   * @param blockSize: size of the block
   * @param range: define the range of the request
   */
  ChunkPieces(const std::string& baseKey,
              uint64_t chunkSize,
              uint64_t blockSize,
              const RequestRange* range)
      : GenericPieces(baseKey, chunkSize, 0, blockSize, range) {}

  static std::string createPieceKey(const std::string& baseKey,
                                    size_t pieceNum) {
    return folly::to<std::string>(baseKey, "-", pieceNum);
  }
};

// For piecewise caching, a block is stored as multiple chunks all the time.
// Therefore, a raw request is split into one or multiple piece (chunk) requests
// against the cache. The class wraps the struct Request (which represents a
// single request against cache), and maintains the raw block request
// (identified by baseKey) with its piece request (identified by pieceKey).
struct BlockReqWrapper {
  const BlockChunkCacheAdapter& cache;
  const std::string baseKey;
  std::string pieceKey;
  std::vector<size_t> sizes;
  const OpType op;
  // Its internal key is a reference to pieceKey.
  // Immutable except the op field
  Request req;
  std::unique_ptr<ChunkPieces> cachePieces;
  const RequestRange requestRange;
  // Tracks the piece range missed in the cache for get request
  std::pair<int, int> missPieceRange{-1, -1};
  // Whether this trace was a hit. For the cases that we know whether a trace
  // was a hit, we use this to calculate the hit rate as a benchmark.
  const folly::Optional<bool> isHit;
  // Tracker to record the start/end of request. Initialize it at the
  // start of request processing.
  std::unique_ptr<util::LatencyTracker> latencyTracker_;

  // @param cache: the cache adapter instance
  // @param timestamp: the timestamp of the request
  // @param reqId: the unique request id for the raw request
  // @param opType: the operation type of the request
  // @param key: base key (i.e., block id + shard id) for the raw request
  // @param rangeStart: start of the range if it's range request
  // @param rangeEnd: end of the range if it's range request
  explicit BlockReqWrapper(BlockChunkCacheAdapter& cache,
                           uint64_t timestamp,
                           uint64_t reqId,
                           OpType opType,
                           folly::StringPiece key,
                           uint64_t rangeStart,
                           uint64_t rangeEnd);

  BlockReqWrapper(const BlockReqWrapper& other);

  BlockReqWrapper& operator=(const BlockReqWrapper& other) = delete;
};

// The class adapts/updates the BlockReqWrapper to its next operation and/or
// next piece based on the piecewise logic, and maintains BlockChunkCacheStats
// for all processings. For piecewise caching, we always fetch/store a complete
// piece from/to the cache, and then trim the fetched pieces accordingly for
// egress to match the request range.
class BlockChunkCacheAdapter {
 public:
  // @param blockSize : size of block (i.e., full object size)
  // @param chunkSize : size of each chunk
  explicit BlockChunkCacheAdapter(uint64_t blockSize, uint64_t chunkSize)
      : stats_(),
        blockSize_(blockSize),
        chunkSize_(chunkSize),
        maxCachePieces_(blockSize / chunkSize) {}

  // Record corresponding stats for a new request.
  void recordNewReq(BlockReqWrapper& rw);

  // Process the request rw, which updates rw to its next operation, and
  // record stats accordingly.
  // @return true if all ops for the request rw have been done
  bool processReq(BlockReqWrapper& rw, OpResultType result);

  const BlockChunkCacheStats& getStats() const { return stats_; }

  void setNvmCacheWarmedUp(uint64_t timestamp) {
    stats_.setNvmCacheWarmedUp(timestamp);
  }

  uint64_t getBlockSize() const { return blockSize_; }

 private:
  friend struct BlockReqWrapper;

  BlockChunkCacheStats stats_;

  const uint64_t blockSize_;
  const uint64_t chunkSize_;
  const uint64_t maxCachePieces_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
