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

// physical grouping size (in Bytes) for contiguous pieces
constexpr uint64_t kCachePieceGroupSize = 16777216;

class PieceWiseCacheStats {
 public:
  struct InternalStats {
    // Byte wise stats:
    // getBytes: record both body and header bytes we fetch from cache and
    //           upstream;
    // getHitBytes: record both body and header bytes we fetch from cache;
    // getFullHitBytes: similar to getHitBytes, but only if all bytes for the
    //                  request are cache hits, i.e., excluding partial hits

    // getBodyBytes: similar to getBytes, but only for body bytes
    // getHitBodyBytes: similar to getHitBytes, but only for body bytes
    // getFullHitBodyBytes: similar to getFullHitBytes, but only for body bytes
    //
    // totalIngressBytes: record both body and header bytes we fetch from
    //                    upstream. Note we fetch both the header (which might
    //                    have been a hit) and the remaining part of missing
    //                    pieces
    // totalEgressBytes: record both body and header bytes we send out to
    //                   downstream. Note we trim the bytes fetched from cache
    //                   and/or upstream to for egress match request range
    //
    // For example, for range request of 0-50k (assuming 64k piece size), we
    // will fetch the first complete piece, but egress only 0-50k bytes; so
    // getBytes = 64k+header, getBodyBytes = 64k, totalEgressBytes = 50001
    AtomicCounter getBytes{0};
    AtomicCounter getHitBytes{0};
    AtomicCounter getFullHitBytes{0};
    AtomicCounter getBodyBytes{0};
    AtomicCounter getHitBodyBytes{0};
    AtomicCounter getFullHitBodyBytes{0};
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
      getBodyBytes.set(0);
      getHitBodyBytes.set(0);
      getFullHitBodyBytes.set(0);
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

  // @param numAggregationFields: # of aggregation fields in trace sample
  // @param statsPerAggField: list of values we track for each aggregation field
  // Example: numAggregationFields: 2; statsPerAggField: {0: [X, Y]; 1: [Z]}
  // for these inputs, we track the aggregated stats for samples that have value
  // X for aggregation field 0, stats for samples that have value Y for field
  // 0, and stats for samples that have value Z for field 1
  explicit PieceWiseCacheStats(
      uint32_t numAggregationFields,
      const std::unordered_map<uint32_t, std::vector<std::string>>&
          statsPerAggField);

  // Record both byte-wise and object-wise stats for a get request access
  void recordAccess(uint64_t timestamp,
                    size_t getBytes,
                    size_t getBodyBytes,
                    size_t bytesEgress,
                    const std::vector<std::string>& statsAggFields,
                    folly::Optional<bool> isHit);

  // Record cache hit for objects that are not stored in pieces
  void recordNonPieceHit(size_t hitBytes,
                         size_t hitBodyBytes,
                         const std::vector<std::string>& statsAggFields);

  // Record cache hit for a header piece
  void recordPieceHeaderHit(size_t pieceBytes,
                            const std::vector<std::string>& statsAggFields);

  // Record cache hit for a body piece
  void recordPieceBodyHit(size_t pieceBytes,
                          const std::vector<std::string>& statsAggFields);

  // Record full hit stats for piece wise caching
  void recordPieceFullHit(size_t headerBytes,
                          size_t bodyBytes,
                          const std::vector<std::string>& statsAggFields);

  // Record bytes ingress from upstream
  void recordIngressBytes(size_t ingressBytes,
                          const std::vector<std::string>& statsAggFields);

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

  // Collect breakdown stats for following map. The keys of these maps are added
  // in constructor and never changed afterwards, so no map structure change
  // after construction, and we can use const method from multi-threads without
  // lock
  //
  // extraStatsIndexM_: extra field index --> field value to track --> index
  // into extraStatsV_
  std::map<uint32_t, std::map<std::string, uint32_t>> extraStatsIndexM_;
  std::vector<InternalStats> extraStatsV_;

  // Latency stats
  mutable util::PercentileStats reqLatencyStats_;

  bool hasNvmCacheWarmedUp_{false};
  uint64_t nvmCacheWarmupTimestamp_{0};

  template <typename F, typename... Args>
  void recordStats(F& func,
                   const std::vector<std::string>& fields,
                   Args... args) {
    func(stats_, args...);
    func(lastWindowStats_, args...);
    if (hasNvmCacheWarmedUp_) {
      func(statsAfterWarmUp_, args...);
    }

    for (const auto& [fieldNum, statM] : extraStatsIndexM_) {
      XDCHECK_LT(fieldNum, fields.size());
      auto it = statM.find(fields[fieldNum]);
      if (it != statM.end()) {
        func(extraStatsV_[it->second], args...);
      }
    }
  }

  static void recordAccessInternal(InternalStats& stats,
                                   size_t getBytes,
                                   size_t getBodyBytes,
                                   size_t egressBytes);
  static void recordBenchmark(InternalStats& stats,
                              size_t getBytes,
                              size_t getBodyBytes,
                              size_t egressBytes,
                              bool isHit);
  static void recordNonPieceHitInternal(InternalStats& stats,
                                        size_t hitBytes,
                                        size_t hitBodyBytes);
  static void recordPieceHeaderHitInternal(InternalStats& stats,
                                           size_t pieceBytes);
  static void recordPieceBodyHitInternal(InternalStats& stats,
                                         size_t pieceBytes);
  static void recordPieceFullHitInternal(InternalStats& stats,
                                         size_t headerBytes,
                                         size_t bodyBytes);
  static void recordIngressBytesInternal(InternalStats& stats,
                                         size_t ingressBytes);
  static void renderStatsInternal(const InternalStats& stats,
                                  double elapsedSecs,
                                  std::ostream& out);
};

// For piecewise caching, an object is stored as multiple pieces (one header
// piece + several body pieces) if its size is larger than cachePieceSize;
// otherwise, it's stored as a single piece.
// Therefore, a raw request spawns into one or multiple piece requests against
// the cache.
// The class wraps the struct Request (which represents a single request against
// cache), and maintains the raw request (identified by baseKey) with its piece
// request (identified by pieceKey).
struct PieceWiseReqWrapper {
  const std::string baseKey;
  std::string pieceKey;
  std::vector<size_t> sizes;
  // Its internal key is a reference to pieceKey.
  // Immutable except the op field
  Request req;
  std::unique_ptr<GenericPieces> cachePieces;
  const RequestRange requestRange;
  // whether current pieceKey is header piece or body piece, mutable
  bool isHeaderPiece{false};
  // response header size
  const size_t headerSize;
  // The size of the complete object, excluding response header.
  const size_t fullObjectSize;
  // Additional stats aggregation fields for this request sample other
  // than the defined SampleFields
  const std::vector<std::string> statsAggFields;
  // Whether this trace was a hit. For the cases that we know whether a trace
  // was a hit, we use this to calculate the hit rate as a benchmark.
  const folly::Optional<bool> isHit;

  // Tracker to record the start/end of request. Initialize it at the
  // start of request processing.
  std::unique_ptr<util::LatencyTracker> latencyTracker_;

  // @param cachePieceSize: byte size of a cache piece
  // @param reqId: the unique request id for the raw request
  // @param key: base key for the raw request
  // @param fullContentSize: byte size of the full content/object
  // @param responseHeaderSize: response header size for the request
  // @param rangeStart: start of the range if it's range request
  // @param rangeEnd: end of the range if it's range request
  // @param ttl: ttl of the content for caching
  // @param statsAggFieldV: extra fields used for stats aggregation
  // @param admFeatureM: features map for admission policy
  // @param itemValue: client-specific data for the item

  explicit PieceWiseReqWrapper(
      uint64_t cachePieceSize,
      uint64_t timestamp,
      uint64_t reqId,
      OpType opType,
      folly::StringPiece key,
      size_t fullContentSize,
      size_t responseHeaderSize,
      folly::Optional<uint64_t> rangeStart,
      folly::Optional<uint64_t> rangeEnd,
      uint32_t ttl,
      std::vector<std::string>&& statsAggFieldV,
      std::unordered_map<std::string, std::string>&& admFeatureM,
      folly::Optional<bool> isHit,
      const std::string& itemValue);

  PieceWiseReqWrapper(const PieceWiseReqWrapper& other);
};

// The class adapts/updates the PieceWiseReqWrapper to its next operation and/or
// next piece based on the piecewise logic, and maintains PieceWiseCacheStats
// for all processings.
// For piecewise caching, we always fetch/store a complete piece from/to
// the cache, and then trim the fetched pieces accordingly for egress to match
// the request range.
class PieceWiseCacheAdapter {
 public:
  // @param maxCachePieces: maximum # of cache pieces we store for a single
  // content
  // @param numAggregationFields: # of aggregation fields in trace sample
  // @param statsPerAggField: list of values we track for each aggregation field
  explicit PieceWiseCacheAdapter(
      uint64_t maxCachePieces,
      uint32_t numAggregationFields,
      const std::unordered_map<uint32_t, std::vector<std::string>>&
          statsPerAggField)
      : stats_(numAggregationFields, statsPerAggField),
        maxCachePieces_(maxCachePieces) {}

  // Record corresponding stats for a new request.
  void recordNewReq(PieceWiseReqWrapper& rw);

  // Process the request rw, which updates rw to its next operation, and
  // record stats accordingly.
  // @return true if all ops for the request rw are done.
  bool processReq(PieceWiseReqWrapper& rw, OpResultType result);

  const PieceWiseCacheStats& getStats() const { return stats_; }

  void setNvmCacheWarmedUp(uint64_t timestamp) {
    stats_.setNvmCacheWarmedUp(timestamp);
  }

 private:
  // Called when rw is piecewise cached. The method updates rw to the next
  // operation and/or next piece based on piece logic, and record stats.
  // @return true if all the ops for all pieces behind the request are done.
  bool updatePieceProcessing(PieceWiseReqWrapper& rw, OpResultType result);

  // Called when rw is not piecewise cached. The method updates rw to the
  // next operation, and record the stats.
  // @return true if all ops for the request are done.
  bool updateNonPieceProcessing(PieceWiseReqWrapper& rw, OpResultType result);

  PieceWiseCacheStats stats_;

  const uint64_t maxCachePieces_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
