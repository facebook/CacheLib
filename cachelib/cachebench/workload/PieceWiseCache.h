#pragma once

#include <folly/Optional.h>
#include <folly/Range.h>

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
  void recordAccess(size_t getBytes,
                    size_t getBodyBytes,
                    size_t bytesEgress,
                    const std::vector<std::string>& extraFields);

  // Record cache hit for objects that are not stored in pieces
  void recordNonPieceHit(size_t hitBytes,
                         size_t hitBodyBytes,
                         const std::vector<std::string>& extraFields);

  // Record cache hit for a header piece
  void recordPieceHeaderHit(size_t pieceBytes,
                            const std::vector<std::string>& extraFields);

  // Record cache hit for a body piece
  void recordPieceBodyHit(size_t pieceBytes,
                          const std::vector<std::string>& extraFields);

  // Record full hit stats for piece wise caching
  void recordPieceFullHit(size_t headerBytes,
                          size_t bodyBytes,
                          const std::vector<std::string>& extraFields);

  // Record total bytes we ingress
  void recordBytesIngress(size_t bytesIngress);

  util::PercentileStats& getLatencyStatsObject();

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const;

 private:
  struct InternalStats {
    // Byte wise stats: getBytes, getHitBytes and getFullHitBytes record the
    // bytes of the whole response (body + response header), while
    // getBodyBytes, getHitBodyBytes and getFullHitBodyBytes record the body
    // bytes only. getFullHitBytes and getFullHitBodyBytes only include cache
    // hits of the full object (ie, all pieces), while getHitBytes and
    // getHitBodyBytes includes partial hits as well.
    // totalIngressBytes record all bytes we fetch from upstream, and
    // totalEgressBytes record all bytes we send out to downstream.
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
  };

  // Overall hit rate stats
  InternalStats stats_;

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

  template <typename F, typename... Args>
  void recordStats(F& func,
                   const std::vector<std::string>& fields,
                   Args... args) {
    func(stats_, args...);

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
                                   size_t getBodyBytes);
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

  static void renderStatsInternal(const InternalStats& stats,
                                  double elapsedSecs,
                                  std::ostream& out);
};

// For piecewise caching, a raw request spawns into one or multiple piece
// requests against the cache. The class wraps the struct Request (which
// represents a single request against cache), and maintains the raw request
// (identified by baseKey) with its piece request (identified by pieceKey).
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
  bool isHeaderPiece;
  // response header size
  const size_t headerSize;
  // The size of the complete object, excluding response header.
  const size_t fullObjectSize;
  // Extra fields for this request sample other than the SampleFields
  const std::vector<std::string> extraFields;

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
  // @param extraFieldV: Extra fields used for stats aggregation
  explicit PieceWiseReqWrapper(uint64_t cachePieceSize,
                               uint64_t reqId,
                               folly::StringPiece key,
                               size_t fullContentSize,
                               size_t responseHeaderSize,
                               folly::Optional<uint64_t> rangeStart,
                               folly::Optional<uint64_t> rangeEnd,
                               uint32_t ttl,
                               std::vector<std::string>&& extraFieldV);
};

// The class adapts/updates the PieceWiseReqWrapper to its next operation and/or
// next piece based on the piecewise logic, and maintains the
// PieceWiseCacheStats for all processings.
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

 private:
  // Called when rw is piecewise cached. The method updates rw to the next
  // operation and/or next piece based on piece logic, and record stats.
  // @return true if all the ops for all pieces behind the request are done.
  bool updatePieceProcessing(PieceWiseReqWrapper& rw, OpResultType result);

  // Called when rw is not piecewise cached. The method updates rw to the
  // next operation, and record the stats.
  // @return true if all ops for the request are done.
  bool updateNonPieceProcessing(PieceWiseReqWrapper& rw, OpResultType result);

  void recordIngressBytes(const PieceWiseReqWrapper& rw, OpResultType result);

  PieceWiseCacheStats stats_;

  const uint64_t maxCachePieces_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
