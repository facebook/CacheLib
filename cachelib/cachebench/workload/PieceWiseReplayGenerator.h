#pragma once

#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/piecewise/GenericPieces.h"

#include <folly/SpinLock.h>

namespace facebook {
namespace cachelib {
namespace cachebench {

// physical grouping size (in Bytes) for contiguous pieces
constexpr uint64_t kCachePieceGroupSize = 16777216;

class PieceWiseReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit PieceWiseReplayGenerator(StressorConfig config)
      : ReplayGeneratorBase(config) {}

  virtual ~PieceWiseReplayGenerator() {
    XLOG(INFO) << "Summary count of samples in workload generator: "
               << "# of prepopulate samples: " << prepopulateSamples_.get()
               << ", # of postpopulate samples: " << postpopulateSamples_.get()
               << ", # of invalid samples: " << invalidSamples_.get();
  }

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  // timestamp, cacheKey, OpType, objectSize, responseSize, responseHeaderSize,
  // rangeStart, rangeEnd, TTL, samplingRate
  const Request& getReq(
      uint8_t,
      std::mt19937&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const override;

 private:
  struct ReqWrapper {
    const std::string baseKey;
    std::string pieceKey;                       // immutable
    std::vector<size_t> sizes;                  // mutable
    Request req;                                // immutable except the op field
    std::unique_ptr<GenericPieces> cachePieces; // mutable
    RequestRange requestRange;                  // immutable
    // whether current pieceKey is header piece or body piece, mutable
    bool isHeaderPiece;
    // response header size
    const size_t headerSize;

    /**
     * @param fullContentSize: byte size of the full content
     * @param responseHeaderSize: response header size for the request
     */
    explicit ReqWrapper(const StressorConfig& config,
                        uint64_t reqId,
                        folly::StringPiece key,
                        size_t fullContentSize,
                        size_t responseHeaderSize,
                        folly::Optional<uint64_t> rangeStart,
                        folly::Optional<uint64_t> rangeEnd)
        : baseKey(GenericPieces::escapeCacheKey(key.str())),
          pieceKey(baseKey),
          sizes(1),
          req(pieceKey,
              sizes.begin(),
              sizes.end(),
              OpType::kGet, // Only support get from trace for now
              reqId),
          requestRange(rangeStart, rangeEnd),
          headerSize(responseHeaderSize) {
      if (fullContentSize < config.cachePieceSize) {
        // The entire object is stored along with the response header.
        // We always fetch the full content first, then trim the
        // response if it's range request
        sizes[0] = fullContentSize + responseHeaderSize;
      } else {
        // Piecewise caching
        cachePieces = std::make_unique<GenericPieces>(
            baseKey,
            config.cachePieceSize,
            kCachePieceGroupSize / config.cachePieceSize,
            fullContentSize,
            &requestRange);

        // Header piece is the first piece
        pieceKey = GenericPieces::createPieceHeaderKey(baseKey);
        sizes[0] = responseHeaderSize;
        isHeaderPiece = true;
      }
    }
  };

  struct PieceWiseReplayGeneratorStats {
    // Byte wise stats: getBytes, getHitBytes and getFullHitBytes record the
    // bytes of the whole response (body + response header), while
    // getBodyBytes, getHitBodyBytes and getFullHitBodyBytes record the body
    // bytes only. getFullHitBytes and getFullHitBodyBytes only include cache
    // hits of the full object (ie, all pieces), while getHitBytes and
    // getHitBodyBytes includes partial hits as well.
    AtomicCounter getBytes{0};
    AtomicCounter getHitBytes{0};
    AtomicCounter getFullHitBytes{0};
    AtomicCounter getBodyBytes{0};
    AtomicCounter getHitBodyBytes{0};
    AtomicCounter getFullHitBodyBytes{0};

    // Object wise stats: for an object get, objGetFullHits is incremented when
    // all pieces are cache hits, while objGetHits is incremented for partial
    // hits as well.
    AtomicCounter objGets{0};
    AtomicCounter objGetHits{0};
    AtomicCounter objGetFullHits{0};
  };

  static size_t getShard(uint64_t requestId) { return requestId % kShards; }

  std::atomic<uint64_t> nextReqId_{1};

  using LockHolder = std::lock_guard<folly::SpinLock>;
  static constexpr size_t kShards = 1024;

  // Active requests that are in processing. Mapping from requestId to
  // ReqWrapper.
  std::unordered_map<uint64_t, ReqWrapper> activeReqM_[kShards];

  // Lock to activeReqM_
  mutable folly::SpinLock activeReqLock_[kShards];

  // mutex for reading from the trace file concurrently.
  mutable folly::SpinLock getLineLock_;

  AtomicCounter invalidSamples_{0};
  AtomicCounter prepopulateSamples_{0};
  AtomicCounter postpopulateSamples_{0};

  PieceWiseReplayGeneratorStats stats_;

  const Request& getReqFromTrace();

  // update the piece logic to the next operation and return true if the
  // entire sequence is done.
  bool updatePieceProcessing(ReqWrapper& req, OpResultType result);
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/PieceWiseReplayGenerator-inl.h"
