#pragma once

#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"
#include "cachelib/common/piecewise/GenericPieces.h"

#include <folly/SpinLock.h>

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr uint64_t kInvalidRequestId = 0;
// physical grouping size (in Bytes) for contiguous pieces
constexpr uint64_t kCachePieceGroupSize = 16777216;

class PieceWiseReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit PieceWiseReplayGenerator(StressorConfig config)
      : ReplayGeneratorBase(config) {
    // Insert an invalid request to be used when getReq() fails.
    activeReqM_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(kInvalidRequestId),
        std::forward_as_tuple(
            config, kInvalidRequestId, "", 1, 1, folly::none, folly::none));
  }

  virtual ~PieceWiseReplayGenerator() {
    XLOG(INFO) << "Summary count of samples in workload generator: "
               << "# of prepopulate samples: " << prepopulateSamples_
               << ", # of postpopulate samples: " << postpopulateSamples_
               << ", # of invalid samples: " << invalidSamples_;
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

  OpType getOp(uint8_t,
               std::mt19937&,
               std::optional<uint64_t> requestId = std::nullopt) override;

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const override;

 private:
  struct ReqWrapper {
    std::string baseKey;
    std::string pieceKey;
    std::vector<size_t> sizes;
    Request req;
    OpType op;
    std::unique_ptr<GenericPieces> cachePieces;
    RequestRange requestRange;
    // whether current pieceKey is header piece or body piece
    bool isHeaderPiece;
    // response header size
    size_t headerSize;

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
          req(pieceKey, sizes.begin(), sizes.end(), reqId),
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

      // Only support get from trace for now
      op = OpType::kGet;
    }
  };

  struct PieceWiseReplayGeneratorStats {
    // Byte wise stats: getBytes, getHitBytes and getFullHitBytes record the
    // bytes of the whole response (body + response header), while
    // getBodyBytes, getHitBodyBytes and getFullHitBodyBytes record the body
    // bytes only. getFullHitBytes and getFullHitBodyBytes only include cache
    // hits of the full object (ie, all pieces), while getHitBytes and
    // getHitBodyBytes includes partial hits as well.
    double getBytes{0};
    double getHitBytes{0};
    double getFullHitBytes{0};
    double getBodyBytes{0};
    double getHitBodyBytes{0};
    double getFullHitBodyBytes{0};

    // Object wise stats: for an object get, objGetFullHits is incremented when
    // all pieces are cache hits, while objGetHits is incremented for partial
    // hits as well.
    uint64_t objGets{0};
    uint64_t objGetHits{0};
    uint64_t objGetFullHits{0};
  };

  std::atomic<uint64_t> nextReqId_{1};

  // Lock to protect file operation, activeReqM_, and stats_
  mutable folly::SpinLock lock_;
  using LockHolder = std::lock_guard<folly::SpinLock>;

  // Active requests that are in processing.
  // Mapping from requestId to ReqWrapper.
  std::unordered_map<uint64_t, ReqWrapper> activeReqM_;

  uint64_t invalidSamples_{0};
  uint64_t prepopulateSamples_{0};
  uint64_t postpopulateSamples_{0};

  PieceWiseReplayGeneratorStats stats_;

  const Request& getReqFromTrace();

  void updatePieceProcessing(
      std::unordered_map<uint64_t, ReqWrapper>::iterator it,
      OpResultType result);
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/PieceWiseReplayGenerator-inl.h"
