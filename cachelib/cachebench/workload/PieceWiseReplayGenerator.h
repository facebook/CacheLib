#pragma once

#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"
#include "cachelib/common/piecewise/GenericPieces.h"

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
    XLOG(INFO) << "# of invalid samples: " << invalidSamples_;
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

 private:
  struct ReqWrapper {
    std::string baseKey;
    std::string pieceKey;
    std::vector<size_t> sizes;
    Request req;
    OpType op;
    std::unique_ptr<GenericPieces> cachePieces;
    RequestRange requestRange;

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
          requestRange(rangeStart, rangeEnd) {
      if (fullContentSize < config.cachePieceSize) {
        // Store the entire object along with the response header
        // TODO: deal with range request in this case, we need to trim the
        // response after fetching it.
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
      }

      // Only support get from trace for now
      op = OpType::kGet;
    }
  };

  std::atomic<uint64_t> nextReqId_{1};

  // Lock to protect file operation and activeReqM_
  std::mutex lock_;

  // Active requests that are in processing.
  // Mapping from requestId to ReqWrapper.
  std::unordered_map<uint64_t, ReqWrapper> activeReqM_;

  uint64_t invalidSamples_{0};

  const Request& getReqFromTrace();

  void updatePieceProcessing(
      std::unordered_map<uint64_t, ReqWrapper>::iterator it,
      OpResultType result);
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/PieceWiseReplayGenerator-inl.h"
