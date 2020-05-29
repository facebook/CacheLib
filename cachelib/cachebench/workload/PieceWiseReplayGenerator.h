#pragma once

#include <folly/ProducerConsumerQueue.h>
#include <folly/ThreadLocal.h>

#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/piecewise/GenericPieces.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// physical grouping size (in Bytes) for contiguous pieces
constexpr uint64_t kCachePieceGroupSize = 16777216;
constexpr uint32_t kMaxRequestQueueSize = 10000;

class PieceWiseReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit PieceWiseReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        mode_(config_.replayGeneratorConfig.getSerializationMode()),
        numShards_(config.numThreads),
        activeReqQ_(config.numThreads) {
    for (uint32_t i = 0; i < numShards_; ++i) {
      activeReqQ_[i] =
          std::make_unique<folly::ProducerConsumerQueue<ReqWrapper>>(
              kMaxRequestQueueSize);
    }

    traceGenThread_ = std::thread([this]() { getReqFromTrace(); });
  }

  virtual ~PieceWiseReplayGenerator() {
    traceGenThread_.join();

    XLOG(INFO) << "ProducerConsumerQueue Stats: producer waits: "
               << queueProducerWaitCounts_.get()
               << ", consumer waits: " << queueConsumerWaitCounts_.get();

    XLOG(INFO) << "Summary count of samples in workload generator: "
               << ", # of samples: " << samples_.get()
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
                        folly::Optional<uint64_t> rangeEnd,
                        uint32_t ttl)
        : baseKey(GenericPieces::escapeCacheKey(key.str())),
          pieceKey(baseKey),
          sizes(1),
          req(pieceKey,
              sizes.begin(),
              sizes.end(),
              OpType::kGet, // Only support get from trace for now
              ttl,
              reqId),
          requestRange(rangeStart, rangeEnd),
          headerSize(responseHeaderSize),
          fullObjectSize(fullContentSize) {
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

  const ReplayGeneratorConfig::SerializeMode mode_{
      ReplayGeneratorConfig::SerializeMode::strict};

  uint64_t nextReqId_{1};

  // # of shards is equal to the # of stressor threads
  const uint32_t numShards_;

  // Used to assign tlStickyIdx_
  std::atomic<uint32_t> incrementalIdx_{0};

  // A sticky index assigned to each stressor threads that calls into
  // the generator.
  folly::ThreadLocalPtr<uint32_t> tlStickyIdx_;

  // Request queues for each stressor threads, one queue per thread.
  // The first request in the queue is the active request in processing.
  // Vector size is equal to the # of stressor threads;
  // tlStickyIdx_ is used to index.
  std::vector<std::unique_ptr<folly::ProducerConsumerQueue<ReqWrapper>>>
      activeReqQ_;

  // The thread used to process trace file and generate workloads for each
  // activeReqQ_ queue.
  std::thread traceGenThread_;
  std::atomic<bool> isEndOfFile_{false};

  AtomicCounter queueProducerWaitCounts_{0};
  AtomicCounter queueConsumerWaitCounts_{0};

  AtomicCounter invalidSamples_{0};
  AtomicCounter samples_{0};

  PieceWiseReplayGeneratorStats stats_;

  void getReqFromTrace();

  // update the piece logic to the next operation and return true if the
  // entire sequence is done.
  bool updatePieceProcessing(ReqWrapper& req, OpResultType result);

  // Return the shard for the key.
  uint32_t getShard(folly::StringPiece key);

  folly::ProducerConsumerQueue<ReqWrapper>& getTLReqQueue() {
    if (!tlStickyIdx_.get()) {
      tlStickyIdx_.reset(new uint32_t(incrementalIdx_++));
    }

    XCHECK_LT(*tlStickyIdx_, numShards_);
    return *activeReqQ_[*tlStickyIdx_];
  }
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/PieceWiseReplayGenerator-inl.h"
