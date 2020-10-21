#pragma once

#include <folly/ProducerConsumerQueue.h>
#include <folly/ThreadLocal.h>

#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/piecewise/GenericPieces.h"

#include "cachelib/common/PercentileStats.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// physical grouping size (in Bytes) for contiguous pieces
constexpr uint64_t kCachePieceGroupSize = 16777216;
constexpr uint32_t kMaxRequestQueueSize = 10000;

class PieceWiseReplayGeneratorStats {
 public:
  explicit PieceWiseReplayGeneratorStats(const StressorConfig& config) {
    // Doing a pre-allocation for extraStatsIndexM_ and extraStatsV_ here,
    // so we can use them afterwards without lock.
    // c++ guarantees thread safety for its const functions in the absence
    // of any non-const access.
    uint32_t extraStatsCount = 0;
    for (const auto& kv : config.replayGeneratorConfig.statsPerAggField) {
      XCHECK_LT(kv.first, config.replayGeneratorConfig.numAggregationFields);
      std::map<std::string, uint32_t> stat;
      for (const auto& fieldValue : kv.second) {
        stat[fieldValue] = extraStatsCount++;
      }
      extraStatsIndexM_[kv.first] = std::move(stat);
    }
    extraStatsV_ = std::vector<InternalStats>(extraStatsCount);
  }

  // Record both byte-wise and object-wise stats for a get request access
  void recordAccess(size_t getBytes,
                    size_t getBodyBytes,
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

  util::LatencyTracker makeLatencyTracker();

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const;

  // Record total bytes we ingress
  void recordBytesIngress(size_t bytesIngress);

  // Record total bytes we egress
  void recordBytesEgress(size_t bytesEgress);

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

class PieceWiseReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit PieceWiseReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        stats_(config),
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
               << "# of samples: " << samples_.get()
               << ", # of invalid samples: " << invalidSamples_.get()
               << ", # of non-get samples: " << nonGetSamples_.get()
               << ". Total invalid sample ratio: "
               << (double)(invalidSamples_.get() + nonGetSamples_.get()) /
                      samples_.get();
  }

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  const Request& getReq(
      uint8_t,
      std::mt19937&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const override {
    stats_.renderStats(elapsedTimeNs, out);
  }

 private:
  // Line format for the trace file:
  // timestamp, cacheKey, OpType, objectSize, responseSize,
  // responseHeaderSize, rangeStart, rangeEnd, TTL, samplingRate.
  // (extra fields might exist defined by
  // config_.replayGeneratorConfig.numAggregationFields)
  enum SampleFields {
    TIMESTAMP = 0,
    CACHE_KEY,
    OP_TYPE,
    OBJECT_SIZE,
    RESPONSE_SIZE,
    RESPONSE_HEADER_SIZE,
    RANGE_START,
    RANGE_END,
    TTL,
    SAMPLING_RATE,
    TOTAL_FIELDS = 10
  };

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
    // Extra fields for this request sample other than the SampleFields
    const std::vector<std::string> extraFields;
    // Tracker to record request level latency
    util::LatencyTracker latencyTracker_;

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
                        uint32_t ttl,
                        std::vector<std::string>&& extraFieldV)
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
          fullObjectSize(fullContentSize),
          extraFields(extraFieldV) {
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

  PieceWiseReplayGeneratorStats stats_;

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
  AtomicCounter nonGetSamples_{0};
  AtomicCounter samples_{0};

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
