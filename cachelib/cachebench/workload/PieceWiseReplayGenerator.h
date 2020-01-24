#pragma once

#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr uint64_t kInvalidRequestId = 0;

class PieceWiseReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit PieceWiseReplayGenerator(StressorConfig config)
      : ReplayGeneratorBase(config) {
    // Insert an invalid request to be used when getReq() fails.
    activeReqM_.emplace(std::piecewise_construct,
                        std::forward_as_tuple(kInvalidRequestId),
                        std::forward_as_tuple("", "0", -1));
  }

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  // timestamp, cacheKey, OpType, size, TTL
  const Request& getReq(
      uint8_t,
      std::mt19937&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  OpType getOp(uint8_t,
               std::mt19937&,
               std::optional<uint64_t> requestId = std::nullopt) override;

  void notifyResult(uint64_t requestId, OpResultType result) override;

  template <typename CacheT>
  std::pair<size_t, std::chrono::seconds> prepopulateCache(CacheT& cache);

 private:
  struct ReqWrapper {
    std::string key;
    std::vector<size_t> sizes;
    Request req;
    OpType op;

    explicit ReqWrapper(folly::StringPiece k,
                        folly::StringPiece size,
                        int64_t reqId)
        : key(k.str()), sizes(1), req(key, sizes.begin(), sizes.end(), reqId) {
      sizes[0] = std::stoi(size.str());

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

  const Request& getReqFromTrace();
};

template <typename CacheT>
std::pair<size_t, std::chrono::seconds>
PieceWiseReplayGenerator::prepopulateCache(CacheT& cache) {
  size_t count(0);
  std::mt19937 gen(folly::Random::rand32());
  constexpr size_t batchSize = 1UL << 20;
  auto startTime = std::chrono::steady_clock::now();
  for (auto pid : cache.poolIds()) {
    while (cache.getPoolStats(pid).numEvictions() == 0) {
      for (size_t j = 0; j < batchSize; j++) {
        // we know using pool 0 is safe here, the trace generator doesn't use
        // this parameter
        const auto& req = getReq(0, gen);
        const auto allocHandle =
            cache.allocate(pid, req.key, req.key.size() + *(req.sizeBegin));
        if (allocHandle) {
          cache.insertOrReplace(allocHandle);
          // We throttle in case we are using flash so that we dont drop
          // evictions to flash by inserting at a very high rate.
          if (!cache.isRamOnly() && count % 8 == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
          }
          count++;
          notifyResult(*req.requestId, OpResultType::kGetHit);
        }
      }
    }
  }

  return std::make_pair(count,
                        std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now() - startTime));
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/PieceWiseReplayGenerator-inl.h"
