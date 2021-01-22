#include "cachelib/cachebench/workload/PieceWiseReplayGenerator.h"

#include "cachelib/cachebench/util/Exceptions.h"
#include "folly/String.h"

namespace {
constexpr uint32_t kProducerConsumerWaitTimeUs = 5;
} // namespace

namespace facebook {
namespace cachelib {
namespace cachebench {

const Request& PieceWiseReplayGenerator::getReq(
    uint8_t, std::mt19937_64&, std::optional<uint64_t> lastRequestId) {
  auto& activeReqQ = getTLReqQueue();

  // Spin until the queue has a value
  while (activeReqQ.isEmpty()) {
    if (isEndOfFile_.load(std::memory_order_relaxed) || shouldShutdown()) {
      throw cachelib::cachebench::EndOfTrace("");
    } else {
      // Wait a while to allow traceGenThread_ to process new samples.
      queueConsumerWaitCounts_.inc();
      std::this_thread::sleep_for(
          std::chrono::microseconds(kProducerConsumerWaitTimeUs));
    }
  }

  auto reqWrapper = activeReqQ.frontPtr();
  bool isNewReq = true;
  if (lastRequestId) {
    XCHECK_LE(*lastRequestId, reqWrapper->req.requestId.value());
    if (*lastRequestId == reqWrapper->req.requestId.value()) {
      isNewReq = false;
    }
  }

  // Record the byte wise and object wise stats that we will fetch
  // when it's a new request
  if (isNewReq) {
    pieceCacheAdapter_.recordNewReq(*reqWrapper);
  }

  return reqWrapper->req;
}

void PieceWiseReplayGenerator::notifyResult(uint64_t requestId,
                                            OpResultType result) {
  auto& activeReqQ = getTLReqQueue();
  auto& rw = *(activeReqQ.frontPtr());
  XCHECK_EQ(rw.req.requestId.value(), requestId);

  auto done = pieceCacheAdapter_.processReq(rw, result);
  if (done) {
    activeReqQ.popFront();
  }
}

void PieceWiseReplayGenerator::getReqFromTrace() {
  std::string line;
  while (true) {
    if (!std::getline(infile_, line)) {
      if (repeatTraceReplay_) {
        XLOG_EVERY_MS(
            INFO, 100'000,
            "Reached the end of trace file. Restarting from beginning.");
        resetTraceFileToBeginning();
        continue;
      }
      isEndOfFile_.store(true, std::memory_order_relaxed);
      break;
    }
    samples_.inc();

    try {
      std::vector<folly::StringPiece> fields;
      folly::split(",", line, fields);
      if (fields.size() !=
          SampleFields::TOTAL_FIELDS +
              config_.replayGeneratorConfig.numAggregationFields +
              config_.replayGeneratorConfig.numExtraFields) {
        invalidSamples_.inc();
        continue;
      }

      auto fullContentSizeT =
          folly::tryTo<size_t>(fields[SampleFields::OBJECT_SIZE]);
      auto responseSizeT =
          folly::tryTo<size_t>(fields[SampleFields::RESPONSE_SIZE]);
      auto responseHeaderSizeT =
          folly::tryTo<size_t>(fields[SampleFields::RESPONSE_HEADER_SIZE]);
      auto ttlT = folly::tryTo<uint32_t>(fields[SampleFields::TTL]);
      // Invalid sample: cacheKey is empty, objectSize is not positive,
      // responseSize is not positive, responseHeaderSize is not positive,
      // ttl is not positive
      if (!fields[1].compare("-") || !fields[1].compare("") ||
          !fullContentSizeT.hasValue() || fullContentSizeT.value() == 0 ||
          !responseSizeT.hasValue() || responseSizeT.value() == 0 ||
          !responseHeaderSizeT.hasValue() || responseHeaderSizeT.value() == 0 ||
          !ttlT.hasValue() || ttlT.value() == 0) {
        invalidSamples_.inc();
        continue;
      }

      auto fullContentSize = fullContentSizeT.value();
      auto responseSize = responseSizeT.value();
      auto responseHeaderSize = responseHeaderSizeT.value();
      auto ttl = ttlT.value();
      // When responseSize and responseHeaderSize is equal, responseBodySize
      // becomes 0 which can make range calculation incorrect. Simply ignore
      // such requests for now.
      // TODO: better handling non-GET requests
      if (responseSize == responseHeaderSize) {
        nonGetSamples_.inc();
        continue;
      }

      auto parseRangeField = [](folly::StringPiece range, size_t contentSize) {
        folly::Optional<uint64_t> result;
        // Negative value means it's not range request
        auto val = folly::to<int64_t>(range);
        if (val >= 0) {
          // range index can not be larger than content size
          result = std::min(static_cast<size_t>(val), contentSize - 1);
        } else {
          result = folly::none;
        }

        return result;
      };
      auto rangeStart =
          parseRangeField(fields[SampleFields::RANGE_START], fullContentSize);
      auto rangeEnd =
          parseRangeField(fields[SampleFields::RANGE_END], fullContentSize);

      // Perform range size check, and rectify the range when responseBodySize
      // is obviously too small.
      auto responseBodySize = responseSize - responseHeaderSize;
      if (!rangeStart.has_value()) {
        // No range request setting, but responseBodySize is smaller than
        // fullContentSize. Convert the sample to range request.
        if (responseBodySize < fullContentSize) {
          rangeStart = 0;
          rangeEnd = responseBodySize - 1;
        }
      } else {
        // The sample is range request, but range size is larger than
        // responseBodySize. Rectify the range end.
        size_t rangeSize = rangeEnd ? (*rangeEnd - *rangeStart + 1)
                                    : (fullContentSize - *rangeStart);
        if (responseBodySize < rangeSize) {
          rangeEnd = responseBodySize + *rangeStart - 1;
        }
      }

      std::vector<std::string> statsAggFields;
      for (size_t i = SampleFields::TOTAL_FIELDS;
           i < SampleFields::TOTAL_FIELDS +
                   config_.replayGeneratorConfig.numAggregationFields;
           ++i) {
        statsAggFields.push_back(fields[i].str());
      }

      auto shard = getShard(fields[SampleFields::CACHE_KEY]);
      // Spin until the queue has room
      while (!activeReqQ_[shard]->write(config_.cachePieceSize,
                                        nextReqId_,
                                        OpType::kGet, // Only support get from
                                                      // trace for now
                                        fields[SampleFields::CACHE_KEY],
                                        fullContentSize,
                                        responseHeaderSize,
                                        rangeStart,
                                        rangeEnd,
                                        ttl,
                                        std::move(statsAggFields))) {
        if (shouldShutdown()) {
          LOG(INFO) << "Forced to stop, terminate reading trace file!";
          return;
        }

        queueProducerWaitCounts_.inc();
        std::this_thread::sleep_for(
            std::chrono::microseconds(kProducerConsumerWaitTimeUs));
      }

      ++nextReqId_;
    } catch (const std::exception& e) {
      XLOG(ERR) << "Processing line: " << line
                << ", causes exception: " << e.what();
    }
  }
}

uint32_t PieceWiseReplayGenerator::getShard(folly::StringPiece key) {
  if (mode_ == ReplayGeneratorConfig::SerializeMode::strict) {
    return folly::hash::SpookyHashV2::Hash32(key.begin(), key.size(), 0) %
           numShards_;
  } else {
    // TODO: implement the relaxed mode
    return folly::Random::rand32(numShards_);
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
