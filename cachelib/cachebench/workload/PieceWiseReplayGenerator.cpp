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
  auto partialFieldCount = SampleFields::TOTAL_DEFINED_FIELDS +
                           config_.replayGeneratorConfig.numAggregationFields;
  auto totalFieldCount =
      partialFieldCount + config_.replayGeneratorConfig.numExtraFields;
  while (true) {
    try {
      traceStream_.getline(line);
    } catch (const cachelib::cachebench::EndOfTrace& e) {
      isEndOfFile_.store(true, std::memory_order_relaxed);
      break;
    }
    samples_.inc();

    try {
      std::vector<folly::StringPiece> fields;
      folly::split(',', line, fields);

      // TODO: remove this after legacy data phased out.
      if (fields.size() > totalFieldCount ||
          fields.size() < totalFieldCount - 2) {
        invalidSamples_.inc();
        continue;
      }

      auto shard = getShard(fields[SampleFields::CACHE_KEY]);
      if (threadFinished_[shard].load(std::memory_order_relaxed)) {
        if (shouldShutdown()) {
          XLOG(INFO) << "Forced to stop, terminate reading trace file!";
          return;
        }

        XLOG_EVERY_MS(INFO, 100'000,
                      folly::sformat("Thread {} finish, skip", shard));
        continue;
      }

      auto fullContentSizeT =
          folly::tryTo<size_t>(fields[SampleFields::OBJECT_SIZE]);
      auto responseSizeT =
          folly::tryTo<size_t>(fields[SampleFields::RESPONSE_SIZE]);
      auto responseHeaderSizeT =
          folly::tryTo<size_t>(fields[SampleFields::RESPONSE_HEADER_SIZE]);
      auto ttlT = folly::tryTo<uint32_t>(fields[SampleFields::TTL]);
      // Invalid sample: cacheKey is empty, responseSize is not positive,
      // responseHeaderSize is not positive, or ttl is not positive.
      // objectSize can be zero for request handler like
      // interncache_everstore_metadata. Hence it is valid.
      if (!fields[1].compare("-") || !fields[1].compare("") ||
          !fullContentSizeT.hasValue() || !responseSizeT.hasValue() ||
          responseSizeT.value() == 0 || !responseHeaderSizeT.hasValue() ||
          responseHeaderSizeT.value() == 0 || !ttlT.hasValue() ||
          ttlT.value() == 0) {
        invalidSamples_.inc();
        continue;
      }

      // Convert timestamp to seconds.
      uint64_t timestampRaw =
          folly::tryTo<size_t>(fields[SampleFields::TIMESTAMP]).value();
      uint64_t timestampSeconds = timestampRaw / timestampFactor_;
      auto fullContentSize = fullContentSizeT.value();
      auto responseSize = responseSizeT.value();
      auto responseHeaderSize = responseHeaderSizeT.value();
      auto ttl = ttlT.value();

      if (responseSize < responseHeaderSize) {
        invalidSamples_.inc();
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
          if (responseBodySize == 0) {
            invalidSamples_.inc();
            continue;
          }
          rangeStart = 0;
          rangeEnd = responseBodySize - 1;
        }
      } else {
        // The sample is range request, but range size is larger than
        // responseBodySize. Rectify the range end.
        size_t rangeSize = rangeEnd ? (*rangeEnd - *rangeStart + 1)
                                    : (fullContentSize - *rangeStart);
        if (responseBodySize < rangeSize) {
          if (responseBodySize + *rangeStart == 0) {
            invalidSamples_.inc();
            continue;
          }
          rangeEnd = responseBodySize + *rangeStart - 1;
        }
      }

      size_t statsAggFieldStartIndex = SampleFields::TOTAL_DEFINED_FIELDS;
      size_t statsAggFieldEndIndex = partialFieldCount;
      // Parse expected cache result.
      folly::Optional<bool> cacheHit;
      if (fields.size() == totalFieldCount) {
        auto cacheHitT = folly::tryTo<int>(fields[SampleFields::CACHE_HIT]);
        if (cacheHitT.hasValue() &&
            (cacheHitT.value() == 0 || cacheHitT.value() == 1)) {
          cacheHit = cacheHitT.value();
        }
      } else {
        // We added cache hit field recently. Some data are still in the old
        // format.
        // TODO: remove this after legacy data saved in manifold phased out.
        XLOG_EVERY_MS(
            WARN, 100'000,
            folly::sformat("Expect {} but only have {} fields in trace. "
                           "Process it as no cache hit info field.",
                           totalFieldCount, fields.size()));
        --statsAggFieldStartIndex;
        --statsAggFieldEndIndex;
      }

      std::vector<std::string> statsAggFields;
      for (size_t i = statsAggFieldStartIndex; i < statsAggFieldEndIndex; ++i) {
        statsAggFields.push_back(fields[i].str());
      }

      // Admission policy related fields: feature name --> feature value
      std::unordered_map<std::string, std::string> admFeatureMap;
      if (config_.replayGeneratorConfig.mlAdmissionConfig) {
        for (const auto& [featureName, index] :
             config_.replayGeneratorConfig.mlAdmissionConfig->numericFeatures) {
          XCHECK_LT(index, totalFieldCount);
          admFeatureMap.emplace(featureName, fields[index].str());
        }
        for (const auto& [featureName, index] :
             config_.replayGeneratorConfig.mlAdmissionConfig
                 ->categoricalFeatures) {
          XCHECK_LT(index, totalFieldCount);
          admFeatureMap.emplace(featureName, fields[index].str());
        }
      }

      const std::string itemValue =
          fields.size() == totalFieldCount
              ? folly::to<std::string>(fields[SampleFields::ITEM_VALUE])
              : "";

      // Spin until the queue has room
      while (!activeReqQ_[shard]->write(config_.cachePieceSize,
                                        timestampSeconds,
                                        nextReqId_,
                                        OpType::kGet, // Only support get from
                                                      // trace for now
                                        fields[SampleFields::CACHE_KEY],
                                        fullContentSize,
                                        responseHeaderSize,
                                        rangeStart,
                                        rangeEnd,
                                        ttl,
                                        std::move(statsAggFields),
                                        std::move(admFeatureMap),
                                        cacheHit,
                                        itemValue)) {
        if (shouldShutdown()) {
          XLOG(INFO) << "Forced to stop, terminate reading trace file!";
          return;
        }

        queueProducerWaitCounts_.inc();
        std::this_thread::sleep_for(
            std::chrono::microseconds(kProducerConsumerWaitTimeUs));

        if (threadFinished_[shard].load(std::memory_order_relaxed)) {
          XLOG_EVERY_MS(INFO, 100'000,
                        folly::sformat("Thread {} finish, skip", shard));
          break;
        }
      }

      ++nextReqId_;
    } catch (const std::exception& e) {
      XLOG(ERR) << "Processing line: " << line
                << ", causes exception: " << e.what();
    }
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
