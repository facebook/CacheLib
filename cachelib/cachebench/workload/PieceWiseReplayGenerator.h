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

#include <folly/ProducerConsumerQueue.h>
#include <folly/ThreadLocal.h>

#include "cachelib/cachebench/workload/PieceWiseCache.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr uint32_t kMaxRequestQueueSize = 10000;

class PieceWiseReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit PieceWiseReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        traceStream_(config, 0),
        pieceCacheAdapter_(config.maxCachePieces,
                           config.replayGeneratorConfig.numAggregationFields,
                           config.replayGeneratorConfig.statsPerAggField),
        activeReqQ_(config.numThreads),
        threadFinished_(config.numThreads),
        timestampFactor_(config.timestampFactor) {
    for (uint32_t i = 0; i < numShards_; ++i) {
      activeReqQ_[i] =
          std::make_unique<folly::ProducerConsumerQueue<PieceWiseReqWrapper>>(
              kMaxRequestQueueSize);
      threadFinished_[i].store(false, std::memory_order_relaxed);
    }

    traceGenThread_ = std::thread([this]() { getReqFromTrace(); });
  }

  virtual ~PieceWiseReplayGenerator() {
    markShutdown();
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
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void setNvmCacheWarmedUp(uint64_t timestamp) override {
    pieceCacheAdapter_.setNvmCacheWarmedUp(timestamp);
  }

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const override {
    pieceCacheAdapter_.getStats().renderStats(elapsedTimeNs, out);
  }

  void renderStats(uint64_t elapsedTimeNs,
                   folly::UserCounters& counters) const override {
    pieceCacheAdapter_.getStats().renderStats(elapsedTimeNs, counters);
  }

  void renderWindowStats(double elapsedSecs, std::ostream& out) const override {
    pieceCacheAdapter_.getStats().renderWindowStats(elapsedSecs, out);
  }

  void markFinish() override {
    threadFinished_[*tlStickyIdx_].store(true, std::memory_order_relaxed);
  }

 private:
  void getReqFromTrace();

  folly::ProducerConsumerQueue<PieceWiseReqWrapper>& getTLReqQueue() {
    if (!tlStickyIdx_.get()) {
      tlStickyIdx_.reset(new uint32_t(incrementalIdx_++));
    }

    XCHECK_LT(*tlStickyIdx_, numShards_);
    return *activeReqQ_[*tlStickyIdx_];
  }

  // Line format for the trace file:
  // timestamp, cacheKey, OpType, objectSize, responseSize,
  // responseHeaderSize, rangeStart, rangeEnd, TTL, samplingRate, cacheHit
  // (extra fields might exist defined by
  // config_.replayGeneratorConfig.numAggregationFields and
  // config_.replayGeneratorConfig.numExtraFields)
  // cacheHit field is for the trace that we know it was a hit or miss. Use
  // 0 for miss and 1 for hit. Any other values will be ignored. When it is
  // specified with a valid value, we will calculate the expected hit rate based
  // on it.
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
    CACHE_HIT,
    ITEM_VALUE,
    TOTAL_DEFINED_FIELDS = 12
  };

  TraceFileStream traceStream_;

  PieceWiseCacheAdapter pieceCacheAdapter_;

  uint64_t nextReqId_{1};

  // Used to assign tlStickyIdx_
  std::atomic<uint32_t> incrementalIdx_{0};

  // A sticky index assigned to each stressor threads that calls into
  // the generator.
  folly::ThreadLocalPtr<uint32_t> tlStickyIdx_;

  // Request queues for each stressor threads, one queue per thread.
  // The first request in the queue is the active request in processing.
  // Vector size is equal to the # of stressor threads;
  // tlStickyIdx_ is used to index.
  std::vector<
      std::unique_ptr<folly::ProducerConsumerQueue<PieceWiseReqWrapper>>>
      activeReqQ_;

  // Thread that finish its operations mark it here, so we will skip
  // further request on its shard
  std::vector<std::atomic<bool>> threadFinished_;

  // The thread used to process trace file and generate workloads for each
  // activeReqQ_ queue.
  std::thread traceGenThread_;
  std::atomic<bool> isEndOfFile_{false};

  // The constant to be divided from the timestamp value
  // to turn the timestamp into seconds.
  const uint64_t timestampFactor_{1};

  AtomicCounter queueProducerWaitCounts_{0};
  AtomicCounter queueConsumerWaitCounts_{0};

  AtomicCounter invalidSamples_{0};
  AtomicCounter nonGetSamples_{0};
  AtomicCounter samples_{0};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
