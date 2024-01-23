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

#include <string>

#include "cachelib/cachebench/workload/BlockChunkCache.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class BlockChunkReplayGenerator : public ReplayGeneratorBase {
 public:
  // The size of request queue for each stress worker thread
  static constexpr uint32_t kMaxRequestQueueSize = 1000;

  explicit BlockChunkReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        traceStream_(config, 0, columnTable_),
        blockCacheAdapter_(config.replayGeneratorConfig.blockSizeKB * 1024,
                           config.replayGeneratorConfig.chunkSizeKB * 1024),
        activeReqQ_(config.numThreads),
        threadFinished_(config.numThreads) {
    for (uint32_t i = 0; i < numShards_; ++i) {
      activeReqQ_[i] =
          std::make_unique<folly::ProducerConsumerQueue<BlockReqWrapper>>(
              kMaxRequestQueueSize);
      threadFinished_[i].store(false, std::memory_order_relaxed);
    }

    // Start a thread which fetches requests from the trace file
    traceGenThread_ = std::thread([this]() { getReqFromTrace(); });
  }

  virtual ~BlockChunkReplayGenerator() override {
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
    blockCacheAdapter_.setNvmCacheWarmedUp(timestamp);
  }

  void renderStats(uint64_t elapsedTimeNs, std::ostream& out) const override {
    blockCacheAdapter_.getStats().renderStats(elapsedTimeNs, out);
  }

  void renderStats(uint64_t elapsedTimeNs,
                   folly::UserCounters& counters) const override {
    blockCacheAdapter_.getStats().renderStats(elapsedTimeNs, counters);
  }

  void renderWindowStats(double elapsedSecs, std::ostream& out) const override {
    blockCacheAdapter_.getStats().renderWindowStats(elapsedSecs, out);
  }

  void markFinish() override {
    threadFinished_[*tlStickyIdx_].store(true, std::memory_order_relaxed);
  }

 private:
  void getReqFromTrace();

  OpType getOpType(const std::string& opName) {
    if (!opName.compare(0, 12, "getChunkData")) {
      return OpType::kGet;
    } else if (!opName.compare(0, 8, "putChunk")) {
      return OpType::kSet;
    }

    return OpType::kSize;
  }

  folly::ProducerConsumerQueue<BlockReqWrapper>& getTLReqQueue() {
    if (!tlStickyIdx_.get()) {
      tlStickyIdx_.reset(new uint32_t(incrementalIdx_++));
    }

    XCHECK_LT(*tlStickyIdx_, numShards_);
    return *activeReqQ_[*tlStickyIdx_];
  }

  // Line format for the trace file:
  enum SampleFields : uint8_t {
    OP_TIME = 0,
    BLOCK_ID,
    BLOCK_ID_SIZE,
    SHARD_ID,
    OP,
    OP_COUNT,
    IO_OFFSET,
    IO_SIZE,
    END
  };

  const ColumnTable columnTable_ = {
      {SampleFields::OP_TIME, false, {"op_time"}},
      {SampleFields::BLOCK_ID, true, {"block_id"}}, /* required */
      {SampleFields::BLOCK_ID_SIZE, false, {"block_id_size"}},
      {SampleFields::SHARD_ID, false, {"rs_shard_id"}},
      {SampleFields::OP, true, {"op_name"}}, /* required */
      {SampleFields::OP_COUNT, false, {"op_count"}},
      {SampleFields::IO_OFFSET, true, {"io_offset"}}, /* required */
      {SampleFields::IO_SIZE, true, {"io_size"}},     /* required */
  };

  TraceFileStream traceStream_;

  BlockChunkCacheAdapter blockCacheAdapter_;

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
  std::vector<std::unique_ptr<folly::ProducerConsumerQueue<BlockReqWrapper>>>
      activeReqQ_;

  // Thread that finish its operations mark it here, so we will skip
  // further request on its shard
  std::vector<std::atomic<bool>> threadFinished_;

  // The thread used to process trace file and generate workloads for each
  // activeReqQ_ queue.
  std::thread traceGenThread_;
  std::atomic<bool> isEndOfFile_{false};

  AtomicCounter queueProducerWaitCounts_{0};
  AtomicCounter queueConsumerWaitCounts_{0};

  AtomicCounter invalidSamples_{0};
  AtomicCounter nonGetSamples_{0};
  AtomicCounter samples_{0};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
