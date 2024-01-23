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

#include "cachelib/cachebench/workload/BlockChunkReplayGenerator.h"

#include "cachelib/cachebench/util/Exceptions.h"

namespace {
constexpr uint32_t kProducerConsumerWaitTimeUs = 100;
} // namespace

namespace facebook {
namespace cachelib {
namespace cachebench {

const Request& BlockChunkReplayGenerator::getReq(
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
    blockCacheAdapter_.recordNewReq(*reqWrapper);
  }

  return reqWrapper->req;
}

void BlockChunkReplayGenerator::notifyResult(uint64_t requestId,
                                             OpResultType result) {
  auto& activeReqQ = getTLReqQueue();
  auto& rw = *(activeReqQ.frontPtr());
  XCHECK_EQ(rw.req.requestId.value(), requestId);

  auto done = blockCacheAdapter_.processReq(rw, result);
  if (done) {
    activeReqQ.popFront();
  }
}

void BlockChunkReplayGenerator::getReqFromTrace() {
  std::string line;
  while (true) {
    try {
      traceStream_.getline(line);
    } catch (const cachelib::cachebench::EndOfTrace& e) {
      isEndOfFile_.store(true, std::memory_order_relaxed);
      break;
    }
    samples_.inc();

    if (!traceStream_.setNextLine(line)) {
      invalidSamples_.inc();
      continue;
    }

    // Convert timestamp to seconds.
    uint64_t timestampRaw = 0;
    auto timestampRawT =
        traceStream_.template getField<size_t>(SampleFields::OP_TIME);
    if (timestampRawT.hasValue()) {
      timestampRaw = timestampRawT.value();
    }
    // Set op
    auto opName = folly::to<std::string>(
        traceStream_.template getField<>(SampleFields::OP).value());
    OpType op = getOpType(opName);

    // Set key
    std::string blockId(
        traceStream_.template getField<>(SampleFields::BLOCK_ID).value());

    auto shardIdT =
        traceStream_.template getField<size_t>(SampleFields::SHARD_ID);
    if (shardIdT.hasValue()) {
      // The block ID (key) is encoded as <block id>_<shard id>.
      blockId = fmt::format("{}_{:03d}", blockId, shardIdT.value());
    }

    auto blockIdSizeT =
        traceStream_.template getField<size_t>(SampleFields::BLOCK_ID_SIZE);
    if (blockIdSizeT.hasValue()) {
      // Generate block ID whose size matches with that of the original one
      size_t blockIdSize =
          std::max<size_t>(blockIdSizeT.value(), blockId.size());
      // The block id should not exceed 256
      size_t newSize = std::min<size_t>(blockIdSize, 256);
      // If ampFactor_ > 1, the key will be extended with stream id of 5 bytes
      if (ampFactor_ > 1 && newSize > 5u) {
        newSize -= 5u;
      }
      if (newSize > blockId.size()) {
        blockId.resize(newSize, '0');
      }
    }

    auto ioOffset =
        traceStream_.template getField<size_t>(SampleFields::IO_OFFSET).value();
    auto ioSize =
        traceStream_.template getField<size_t>(SampleFields::IO_SIZE).value();

    if (ioOffset + ioSize > blockCacheAdapter_.getBlockSize()) {
      invalidSamples_.inc();
      continue;
    }

    // We have a valid sample
    // Bump the counter for the op type
    if (op != OpType::kGet) {
      nonGetSamples_.inc();
    }

    // Amplify the trace by the factor of ampFactor_
    for (size_t keySuffix = 0; keySuffix < ampFactor_; keySuffix++) {
      std::string key = blockId;
      if (ampFactor_ > 1) {
        // Extend the key with the stream id of 4 decimal chars
        key.append(folly::sformat("_{:04d}", keySuffix));
      }

      // Push the request to the queue of the worker identified by the shard
      auto shard = getShard(key);
      while (true) {
        if (shouldShutdown()) {
          XLOG(INFO) << "Forced to stop, terminate reading trace file!";
          return;
        }

        // Skip the shard if the stressor thread wants to leave
        if (threadFinished_[shard].load(std::memory_order_relaxed)) {
          XLOG_EVERY_MS(INFO, 100'000,
                        folly::sformat("Thread {} finish, skip", shard));
          break;
        }

        std::vector<std::string> statsAggFields;
        if (!activeReqQ_[shard]->isFull()) {
          auto status = activeReqQ_[shard]->write(blockCacheAdapter_,
                                                  timestampRaw,
                                                  nextReqId_,
                                                  op,
                                                  key,
                                                  ioOffset,
                                                  ioOffset + ioSize - 1);
          XCHECK(status);
          break;
        }

        // Spin until the queue has room
        queueProducerWaitCounts_.inc();
        std::this_thread::sleep_for(
            std::chrono::microseconds(kProducerConsumerWaitTimeUs));
      }
    }

    ++nextReqId_;
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
