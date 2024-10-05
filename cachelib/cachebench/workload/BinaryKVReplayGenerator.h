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

#include <folly/Conv.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/ThreadLocal.h>
#include <folly/lang/Aligned.h>
#include <folly/logging/xlog.h>
#include <folly/synchronization/Latch.h>
#include <folly/system/ThreadName.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// BinaryKVReplayGenerator generates the cachelib requests based on the
// requests read from the given binary trace file made with KVReplayGenerator
// In order to minimize the contentions for the request submission queues
// which might need to be dispatched by multiple stressor threads,
// the requests are sharded to each stressor by doing hashing over the key.
class BinaryKVReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit BinaryKVReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config), binaryStream_(config) {
    for (uint32_t i = 0; i < numShards_; ++i) {
      stressorCtxs_.emplace_back(
          std::make_unique<StressorCtx>(i, fastForwardCount_));
    }

    XLOGF(INFO,
          "Started BinaryKVReplayGenerator (# of stressor threads {}, total "
          "requests {})",
          numShards_, binaryStream_.getNumReqs());
  }

  virtual ~BinaryKVReplayGenerator() { XCHECK(shouldShutdown()); }

  // getReq generates the next request from the trace file.
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void renderStats(uint64_t, std::ostream& out) const override {
    out << std::endl << "== BinaryKVReplayGenerator Stats ==" << std::endl;

    out << folly::sformat("{}: {:.2f} million (parse error: {})",
                          "Total Processed Samples",
                          (double)parseSuccess.load() / 1e6, parseError.load())
        << std::endl;
  }

  void markFinish() override { getStressorCtx().markFinish(); }

 private:
  // per thread: the number of requests to run through
  // the trace before jumping to next offset
  static constexpr size_t kRunLength = 10000;
  static constexpr size_t kMinKeySize = 16;
  static constexpr size_t maxAmpFactor = 10000;

  // StressorCtx keeps track of the state including the submission queues
  // per stressor thread. Since there is only one request generator thread,
  // lock-free ProducerConsumerQueue is used for performance reason.
  // Also, separate queue which is dispatched ahead of any requests in the
  // submission queue is used for keeping track of the requests which need to be
  // resubmitted (i.e., a request having remaining repeat count); there could
  // be more than one requests outstanding for async stressor while only one
  // can be outstanding for sync stressor
  struct StressorCtx {
    explicit StressorCtx(uint32_t id, uint32_t fastForwardCount)
        : id_(id),
          reqIdx_(id * kRunLength + fastForwardCount),
          request_(std::string_view("abc"),
                   reinterpret_cast<size_t*>(0),
                   OpType::kGet,
                   0),
          runIdx_(0) {
      for (int i = 0; i < maxAmpFactor; i++) {
        suffixes.push_back(folly::sformat("{:05d}", i));
      }
    }

    bool isFinished() { return finished_.load(std::memory_order_relaxed); }
    void markFinish() { finished_.store(true, std::memory_order_relaxed); }
    void resetIdx() {
      runIdx_ = 0;
      reqIdx_ = id_ * kRunLength;
    }

    std::string_view updateKeyWithAmpFactor(std::string_view key) {
      if (ampFactor_ > 0) {
        // trunkcate the key so we don't overflow
        // max ampFactor is 10000, so we reserve at least 5 bytes
        size_t keySize = key.size() > kMinKeySize ?
                std::max<size_t>(key.size() - 5, kMinKeySize) :
                key.size();
        // copy the key into the currKey memory
        std::memcpy(currKey_, key.data(), keySize);
        std::memcpy(currKey_ + keySize, suffixes[ampFactor_].data(),
                    suffixes[ampFactor_].size());
        // add null terminating
        currKey_[keySize + suffixes[ampFactor_].size()] = '\0';
        ampFactor_--;
      } else {
        // copy the key into the currKey memory
        std::memcpy(currKey_, key.data(), key.size());
        // add null terminating
        currKey_[key.size()] = '\0';
      }
      return std::string_view(currKey_);
    }

    Request request_;
    uint64_t reqIdx_{0};
    uint64_t runIdx_{0};
    uint32_t id_{0};
    uint32_t ampFactor_{0};
    std::vector<std::string> suffixes;
    // space for the current key, with the
    // ampFactor suffix
    char currKey_[256];

    // Thread that finish its operations mark it here, so we will skip
    // further request on its shard
    std::atomic<bool> finished_{false};
  };

  // Used to assign stressorIdx_
  std::atomic<uint32_t> incrementalIdx_{0};

  // A sticky index assigned to each stressor threads that calls into
  // the generator.
  folly::ThreadLocalPtr<uint32_t> stressorIdx_;

  // Vector size is equal to the # of stressor threads;
  // stressorIdx_ is used to index.
  std::vector<std::unique_ptr<StressorCtx>> stressorCtxs_;

  // Class that holds a vector of pointers to the
  // binary data
  BinaryFileStream binaryStream_;

  // Used to signal end of file as EndOfTrace exception
  std::atomic<bool> eof{false};

  // Stats
  std::atomic<uint64_t> parseError = 0;
  std::atomic<uint64_t> parseSuccess = 0;

  void setEOF() { eof.store(true, std::memory_order_relaxed); }
  bool isEOF() { return eof.load(std::memory_order_relaxed); }

  inline StressorCtx& getStressorCtx(size_t shardId) {
    XCHECK_LT(shardId, numShards_);
    return *stressorCtxs_[shardId];
  }

  inline StressorCtx& getStressorCtx() {
    if (!stressorIdx_.get()) {
      stressorIdx_.reset(new uint32_t(incrementalIdx_++));
    }

    return getStressorCtx(*stressorIdx_);
  }
};

const Request& BinaryKVReplayGenerator::getReq(uint8_t,
                                               std::mt19937_64&,
                                               std::optional<uint64_t>) {
  auto& stressorCtx = getStressorCtx();
  auto& r = stressorCtx.request_;
  BinaryRequest* prevReq = reinterpret_cast<BinaryRequest*>(*(r.requestId));
  if (prevReq != nullptr && prevReq->repeats_ > 1) {
    prevReq->repeats_ = prevReq->repeats_ - 1;
  } else if (stressorCtx.ampFactor_ == 0) {
    BinaryRequest* req = nullptr;
    try {
      req = binaryStream_.getNextPtr(stressorCtx.reqIdx_ + stressorCtx.runIdx_);
      stressorCtx.ampFactor_ = ampFactor_;
      // update the binary request index
      if (stressorCtx.runIdx_ < kRunLength) {
        stressorCtx.runIdx_++;
      } else {
        stressorCtx.runIdx_ = 0;
        stressorCtx.reqIdx_ += numShards_ * kRunLength;
      }
    } catch (const EndOfTrace& e) {
      if (config_.repeatTraceReplay) {
        XLOGF_EVERY_MS(
            INFO, 60'000,
            "{} Reached the end of trace files. Restarting from beginning.",
            stressorCtx.id_);
        stressorCtx.resetIdx();
        stressorCtx.ampFactor_ = ampFactor_;
        req =
            binaryStream_.getNextPtr(stressorCtx.reqIdx_ + stressorCtx.runIdx_);
      } else {
        setEOF();
        throw cachelib::cachebench::EndOfTrace("Test stopped or EOF reached");
      }
    }

    XDCHECK_NE(req, nullptr);
    XDCHECK_LT(req->op_, 12);
    OpType op = static_cast<OpType>(req->op_);
    req->valueSize_ = (req->valueSize_) * ampSizeFactor_;
    r.update(stressorCtx.updateKeyWithAmpFactor(req->getKey()),
             const_cast<size_t*>(reinterpret_cast<size_t*>(&req->valueSize_)),
             op,
             req->ttl_,
             reinterpret_cast<uint64_t>(req));
  } else {
    r.updateKey(stressorCtx.updateKeyWithAmpFactor(prevReq->getKey()));
  }
  return r;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
