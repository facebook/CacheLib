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
#include <folly/system/ThreadName.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

struct ReqWrapper {
  ReqWrapper() = default;

  ReqWrapper(const ReqWrapper& other)
      : key_(other.key_),
        sizes_(other.sizes_),
        req_(key_,
             sizes_.begin(),
             sizes_.end(),
             reinterpret_cast<uint64_t>(this),
             other.req_),
        repeats_(other.repeats_) {}

  // current outstanding key
  std::string key_;
  std::vector<size_t> sizes_{1};
  // current outstanding req object
  // Use 'this' as the request ID, so that this object can be
  // identified on completion (i.e., notifyResult call)
  Request req_{key_, sizes_.begin(), sizes_.end(), OpType::kGet,
               reinterpret_cast<uint64_t>(this)};

  // number of times to issue the current req object
  // before fetching a new line from the trace
  uint32_t repeats_{0};
};

// KVReplayGenerator generates the cachelib requests based the trace data
// read from the given trace file(s).
// KVReplayGenerator supports amplifying the key population by appending
// suffixes (i.e., stream ID) to each key read from the trace file.
// In order to minimize the contentions for the request submission queues
// which might need to be dispatched by multiple stressor threads,
// the requests are sharded to each stressor by doing hashing over the key.
class KVReplayGenerator : public ReplayGeneratorBase {
 public:
  // input format is: key,op,size,op_count,key_size,ttl
  enum SampleFields { KEY = 0, OP, SIZE, OP_COUNT, KEY_SIZE, TTL, END };

  explicit KVReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        ampFactor_(config.replayGeneratorConfig.ampFactor),
        traceStream_(config, 0) {
    for (uint32_t i = 0; i < numShards_; ++i) {
      stressorCtxs_.emplace_back(std::make_unique<StressorCtx>(i));
    }

    genWorker_ = std::thread([this] {
      folly::setThreadName("cb_replay_gen");
      genRequests();
    });

    XLOGF(INFO,
          "Started KVReplayGenerator (amp factor {}, # of stressor threads {})",
          ampFactor_, numShards_);
  }

  virtual ~KVReplayGenerator() {
    XCHECK(shouldShutdown());
    if (genWorker_.joinable()) {
      genWorker_.join();
    }
  }

  // getReq generates the next request from the trace file.
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void renderStats(uint64_t, std::ostream& out) const override {
    out << std::endl << "== KVReplayGenerator Stats ==" << std::endl;

    out << folly::sformat("{}: {:.2f} million (parse error: {})",
                          "Total Processed Samples",
                          (double)parseSuccess.load() / 1e6, parseError.load())
        << std::endl;
  }

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void markFinish() override { getStressorCtx().markFinish(); }

  // Parse the request from the trace line and set the ReqWrapper
  bool parseRequest(const std::string& line, std::unique_ptr<ReqWrapper>& req);

 private:
  // Interval at which the submission queue is polled when it is either
  // full (producer) or empty (consumer).
  // We use polling with the delay since the ProducerConsumerQueue does not
  // support blocking read or writes with a timeout
  static constexpr uint64_t checkIntervalUs_ = 100;
  static constexpr size_t kMaxRequests = 10000;

  using ReqQueue = folly::ProducerConsumerQueue<std::unique_ptr<ReqWrapper>>;

  // StressorCtx keeps track of the state including the submission queues
  // per stressor thread. Since there is only one request generator thread,
  // lock-free ProducerConsumerQueue is used for performance reason.
  // Also, separate queue which is dispatched ahead of any requests in the
  // submission queue is used for keeping track of the requests which need to be
  // resubmitted (i.e., a request having remaining repeat count); there could
  // be more than one requests outstanding for async stressor while only one
  // can be outstanding for sync stressor
  struct StressorCtx {
    explicit StressorCtx(uint32_t id)
        : id_(id), reqQueue_(std::in_place_t{}, kMaxRequests) {}

    bool isFinished() { return finished_.load(std::memory_order_relaxed); }
    void markFinish() { finished_.store(true, std::memory_order_relaxed); }

    uint32_t id_{0};
    std::queue<std::unique_ptr<ReqWrapper>> resubmitQueue_;
    folly::cacheline_aligned<ReqQueue> reqQueue_;
    // Thread that finish its operations mark it here, so we will skip
    // further request on its shard
    std::atomic<bool> finished_{false};
  };

  // Read next trace line from TraceFileStream and fill ReqWrapper
  std::unique_ptr<ReqWrapper> getReqInternal();

  // Used to assign stressorIdx_
  std::atomic<uint32_t> incrementalIdx_{0};

  // A sticky index assigned to each stressor threads that calls into
  // the generator.
  folly::ThreadLocalPtr<uint32_t> stressorIdx_;

  // Vector size is equal to the # of stressor threads;
  // stressorIdx_ is used to index.
  std::vector<std::unique_ptr<StressorCtx>> stressorCtxs_;

  // used to select stream in round-robin
  const size_t ampFactor_;

  std::atomic<size_t> nextStream_{0};
  TraceFileStream traceStream_;

  std::thread genWorker_;

  // Used to signal end of file as EndOfTrace exception
  std::atomic<bool> eof{false};

  // Stats
  std::atomic<uint64_t> parseError = 0;
  std::atomic<uint64_t> parseSuccess = 0;

  void genRequests();

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

inline bool KVReplayGenerator::parseRequest(const std::string& line,
                                            std::unique_ptr<ReqWrapper>& req) {
  // input format is: key,op,size,op_count,key_size,ttl
  std::vector<folly::StringPiece> fields;
  folly::split(",", line, fields);

  if (fields.size() <= SampleFields::KEY_SIZE) {
    return false;
  }

  auto keySizeField = folly::tryTo<size_t>(fields[SampleFields::KEY_SIZE]);
  auto sizeField = folly::tryTo<size_t>(fields[SampleFields::SIZE]);
  auto opCountField = folly::tryTo<uint32_t>(fields[SampleFields::OP_COUNT]);

  if (!keySizeField.hasValue() || !sizeField.hasValue() ||
      !opCountField.hasValue()) {
    return false;
  }

  // Set key
  req->key_ = fields[SampleFields::KEY];

  // Generate key whose size matches with that of the original one.
  size_t keySize = std::max<size_t>(keySizeField.value(), req->key_.size());
  // The key size should not exceed 256
  keySize = std::min<size_t>(keySize, 256);
  req->key_.resize(keySize, '0');

  // Set op
  const auto& op = fields[SampleFields::OP];
  // TODO implement GET_LEASE and SET_LEASE emulations
  if (!op.compare("GET") || !op.compare("GET_LEASE")) {
    req->req_.setOp(OpType::kGet);
  } else if (!op.compare("SET") || !op.compare("SET_LEASE")) {
    req->req_.setOp(OpType::kSet);
  } else if (!op.compare("DELETE")) {
    req->req_.setOp(OpType::kDel);
  } else {
    return false;
  }

  // Set size
  req->sizes_[0] = sizeField.value();

  // Set op_count
  req->repeats_ = opCountField.value();
  if (!req->repeats_) {
    return false;
  }
  if (config_.ignoreOpCount) {
    req->repeats_ = 1;
  }

  // Set TTL (optional)
  if (fields.size() > SampleFields::TTL) {
    auto ttlField = folly::tryTo<size_t>(fields[SampleFields::TTL]);
    req->req_.ttlSecs = ttlField.hasValue() ? ttlField.value() : 0;
  } else {
    req->req_.ttlSecs = 0;
  }

  return true;
}

inline std::unique_ptr<ReqWrapper> KVReplayGenerator::getReqInternal() {
  auto reqWrapper = std::make_unique<ReqWrapper>();
  do {
    std::string line;
    traceStream_.getline(line); // can throw

    if (!parseRequest(line, reqWrapper)) {
      parseError++;
    } else {
      parseSuccess++;
    }
  } while (reqWrapper->repeats_ == 0);

  return reqWrapper;
}

inline void KVReplayGenerator::genRequests() {
  while (!shouldShutdown()) {
    std::unique_ptr<ReqWrapper> reqWrapper;
    try {
      reqWrapper = getReqInternal();
    } catch (const EndOfTrace& e) {
      break;
    }

    for (size_t keySuffix = 0; keySuffix < ampFactor_; keySuffix++) {
      std::unique_ptr<ReqWrapper> req;
      // Use a copy of ReqWrapper except for the last one
      if (keySuffix == ampFactor_ - 1) {
        req.swap(reqWrapper);
      } else {
        req = std::make_unique<ReqWrapper>(*reqWrapper);
      }

      if (ampFactor_ > 1) {
        // Replace the last 4 bytes with thread Id of 4 decimal chars. In doing
        // so, keep at least 10B from the key for uniqueness; 10B is the max
        // number of decimal digits for uint32_t which is used to encode the key
        if (req->key_.size() > 10) {
          // trunkcate the key
          size_t newSize = std::max<size_t>(req->key_.size() - 4, 10u);
          req->key_.resize(newSize, '0');
        }
        req->key_.append(folly::sformat("{:04d}", keySuffix));
      }

      auto shardId = getShard(req->req_.key);
      auto& stressorCtx = getStressorCtx(shardId);
      auto& reqQ = *stressorCtx.reqQueue_;

      while (!reqQ.write(std::move(req)) && !stressorCtx.isFinished() &&
             !shouldShutdown()) {
        // ProducerConsumerQueue does not support blocking, so use sleep
        std::this_thread::sleep_for(
            std::chrono::microseconds{checkIntervalUs_});
      }
    }
  }

  setEOF();
}

const Request& KVReplayGenerator::getReq(uint8_t,
                                         std::mt19937_64&,
                                         std::optional<uint64_t>) {
  std::unique_ptr<ReqWrapper> reqWrapper;

  auto& stressorCtx = getStressorCtx();
  auto& reqQ = *stressorCtx.reqQueue_;
  auto& resubmitQueue = stressorCtx.resubmitQueue_;

  while (resubmitQueue.empty() && !reqQ.read(reqWrapper)) {
    if (resubmitQueue.empty() && isEOF()) {
      throw cachelib::cachebench::EndOfTrace("Test stopped or EOF reached");
    }
    // ProducerConsumerQueue does not support blocking, so use sleep
    std::this_thread::sleep_for(std::chrono::microseconds{checkIntervalUs_});
  }

  if (!reqWrapper) {
    XCHECK(!resubmitQueue.empty());
    reqWrapper.swap(resubmitQueue.front());
    resubmitQueue.pop();
  }

  ReqWrapper* reqPtr = reqWrapper.release();
  return reqPtr->req_;
}

void KVReplayGenerator::notifyResult(uint64_t requestId, OpResultType) {
  // requestId should point to the ReqWrapper object. The ownership is taken
  // here to do the clean-up properly if not resubmitted
  std::unique_ptr<ReqWrapper> reqWrapper(
      reinterpret_cast<ReqWrapper*>(requestId));
  XCHECK_GT(reqWrapper->repeats_, 0u);
  if (--reqWrapper->repeats_ == 0) {
    return;
  }
  // need to insert into the queue again
  getStressorCtx().resubmitQueue_.emplace(std::move(reqWrapper));
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
