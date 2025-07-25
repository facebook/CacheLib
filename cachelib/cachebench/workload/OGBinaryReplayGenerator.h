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
#include "cachelib/cachebench/workload/ZstdReader.h"
#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

struct OGReqWrapper {
  OGReqWrapper() = default;

  OGReqWrapper(const OGReqWrapper& other)
      : key_(other.key_),
        sizes_(other.sizes_),
        req_(key_,
             sizes_.begin(),
             sizes_.end(),
             reinterpret_cast<uint64_t>(this),
             other.req_),
        repeats_(other.repeats_) {}
  
  void updateKey(const std::string& key) {
    key_ = key;
    // Request's key is now std::string_view
    req_.key = key_;
  }

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

// OGBinaryReplayGenerator generates the cachelib requests based the trace data
// read from the given trace file(s).
// OGBinaryReplayGenerator supports amplifying the key population by appending
// suffixes (i.e., stream ID) to each key read from the trace file.
// In order to minimize the contentions for the request submission queues
// which might need to be dispatched by multiple stressor threads,
// the requests are sharded to each stressor by doing hashing over the key.
class OGBinaryReplayGenerator : public ReplayGeneratorBase {
 public:
  // Default order is clock_time,object_id,object_size,next_access_vtime
  enum SampleFields : uint8_t {
    CLOCK_TIME = 0,
    OBJECT_ID,
    OBJECT_SIZE,
    NEXT_ACCESS_VTIME,
    END
  };

  const ColumnTable columnTable_ = {
      {SampleFields::CLOCK_TIME, true, {"clock_time"}},   /* required */
      {SampleFields::OBJECT_ID, true, {"object_id"}},     /* required */
      {SampleFields::OBJECT_SIZE, true, {"object_size"}}, /* required */
      {SampleFields::NEXT_ACCESS_VTIME, true, {"next_access_vtime"}}, /* required
                                                                       */
  };

  explicit OGBinaryReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config), traceStream_(config, 0, columnTable_), zstdReader_() {
    if(config.zstdTrace){
      zstdReader_.open(config.traceFileName, config.compressed);
      XLOGF(INFO, "Reading zstd trace file");
    }
    for (uint32_t i = 0; i < numShards_; ++i) {
      stressorCtxs_.emplace_back(std::make_unique<StressorCtx>(i));
    }

    genWorker_ = std::thread([this] {
      folly::setThreadName("cb_replay_gen");
      genRequests();
    });

    XLOGF(INFO,
          "Started OGBinaryReplayGenerator (amp factor {}, # of stressor "
          "threads {})",
          ampFactor_, numShards_);
  }

  virtual ~OGBinaryReplayGenerator() {
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
    out << std::endl << "== OGBinaryReplayGenerator Stats ==" << std::endl;

    out << folly::sformat("{}: {:.2f} million (parse error: {})",
                          "Total Processed Samples",
                          (double)parseSuccess.load() / 1e6, parseError.load())
        << std::endl;
  }

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void markFinish() override { getStressorCtx().markFinish(); }

  // Parse the request from the trace line and set the OGReqWrapper
  bool parseRequest(const std::string& line, std::unique_ptr<OGReqWrapper>& req);

  // for unit test
  bool setHeaderRow(const std::string& header) {
    return config_.zstdTrace ? true : traceStream_.setHeaderRow(header);
  }

 private:
  // Interval at which the submission queue is polled when it is either
  // full (producer) or empty (consumer).
  // We use polling with the delay since the ProducerConsumerQueue does not
  // support blocking read or writes with a timeout
  static constexpr uint64_t checkIntervalUs_ = 100;
  static constexpr size_t kMaxRequests = 10000;
  static constexpr size_t kMinKeySize = 16;
  static constexpr size_t maxSlabSize = 1ULL << facebook::cachelib::Slab::kNumSlabBits;

  using ReqQueue = folly::ProducerConsumerQueue<std::unique_ptr<OGReqWrapper>>;

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
    std::queue<std::unique_ptr<OGReqWrapper>> resubmitQueue_;
    folly::cacheline_aligned<ReqQueue> reqQueue_;
    // Thread that finish its operations mark it here, so we will skip
    // further request on its shard
    std::atomic<bool> finished_{false};
  };

  // Read next trace line from TraceFileStream and fill OGReqWrapper
  std::unique_ptr<OGReqWrapper> getReqInternal();

  std::unique_ptr<OGReqWrapper> getReqInternalZstd();

  // Used to assign stressorIdx_
  std::atomic<uint32_t> incrementalIdx_{0};

  // A sticky index assigned to each stressor threads that calls into
  // the generator.
  folly::ThreadLocalPtr<uint32_t> stressorIdx_;

  // Vector size is equal to the # of stressor threads;
  // stressorIdx_ is used to index.
  std::vector<std::unique_ptr<StressorCtx>> stressorCtxs_;

  TraceFileStream traceStream_;

  ZstdReader zstdReader_;

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

inline bool OGBinaryReplayGenerator::parseRequest(
  const std::string& line, std::unique_ptr<OGReqWrapper>& req) {
  if (!traceStream_.setNextLine(line)) {
    return false;
  }

  auto sizeField =
      traceStream_.template getField<size_t>(SampleFields::OBJECT_SIZE);
  if (!sizeField.hasValue()) {
    return false;
  }

  // Set key
  auto parsedKey = traceStream_.template getField<>(SampleFields::OBJECT_ID).value();
  req->updateKey(std::string{parsedKey});

  // Convert timestamp to seconds.
  // todo: clarify time precision
  auto timestampField =
      traceStream_.template getField<uint64_t>(SampleFields::CLOCK_TIME);
  if (timestampField.hasValue()) {
    uint64_t timestampRaw = timestampField.value();
    uint64_t timestampSeconds = timestampRaw / timestampFactor_;
    req->req_.timestamp = timestampSeconds;
  }

  size_t chunkSize = 1 * 1024 * 1024; // 1 MB
  size_t objSize = sizeField.value();

  if (objSize > chunkSize) {
    size_t numChunks = (objSize + chunkSize - 1) / chunkSize; 
    req->sizes_.clear(); 
    req->sizes_.reserve(numChunks); 

    for (size_t i = 0; i < numChunks; ++i) {
      size_t currentChunkSize = (i == numChunks - 1)
                                ? (objSize % chunkSize == 0 ? chunkSize : objSize % chunkSize)
                                : chunkSize;
      req->sizes_.push_back(currentChunkSize);
    }
    req->req_.setOp(OpType::kAddChained);
  } else {
    req->sizes_.clear(); 
    req->sizes_.resize(1); 
    req->sizes_[0] = objSize;
    req->req_.setOp(OpType::kGet);
  }
  req->req_.sizeBegin = req->sizes_.begin();
  req->req_.sizeEnd = req->sizes_.end();

  req->repeats_ = 1;
  if (!req->repeats_) {
    return false;
  }
  if (config_.ignoreOpCount) {
    req->repeats_ = 1;
  }

  return true;
}

inline std::unique_ptr<OGReqWrapper> OGBinaryReplayGenerator::getReqInternal() {
  auto reqWrapper = std::make_unique<OGReqWrapper>();

  do {
    std::string line;
    traceStream_.getline(line); // can throw

    if (!parseRequest(line, reqWrapper)) {
      parseError++;
      XLOG_N_PER_MS(ERR, 10, 1000) << folly::sformat(
          "Parsing error (total {}): {}", parseError.load(), line);
    } else {
      size_t totalSize = std::accumulate(
          reqWrapper->sizes_.begin(), reqWrapper->sizes_.end(), size_t(0));
      totalSize += reqWrapper->key_.length() + 32;

      if (config_.ignoreLargeReq && totalSize >= maxSlabSize) {
          return getReqInternal();
      }
      parseSuccess++;
    }
  } while (reqWrapper->repeats_ == 0);

  return reqWrapper;
}

inline std::unique_ptr<OGReqWrapper> OGBinaryReplayGenerator::getReqInternalZstd() {
  auto reqWrapper = std::make_unique<OGReqWrapper>();
  size_t chunkSize = 1 * 1024 * 1024; // 1 MB

  do {
    OracleGeneralBinRequest req;
    if (!zstdReader_.read_one_req(&req)) {
      throw EndOfTrace("EOF reached");
    }

    reqWrapper->key_ = std::to_string(req.objId);
    if (config_.ignoreLargeReq &&
      (req.objSize + reqWrapper->key_.length() + 32) >= maxSlabSize) {
      return getReqInternalZstd();
    }

    if (req.objSize > chunkSize) {
      size_t numChunks = (req.objSize + chunkSize - 1) / chunkSize; 
      reqWrapper->sizes_.clear(); 
      reqWrapper->sizes_.reserve(numChunks); 
      for (size_t i = 0; i < numChunks; ++i) {
        size_t currentChunkSize = (i == numChunks - 1) 
                                  ? (req.objSize % chunkSize == 0 ? chunkSize : req.objSize % chunkSize)
                                  : chunkSize;
        reqWrapper->sizes_.push_back(currentChunkSize);
      }

      reqWrapper->req_.setOp(OpType::kAddChained);
    } else {
      reqWrapper->sizes_.clear(); 
      reqWrapper->sizes_.resize(1); 
      reqWrapper->sizes_[0] = req.objSize; 
      reqWrapper->req_.setOp(OpType::kGet);
    }

    reqWrapper->req_.sizeBegin = reqWrapper->sizes_.begin();
    reqWrapper->req_.sizeEnd = reqWrapper->sizes_.end();

    reqWrapper->req_.timestamp = req.clockTime;
    reqWrapper->repeats_ = 1;
    parseSuccess++;
  } while (reqWrapper->repeats_ == 0);

  return reqWrapper;
}

inline void OGBinaryReplayGenerator::genRequests() {
  while (!shouldShutdown()) {
    std::unique_ptr<OGReqWrapper> reqWrapper;
    try {
      if(config_.zstdTrace){
        reqWrapper = getReqInternalZstd();
      } else {
        reqWrapper = getReqInternal();
      }
    } catch (const EndOfTrace&) {
      break;
    }

    for (size_t keySuffix = 0; keySuffix < ampFactor_; keySuffix++) {
      std::unique_ptr<OGReqWrapper> req;
      // Use a copy of ReqWrapper except for the last one
      if (keySuffix == ampFactor_ - 1) {
        req.swap(reqWrapper);
      } else {
        req = std::make_unique<OGReqWrapper>(*reqWrapper);
      }

      if (ampFactor_ > 1) {
        // Replace the last 4 bytes with thread Id of 4 decimal chars. In doing
        // so, keep at least 10B from the key for uniqueness; 10B is the max
        // number of decimal digits for uint32_t which is used to encode the key
        if (req->key_.size() > kMinKeySize) {
          // trunkcate the key
          size_t newSize = std::max<size_t>(req->key_.size() - 4, kMinKeySize);
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

const Request& OGBinaryReplayGenerator::getReq(uint8_t,
                                               std::mt19937_64&,
                                               std::optional<uint64_t>) {
  std::unique_ptr<OGReqWrapper> reqWrapper;

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

  OGReqWrapper* reqPtr = reqWrapper.release();
  return reqPtr->req_;
}

void OGBinaryReplayGenerator::notifyResult(uint64_t requestId, OpResultType) {
  // requestId should point to the OGReqWrapper object. The ownership is taken
  // here to do the clean-up properly if not resubmitted
  std::unique_ptr<OGReqWrapper> reqWrapper(
      reinterpret_cast<OGReqWrapper*>(requestId));
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
