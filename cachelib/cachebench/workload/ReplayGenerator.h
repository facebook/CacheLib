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
#include <folly/logging/xlog.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// forward declaration
class ReplayGenerator;

struct ReqWrapper {
  // current outstanding key
  std::string key_;
  std::vector<size_t> sizes_{1};
  // current outstanding req object
  Request req_{key_, sizes_.begin(), sizes_.end(), OpType::kGet};

  // number of times to issue the current req object
  // before fetching a new line from the trace
  uint32_t repeats_{0};
};

class RequestStream {
 public:
  RequestStream(ReplayGenerator& parent,
                const StressorConfig& config,
                uint32_t threadIdx)
      : parent_(parent),
        threadIdx_(threadIdx),
        numThreads_(config.numThreads),
        traceStream_(config, threadIdx_) {}

  // getReq generates the next request from the given TraceStream
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  //      <key>,<op>,<size>,<op_count>,<key_size>[,<ttl>]
  //
  // Here, repeats gives a number of times to repeat the request specified on
  // this line before reading the next line of the file.
  const Request& getReq();

 private:
  ReplayGenerator& parent_;

  uint32_t threadIdx_ = 0;
  uint32_t numThreads_ = 1;

  ReqWrapper replayReq_;
  TraceFileStream traceStream_;
};

class ReplayGenerator : public ReplayGeneratorBase {
 public:
  // input format is: key,op,size,op_count,key_size,ttl
  enum SampleFields { KEY = 0, OP, SIZE, OP_COUNT, KEY_SIZE, TTL, END };

  explicit ReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config) {}

  virtual ~ReplayGenerator() {}

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  // fbid,OpType,size,repeats
  //
  // Here, repeats gives a number of times to repeat the request specified on
  // this line before reading the next line of the file.
  // TODO: not thread safe, can only work with single threaded stressor
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void renderStats(uint64_t, std::ostream& out) const override {
    out << std::endl << "== ReplayGenerator Stats ==" << std::endl;

    out << folly::sformat("{}: {:.2f} million (parse error: {})",
                          "Total Processed Samples",
                          (double)parseSuccess.load() / 1e6, parseError.load())
        << std::endl;
  }

  // Read next trace line from TraceFileStream and fill ReqWrapper
  void getReqInternal(TraceFileStream& traceStream, ReqWrapper& replayReq);

  // Parse the request from the trace line and set the ReqWrapper
  bool parseRequest(const std::string& line, ReqWrapper& req);

 private:
  // Used to assign thread id
  std::atomic<uint32_t> incrementalIdx_{0};

  class dummyRequestStreamTag {};
  folly::ThreadLocal<RequestStream, dummyRequestStreamTag> tlRequest{
      [&]() { return new RequestStream(*this, config_, incrementalIdx_++); }};

  // Stats
  std::atomic<uint64_t> parseError = 0;
  std::atomic<uint64_t> parseSuccess = 0;
};

inline const Request& RequestStream::getReq() {
  if (!replayReq_.repeats_) {
    parent_.getReqInternal(traceStream_, replayReq_);
    if (numThreads_ > 1) {
      // Replace the last 4 bytes with thread Id of 4 decimal chars. In doing
      // so, keep at least 10B from the key for uniqueness; 10B is the max
      // number of decimal digits for uint32_t which is used to encode the key
      if (replayReq_.key_.size() > 10) {
        // trunkcate the key
        size_t newSize = std::max<size_t>(replayReq_.key_.size() - 4, 10u);
        replayReq_.key_.resize(newSize, '0');
      }
      replayReq_.key_.append(folly::sformat("{:04d}", threadIdx_));
    }
  }

  replayReq_.repeats_--;
  return replayReq_.req_;
}

inline bool ReplayGenerator::parseRequest(const std::string& line,
                                          ReqWrapper& req) {
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
  req.key_ = fields[SampleFields::KEY];

  // Generate key whose size matches with that of the original one.
  size_t keySize = std::max<size_t>(keySizeField.value(), req.key_.size());
  // The key size should not exceed 256
  keySize = std::min<size_t>(keySize, 256);
  req.key_.resize(keySize, '0');

  // Set op
  const auto& op = fields[SampleFields::OP];
  // TODO only memcache optypes are supported
  if (!op.compare("GET")) {
    req.req_.setOp(OpType::kGet);
  } else if (!op.compare("SET")) {
    req.req_.setOp(OpType::kSet);
  } else if (!op.compare("DELETE")) {
    req.req_.setOp(OpType::kDel);
  } else {
    return false;
  }

  // Set size
  req.sizes_[0] = sizeField.value();

  // Set op_count
  req.repeats_ = opCountField.value();
  if (!req.repeats_) {
    return false;
  }
  if (config_.ignoreOpCount) {
    req.repeats_ = 1;
  }

  // Set TTL (optional)
  if (fields.size() > SampleFields::TTL) {
    auto ttlField = folly::tryTo<size_t>(fields[SampleFields::TTL]);
    req.req_.ttlSecs = ttlField.hasValue() ? ttlField.value() : 0;
  } else {
    req.req_.ttlSecs = 0;
  }

  return true;
}

inline void ReplayGenerator::getReqInternal(TraceFileStream& traceStream,
                                            ReqWrapper& req) {
  req.repeats_ = 0;
  while (req.repeats_ == 0) {
    std::string line;
    traceStream.getline(line); // can throw

    if (!parseRequest(line, req)) {
      parseError++;
    } else {
      parseSuccess++;
    }
  }
}

const Request& ReplayGenerator::getReq(uint8_t,
                                       std::mt19937_64&,
                                       std::optional<uint64_t>) {
  return tlRequest->getReq();
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
