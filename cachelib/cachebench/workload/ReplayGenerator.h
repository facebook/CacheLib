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

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

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

  // Read next trace from infile and fill ReqWrapper
  void getReqInternal(std::ifstream& infile, ReqWrapper& replayReq);

 private:
  ReqWrapper replayReq_;

  // Stats
  std::atomic<uint64_t> parseError = 0;
  std::atomic<uint64_t> parseSuccess = 0;

  bool parseRequest(const std::string& line, ReqWrapper& req);
};

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
  // In doing so, reserve four bytes at the end for some generator
  // including AmplifiedReplayGenerator
  size_t keySize = std::max<size_t>(keySizeField.value(), req.key_.size() + 4);
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

  // Set TTL (optional)
  if (fields.size() > SampleFields::TTL) {
    auto ttlField = folly::tryTo<size_t>(fields[SampleFields::TTL]);
    req.req_.ttlSecs = ttlField.hasValue() ? ttlField.value() : 0;
  } else {
    req.req_.ttlSecs = 0;
  }

  return true;
}

inline void ReplayGenerator::getReqInternal(std::ifstream& infile,
                                            ReqWrapper& req) {
  req.repeats_ = 0;
  while (req.repeats_ == 0) {
    std::string line;
    // Get key
    if (!std::getline(infile, line)) {
      throw cachelib::cachebench::EndOfTrace("");
    }

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
  if (!replayReq_.repeats_) {
    getReqInternal(infile_, replayReq_);
  }

  replayReq_.repeats_--;
  return replayReq_.req_;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
