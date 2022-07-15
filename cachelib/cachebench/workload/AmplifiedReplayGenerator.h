/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/logging/xlog.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

struct RequestStream {
  static constexpr uint32_t INVALID_THREAD_ID = static_cast<uint32_t>(-1);

  inline bool isInitialized() { return threadIdx_ != INVALID_THREAD_ID; }

  void resetTraceFileToBeginning() {
    infile_.clear();
    infile_.seekg(0, std::ios::beg);
    // header
    std::string row;
    std::getline(infile_, row);
  }

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  // fbid,OpType,size,repeats
  //
  // Here, repeats gives a number of times to repeat the request specified on
  // this line before reading the next line of the file.
  const Request& getReq(uint8_t,
                        std::mt19937_64&,
                        std::optional<uint64_t> lastRequestId = std::nullopt);
  // ifstream pointing to the trace file
  std::ifstream infile_;
  char infileBuffer_[kIfstreamBufferSize];
  uint32_t threadIdx_{INVALID_THREAD_ID};

  // current outstanding key
  std::string key_;
  std::vector<size_t> sizes_{1};
  // current outstanding req object
  Request req_{key_, sizes_.begin(), sizes_.end(), OpType::kGet};

  // number of times to issue the current req object
  // before fetching a new line from the trace
  uint32_t repeats_{1};
};

class AmplifiedReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit AmplifiedReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config) {}

  const Request& getReq(
      uint8_t poolId,
      std::mt19937_64& gen,
      std::optional<uint64_t> lastRequestId = std::nullopt) override {
    if (!tlRequest->isInitialized()) {
      tlRequest->threadIdx_ = incrementalIdx_++;

      XLOGF(INFO,
            "[{}] opening trace file {}",
            tlRequest->threadIdx_,
            config_.traceFileName);

      openInFile(tlRequest->infile_,
                 tlRequest->infileBuffer_,
                 sizeof(tlRequest->infileBuffer_));
    }

    const Request* req = nullptr;
    try {
      req = &tlRequest->getReq(poolId, gen, lastRequestId);
    } catch (const cachelib::cachebench::EndOfTrace& e) {
      if (!repeatTraceReplay_) {
        throw;
      }

      tlRequest->resetTraceFileToBeginning();
      req = &tlRequest->getReq(poolId, gen, lastRequestId);
    }

    return *req;
  }

 private:
  // Used to assign thread id
  std::atomic<uint32_t> incrementalIdx_{0};

  class dummyRequestStreamTag {};
  folly::ThreadLocal<RequestStream, dummyRequestStreamTag> tlRequest;
};

inline const Request& RequestStream::getReq(uint8_t,
                                            std::mt19937_64&,
                                            std::optional<uint64_t>) {
  if (--repeats_ > 0) {
    return req_;
  }
  while (repeats_ == 0) {
    // input format is: key,op,size,op_count,key_size
    std::string token;
    // Get key
    if (!std::getline(infile_, key_, ',')) {
      repeats_ = 1;
      throw cachelib::cachebench::EndOfTrace("");
    }

    // Get op
    std::getline(infile_, token, ',');
    // TODO only memcache optypes are supported
    if (!token.compare("GET")) {
      req_.setOp(OpType::kGet);
    } else if (!token.compare("SET")) {
      req_.setOp(OpType::kSet);
    } else if (!token.compare("DELETE")) {
      req_.setOp(OpType::kDel);
    } else {
      continue;
    }

    // Get size
    std::getline(infile_, token, ',');
    sizes_[0] = std::stoi(token);
    // Get op_count
    std::getline(infile_, token, ',');
    repeats_ = std::stoi(token);
    // Get key_size; allow to have dummy ','
    std::getline(infile_, token);
    auto tokenSize = token.find(',');
    if (tokenSize != std::string::npos) {
      token.resize(tokenSize);
    }

    // Generate key whose size matches with that of the original one.
    // To do so, the encoded key will be expanded with the thread ID
    // of 4 characters at the suffix and the remaining intermediate space
    // will be padded with '0'. The size of encoded key is 10 digits,
    // so the minimum key size will be 14 including the thread ID
    size_t keySize = std::max<size_t>(std::stoi(token), key_.size() + 4);
    // The key size should not exceed 256
    keySize = std::min<size_t>(keySize, 256);

    key_.resize(keySize - 4, '0');
    // Append thread Id of 4 decimal chars
    key_.append(folly::sformat("{:04d}", threadIdx_));
  }

  return req_;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
