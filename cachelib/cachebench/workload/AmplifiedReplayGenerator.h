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
#include "cachelib/cachebench/workload/ReplayGenerator.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class AmplifiedReplayGenerator;

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

  AmplifiedReplayGenerator* parent = nullptr;

  // ifstream pointing to the trace file
  std::ifstream infile_;
  char infileBuffer_[kIfstreamBufferSize];
  uint32_t threadIdx_{INVALID_THREAD_ID};

  ReqWrapper replayReq_;
};

class AmplifiedReplayGenerator : public ReplayGenerator {
 public:
  explicit AmplifiedReplayGenerator(const StressorConfig& config)
      : ReplayGenerator(config) {}

  const Request& getReq(
      uint8_t poolId,
      std::mt19937_64& gen,
      std::optional<uint64_t> lastRequestId = std::nullopt) override {
    if (!tlReqStream->isInitialized()) {
      tlReqStream->parent = this;
      tlReqStream->threadIdx_ = incrementalIdx_++;

      XLOGF(INFO,
            "[{}] opening trace file {}",
            tlReqStream->threadIdx_,
            config_.traceFileName);

      openInFile(tlReqStream->infile_,
                 tlReqStream->infileBuffer_,
                 sizeof(tlReqStream->infileBuffer_));
    }

    const Request* req = nullptr;
    try {
      req = &tlReqStream->getReq(poolId, gen, lastRequestId);
    } catch (const cachelib::cachebench::EndOfTrace& e) {
      if (!repeatTraceReplay_) {
        throw;
      }

      tlReqStream->resetTraceFileToBeginning();
      req = &tlReqStream->getReq(poolId, gen, lastRequestId);
    }

    return *req;
  }

 private:
  // Used to assign thread id
  std::atomic<uint32_t> incrementalIdx_{0};

  class dummyRequestStreamTag {};
  folly::ThreadLocal<RequestStream, dummyRequestStreamTag> tlReqStream;
};

inline const Request& RequestStream::getReq(uint8_t,
                                            std::mt19937_64&,
                                            std::optional<uint64_t>) {
  if (!replayReq_.repeats_) {
    parent->getReqInternal(infile_, replayReq_);
    // Replace the last 4 bytes with thread Id of 4 decimal chars
    XDCHECK_GT(replayReq_.key_.size(), 4u);
    replayReq_.key_.resize(replayReq_.key_.size() - 4, '0');
    replayReq_.key_.append(folly::sformat("{:04d}", threadIdx_));
  }

  replayReq_.repeats_--;
  return replayReq_.req_;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
