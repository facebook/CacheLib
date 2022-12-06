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

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr size_t kIfstreamBufferSize = 1L << 14;

class TraceFileStream {
 public:
  TraceFileStream(const StressorConfig& config, int64_t id)
      : configPath_(config.configPath),
        repeatTraceReplay_(config.repeatTraceReplay) {
    idStr_ = folly::sformat("[{}]", id);
    if (!config.traceFileName.empty()) {
      infileNames_.push_back(config.traceFileName);
    } else {
      infileNames_ = config.traceFileNames;
    }

    openNextInfile();
  }

  std::istream& getline(std::string& str) {
    while (!std::getline(infile_, str)) {
      openNextInfile();
    }

    return infile_;
  }

  std::string getHeaderLine() { return headerLine_; }

 private:
  void openNextInfile() {
    if (nextInfileIdx_ >= infileNames_.size()) {
      if (!repeatTraceReplay_) {
        throw cachelib::cachebench::EndOfTrace("");
      }

      XLOGF_EVERY_MS(
          INFO, 100'000,
          "{} Reached the end of trace files. Restarting from beginning.",
          idStr_);
      nextInfileIdx_ = 0;
    }

    if (infile_.is_open()) {
      infile_.close();
      infile_.clear();
    }

    const std::string& traceFileName = infileNames_[nextInfileIdx_++];

    std::string filePath;
    if (traceFileName[0] == '/') {
      filePath = traceFileName;
    } else {
      filePath = folly::sformat("{}/{}", configPath_, traceFileName);
    }

    infile_.open(filePath);
    if (infile_.fail()) {
      XLOGF(ERR, "{} Failed to open trace file {}", idStr_, traceFileName);
      return;
    }

    XLOGF(INFO, "{} Opened trace file {}", idStr_, traceFileName);
    infile_.rdbuf()->pubsetbuf(infileBuffer_, sizeof(infileBuffer_));
    // header
    std::getline(infile_, headerLine_);
  }

  std::string idStr_;

  std::string configPath_;
  bool repeatTraceReplay_{false};

  std::string headerLine_;

  std::vector<std::string> infileNames_;
  size_t nextInfileIdx_ = 0;
  std::ifstream infile_;
  char infileBuffer_[kIfstreamBufferSize];
};

class ReplayGeneratorBase : public GeneratorBase {
 public:
  explicit ReplayGeneratorBase(const StressorConfig& config)
      : config_(config), repeatTraceReplay_{config_.repeatTraceReplay} {
    if (config.checkConsistency) {
      throw std::invalid_argument(
          "Cannot replay traces with consistency checking enabled");
    }
  }

  const std::vector<std::string>& getAllKeys() const override {
    throw std::logic_error("ReplayGenerator has no keys precomputed!");
  }

 protected:
  const StressorConfig config_;
  const bool repeatTraceReplay_;

  std::vector<std::string> keys_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
