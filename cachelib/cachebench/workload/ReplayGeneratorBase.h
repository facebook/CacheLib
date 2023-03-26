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
#include <unordered_map>
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
  }

  std::istream& getline(std::string& str) {
    while (!infile_.is_open() || !std::getline(infile_, str)) {
      openNextInfile();
    }

    return infile_;
  }

  std::string getHeaderLine() { return headerLine_; }

  std::vector<std::string> parseRow(const std::string& line) {
    // Parses a line from the trace file into a vector.
    // Returns an empty vector
    std::vector<std::string> splitRow;
    folly::split(',', line, splitRow);
    if (splitRow.size() != columnKeyMap_.size()) {
      XLOG_EVERY_MS(WARNING, 1000)
          << "Expected row with " << columnKeyMap_.size()
          << " elements, but found " << splitRow.size() << "\nRow: " << line;
      return std::vector<std::string>({});
    } else {
      return splitRow;
    }
  }

  const std::unordered_map<std::string, size_t>& getColumnKeyMap() {
    return columnKeyMap_;
  }

  bool validateRequiredFields(const std::vector<std::string>& requiredFields) {
    bool valid = true;
    for (const auto& fieldName : requiredFields) {
      if (!traceHasField(fieldName)) {
        XLOG(WARNING) << "Trace is missing required field " << fieldName;
        valid = false;
      }
    }
    return valid;
  }

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

    parseHeaderRow(headerLine_);
  }

  void parseHeaderRow(const std::string& header) {
    folly::split(',', header, keys_);
    for (size_t i = 0; i < keys_.size(); i++) {
      columnKeyMap_.emplace(folly::to<std::string>(keys_[i]), i);
    }
    std::string headerString;
    for (const auto& kv : columnKeyMap_) {
      headerString +=
          "(" + kv.first + ", " + folly::to<std::string>(kv.second) + ")  ";
    }
    XLOG_EVERY_MS(INFO, 1000) << "Parsed header row: " << headerString;
  }

  const std::vector<std::string>& getAllKeys() { return keys_; }

  bool traceHasField(const std::string& field) {
    for (const auto& k : columnKeyMap_) {
      if (k.first == field) {
        return true;
      }
    }
    return false;
  }

  std::string idStr_;

  const StressorConfig config_;

  std::string configPath_;
  const bool repeatTraceReplay_;

  std::string headerLine_;

  std::vector<std::string> infileNames_;
  size_t nextInfileIdx_ = 0;
  std::ifstream infile_;
  char infileBuffer_[kIfstreamBufferSize];

  std::vector<std::string> keys_;
  std::unordered_map<std::string, size_t> columnKeyMap_;
};

class ReplayGeneratorBase : public GeneratorBase {
 public:
  explicit ReplayGeneratorBase(const StressorConfig& config)
      : config_(config),
        repeatTraceReplay_{config_.repeatTraceReplay},
        numShards_(config.numThreads),
        mode_(config_.replayGeneratorConfig.getSerializationMode()) {
    if (config.checkConsistency) {
      throw std::invalid_argument(
          "Cannot replay traces with consistency checking enabled");
    }
  }

  const std::vector<std::string>& getAllKeys() const override {
    throw std::logic_error("no keys precomputed!");
  }

 protected:
  const StressorConfig config_;
  const bool repeatTraceReplay_;

  // # of shards is equal to the # of stressor threads
  const uint32_t numShards_;
  const ReplayGeneratorConfig::SerializeMode mode_{
      ReplayGeneratorConfig::SerializeMode::strict};

  std::vector<std::string> keys_;

  // Return the shard for the key.
  uint32_t getShard(folly::StringPiece key) {
    if (mode_ == ReplayGeneratorConfig::SerializeMode::strict) {
      return folly::hash::SpookyHashV2::Hash32(key.begin(), key.size(), 0) %
             numShards_;
    } else {
      // TODO: implement the relaxed mode
      return folly::Random::rand32(numShards_);
    }
  }
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
