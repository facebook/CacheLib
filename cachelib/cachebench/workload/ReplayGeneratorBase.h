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

// ColumnInfo is to pass the information required to parse the trace
// to map the column to the replayer-specific field ID.
struct ColumnInfo {
  // replayer specific field ID
  uint8_t fieldId;
  // the field is mandatory or not
  bool required;
  // For the same field ID, the replayer can support multiple column names
  std::vector<std::string> columnNames;
};

using ColumnTable = std::vector<ColumnInfo>;

class TraceFileStream {
 public:
  TraceFileStream(const StressorConfig& config,
                  int64_t id,
                  const ColumnTable& columnTable)
      : configPath_(config.configPath),
        repeatTraceReplay_(config.repeatTraceReplay) {
    idStr_ = folly::sformat("[{}]", id);
    if (!config.traceFileName.empty()) {
      infileNames_.push_back(config.traceFileName);
    } else {
      infileNames_ = config.traceFileNames;
    }

    // construct columnMap_ and requireFields_ table
    for (auto& info : columnTable) {
      for (auto& name : info.columnNames) {
        columnMap_[name] = info.fieldId;
      }
      if (requiredFields_.size() < info.fieldId + 1) {
        requiredFields_.resize(info.fieldId + 1, false);
      }
      requiredFields_[info.fieldId] = info.required;
    }
  }

  std::istream& getline(std::string& str) {
    while (!infile_.is_open() || !std::getline(infile_, str)) {
      openNextInfile();
    }

    return infile_;
  }

  bool setNextLine(const std::string& line) {
    nextLineFields_.clear();
    folly::split(",", line, nextLineFields_);
    if (nextLineFields_.size() < minNumFields_) {
      XLOG_N_PER_MS(INFO, 10, 1000) << folly::sformat(
          "Error parsing next line \"{}\": shorter than min required fields {}",
          line, minNumFields_);
      return false;
    }
    return true;
  }

  template <typename T = folly::StringPiece>
  folly::Optional<T> getField(uint8_t fieldId) {
    int columnIdx = fieldId;
    if (fieldMap_.size() > fieldId) {
      columnIdx = fieldMap_[fieldId];
    }

    if (columnIdx < 0 ||
        nextLineFields_.size() <= static_cast<size_t>(columnIdx)) {
      return folly::none;
    }

    auto field = folly::tryTo<T>(nextLineFields_[columnIdx]);
    if (!field.hasValue()) {
      return folly::none;
    }

    return field.value();
  }

  bool setHeaderRow(const std::string& header) {
    if (!columnMap_.size()) {
      return true;
    }

    // reset fieldMap_
    fieldMap_.clear();
    fieldMap_.resize(requiredFields_.size(), -1);

    bool valid = false;
    keys_.clear();
    folly::split(',', header, keys_);
    for (size_t i = 0; i < keys_.size(); i++) {
      if (columnMap_.find(keys_[i]) == columnMap_.end()) {
        continue;
      }

      size_t fieldNum = columnMap_[keys_[i]];
      XCHECK_GE(fieldMap_.size(), fieldNum + 1);
      if (fieldMap_[fieldNum] != -1) {
        XLOGF(INFO,
              "Error parsing header \"{}\": "
              "duplicated field {} in columns {} {}",
              header, keys_[i], fieldMap_[fieldNum], i);
      } else {
        fieldMap_[fieldNum] = i;
      }

      valid = true;
    }

    if (!valid) {
      XLOG_N_PER_MS(INFO, 10, 1000) << folly::sformat(
          "Error parsing header \"{}\": no field recognized", header);
      return false;
    }

    std::string fieldMapStr;
    minNumFields_ = 0;
    for (size_t i = 0; i < fieldMap_.size(); i++) {
      if (fieldMap_[i] == -1) {
        if (requiredFields_[i]) {
          XLOG_N_PER_MS(INFO, 10, 1000) << folly::sformat(
              "Error parsing header \"{}\": required field {} is missing",
              header, i);
          return false;
        }
        continue;
      }

      if (requiredFields_[i]) {
        minNumFields_ = std::max<size_t>(minNumFields_, fieldMap_[i] + 1);
      }

      fieldMapStr +=
          folly::sformat("{}{} -> {}", fieldMapStr.size() ? ", " : "",
                         keys_[fieldMap_[i]], fieldMap_[i]);
    }

    XLOG_N_PER_MS(INFO, 10, 1000) << folly::sformat(
        "New header detected: header \"{}\" field map {}", header, fieldMapStr);
    return true;
  }

 private:
  bool openNextInfile() {
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
      return false;
    }

    XLOGF(INFO, "{} Opened trace file {}", idStr_, traceFileName);
    infile_.rdbuf()->pubsetbuf(infileBuffer_, sizeof(infileBuffer_));
    // header
    std::string headerLine;
    std::getline(infile_, headerLine);

    if (!setHeaderRow(headerLine)) {
      fieldMap_.clear();
    }
    return true;
  }

  const std::vector<std::string>& getAllKeys() { return keys_; }

  std::string idStr_;

  const StressorConfig config_;

  std::string configPath_;
  const bool repeatTraceReplay_;

  // column map is map of <column name> -> <field id> and used to
  // create filed map of <field id> -> <column idx>
  std::unordered_map<std::string, uint8_t> columnMap_;
  std::vector<int> fieldMap_;

  // keep track of required fields for each field id
  std::vector<bool> requiredFields_;

  // minNumFields_ is the minimum number of fields required
  size_t minNumFields_ = 0;

  std::vector<std::string> infileNames_;
  size_t nextInfileIdx_ = 0;
  std::ifstream infile_;
  char infileBuffer_[kIfstreamBufferSize];

  std::vector<folly::StringPiece> nextLineFields_;

  std::vector<std::string> keys_;
};

class ReplayGeneratorBase : public GeneratorBase {
 public:
  explicit ReplayGeneratorBase(const StressorConfig& config)
      : config_(config),
        repeatTraceReplay_{config_.repeatTraceReplay},
        ampFactor_(config.replayGeneratorConfig.ampFactor),
        timestampFactor_(config.timestampFactor),
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
  const size_t ampFactor_;

  // The constant to be divided from the timestamp value
  // to turn the timestamp into seconds.
  const uint64_t timestampFactor_{1};

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
