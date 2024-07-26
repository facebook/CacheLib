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

#include <errno.h>
#include <fcntl.h>
#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>
#include <folly/synchronization/DistributedMutex.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

#define PG_SIZE 4096

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr size_t kPgRelease = 100000000;
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

  // Returns the total number of requests in the trace
  // plus the average key size so we can properly
  // set up the binary file stream via mmap
  //
  // We return a pair, the first value is the number
  // of lines in the trace and the second value is the
  // average key size
  std::pair<uint64_t, uint64_t> getTraceRequestStats() {
    uint64_t lines = 0;
    uint64_t totalKeySize = 0;
    const std::string& traceFileName = infileNames_[0];
    std::string filePath;
    if (traceFileName[0] == '/') {
      filePath = traceFileName;
    } else {
      filePath = folly::sformat("{}/{}", configPath_, traceFileName);
    }

    std::ifstream file;
    std::string line;
    file.open(filePath);
    if (file.fail()) {
      XLOGF(ERR, "Failed to open trace file {}", filePath);
      return std::make_pair(0, 0);
    }

    XLOGF(INFO, "Opened trace file {}", filePath);
    bool first = true;
    while (std::getline(file, line)) {
      if (first) {
        setHeaderRow(line);
        first = false;
      } else {
        // Default order is key,op,size,op_count,key_size,ttl
        nextLineFields_.clear();
        folly::split(",", line, nextLineFields_);

        if (nextLineFields_.size() < minNumFields_) {
          XLOG_N_PER_MS(INFO, 10, 1000) << folly::sformat(
              "Error parsing next line \"{}\": shorter than min required "
              "fields {}",
              line, minNumFields_);
        }
        auto keySizeField = getField<size_t>(2); 
        if (!keySizeField.hasValue()) {
          XLOG_N_PER_MS(INFO, 10, 1000) << folly::sformat(
              "Error parsing next line \"{}\": key size not found");
        } else {
          // The key is encoded as <encoded key, key size>.
          size_t keySize = keySizeField.value();
          // The key size should not exceed 256
          keySize = std::min<size_t>(keySize, 256);
          totalKeySize += keySize;
          lines++;
        }
      }
    }
    file.close();

    return std::make_pair(lines, totalKeySize);
  }

  // The number of requests (not including ampFactor) to skip
  // in the trace. This is so that after warming up the cache
  // with a certain number of requests, we can easily reattach
  // and resume execution with different cache configurations.
  void fastForwardTrace(uint64_t fastForwardCount) {
    uint64_t count = 0;
    while (count < fastForwardCount) {
      std::string line;
      this->getline(line); // can throw
      count++;
    }
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
      XLOGF(ERR, "{} Failed to open trace file {}", idStr_, filePath);
      return false;
    }

    XLOGF(INFO, "{} Opened trace file {}", idStr_, filePath);
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
  uint64_t lines_ = 0;
};

class BinaryFileStream {
 public:
  BinaryFileStream(const StressorConfig& config, const uint32_t numShards)
      : infileName_(config.traceFileName) {

    std::string filePath;
    if (infileName_[0] == '/') {
      filePath = infileName_;
    } else {
      filePath = folly::sformat("{}/{}", config.configPath, infileName_);
    }
    fd_ = open(filePath.c_str(), O_RDONLY);
    // Get the size of the file
    struct stat fileStat;
    if (fstat(fd_, &fileStat) == -1) {
      close(fd_);
      XLOGF(INFO, "Error reading file size {}", filePath);
    }
    size_t* binaryData = reinterpret_cast<size_t*>(
        mmap(nullptr, fileStat.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE,
             fd_, 0));

    // binary data pg align save for releasing old requests
    pgBinaryData_ = reinterpret_cast<void*>(binaryData);

    // first value is the number of requests in the file
    nreqs_ = binaryData[0];
    binaryData++;
    // next is the request data
    binaryReqData_ = reinterpret_cast<BinaryRequest*>(binaryData);
    binaryKeyData_ = reinterpret_cast<char*>(binaryData);

    // keys are stored after request structures
    binaryKeyData_ += nreqs_ * sizeof(BinaryRequest);
    // save the pg aligned key space for releasing old key data
    pgBinaryKeyData_ = reinterpret_cast<void*>(
        reinterpret_cast<uint64_t>(binaryKeyData_) +
        (PG_SIZE - (reinterpret_cast<uint64_t>(binaryKeyData_) % PG_SIZE)));

    releaseCount_ = 1; // release data after first nRelease reqs
    nRelease_ = kPgRelease;
    releaseIdx_ = nRelease_ * releaseCount_;
  }

  ~BinaryFileStream() { close(fd_); }

  uint64_t getNumReqs() { return nreqs_; }

  void releaseOldData(uint64_t reqsCompleted) {
    // The number of bytes from the key data stream to release
    uint64_t keyBytes =
        (reinterpret_cast<uint64_t>(binaryReqData_ + releaseIdx_) +
         binaryReqData_[releaseIdx_].keyOffset_) -
        reinterpret_cast<uint64_t>(binaryKeyData_);

    int kres = madvise(reinterpret_cast<void*>(pgBinaryKeyData_), keyBytes,
                       MADV_DONTNEED);
    if (kres != 0) {
      XLOGF(INFO, "Failed to release old keys, key bytes: {}, result: {}",
            keyBytes, strerror(errno));
      XDCHECK_EQ(kres, 0);
    } else {
      XLOGF(INFO, "Release old keys, key bytes: {}", keyBytes);
    }

    int reqRes = madvise(reinterpret_cast<void*>(pgBinaryData_),
                         (releaseIdx_) * sizeof(BinaryRequest) + sizeof(size_t),
                         MADV_DONTNEED);

    XDCHECK_EQ(reqRes, 0);
    if (reqRes != 0) {
      XLOGF(INFO,
            "Failed to release old requests, released: {}, completed: {}, "
            "result: {}",
            releaseIdx_, reqsCompleted, strerror(errno));
    } else {
      XLOGF(INFO, "Released old requests, released: {}, completed: {}",
            releaseIdx_, reqsCompleted);
    }

    releaseIdx_ = nRelease_ * (++releaseCount_);
  }

  BinaryRequest* getNextPtr(uint64_t reqIdx) {
    if ((reqIdx > releaseIdx_) && (reqIdx % nRelease_ == 0)) {
      releaseOldData(reqIdx);
    }
    if (reqIdx >= nreqs_) {
      releaseCount_ = 1;
      releaseIdx_ = nRelease_ * releaseCount_;
      throw cachelib::cachebench::EndOfTrace("");
    }
    BinaryRequest* binReq = binaryReqData_ + reqIdx;
    XDCHECK_LT(binReq->op_, 12);
    return binReq;
  }

 private:
  const StressorConfig config_;
  std::string infileName_;

  // pointers to mmaped data
  BinaryRequest* binaryReqData_;
  char* binaryKeyData_;

  // these two pointers are to madvise
  // away old request data
  void* pgBinaryKeyData_;
  void* pgBinaryData_;

  // number of requests released so far
  size_t releaseIdx_;
  size_t releaseCount_;
  // how often to release old requests
  size_t nRelease_;

  size_t fileSize_;
  uint64_t nreqs_;
  int fd_;
};

class ReplayGeneratorBase : public GeneratorBase {
 public:
  explicit ReplayGeneratorBase(const StressorConfig& config)
      : config_(config),
        repeatTraceReplay_{config_.repeatTraceReplay},
        ampFactor_(config.replayGeneratorConfig.ampFactor),
        ampSizeFactor_(config.replayGeneratorConfig.ampSizeFactor),
        binaryFileName_(config.replayGeneratorConfig.binaryFileName),
        fastForwardCount_(config.replayGeneratorConfig.fastForwardCount),
        preLoadReqs_(config.replayGeneratorConfig.preLoadReqs),
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
  const size_t ampSizeFactor_;
  const uint64_t fastForwardCount_;
  const uint64_t preLoadReqs_;
  const std::string binaryFileName_;
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
