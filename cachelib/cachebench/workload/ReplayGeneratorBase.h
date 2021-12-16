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
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr size_t kIfstreamBufferSize = 1L << 14;

class ReplayGeneratorBase : public GeneratorBase {
 public:
  explicit ReplayGeneratorBase(const StressorConfig& config)
      : config_(config), repeatTraceReplay_{config_.repeatTraceReplay} {
    if (config.checkConsistency) {
      throw std::invalid_argument(folly::sformat(
          "Cannot replay traces with consistency checking enabled"));
    }
    std::string file;
    if (config.traceFilePath[0] == '/') {
      file = config.traceFilePath;
    } else {
      file = folly::sformat("{}/{}", config.configPath, config.traceFilePath);
    }
    infile_.open(file);
    if (infile_.fail()) {
      throw std::invalid_argument(
          folly::sformat("could not read file: {}", file));
    }
    infile_.rdbuf()->pubsetbuf(infileBuffer_, kIfstreamBufferSize);

    std::string row;
    std::getline(infile_, row);
  }

  virtual ~ReplayGeneratorBase() { infile_.close(); }

  const std::vector<std::string>& getAllKeys() const {
    throw std::logic_error("ReplayGenerator has no keys precomputed!");
  }

 protected:
  void resetTraceFileToBeginning() {
    infile_.clear();
    infile_.seekg(0, std::ios::beg);

    std::string row;
    std::getline(infile_, row);
  }

  const StressorConfig config_;
  const bool repeatTraceReplay_;

  // ifstream pointing to the trace file
  std::ifstream infile_;
  std::vector<std::string> keys_;
  char infileBuffer_[kIfstreamBufferSize];
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
