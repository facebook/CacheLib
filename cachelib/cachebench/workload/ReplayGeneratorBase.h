#pragma once

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

constexpr size_t kIfstreamBufferSize = 1L << 14;

class ReplayGeneratorBase : public GeneratorBase {
 public:
  explicit ReplayGeneratorBase(StressorConfig config) : config_(config) {
    if (config.checkConsistency) {
      throw std::invalid_argument(folly::sformat(
          "Cannot replay traces with consistency checking enabled"));
    }
    std::string file;
    if (config.traceFileName[0] == '/') {
      file = config.traceFileName;
    } else {
      file = folly::sformat("{}/{}", config.configPath, config.traceFileName);
    }
    infile_.open(file);
    if (infile_.fail()) {
      throw std::invalid_argument(
          folly::sformat("could not read file: {}", file));
    }
    infile_.rdbuf()->pubsetbuf(infileBuffer_, kIfstreamBufferSize);
    // header
    std::string row;
    std::getline(infile_, row);
  }

  virtual ~ReplayGeneratorBase() { infile_.close(); }

  void registerThread() {}

  const std::vector<std::string>& getAllKeys() const {
    throw std::logic_error("ReplayGenerator has no keys precomputed!");
  }

 protected:
  // ifstream pointing to the trace file
  std::ifstream infile_;
  std::vector<std::string> keys_;
  char infileBuffer_[kIfstreamBufferSize];

  const StressorConfig config_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
