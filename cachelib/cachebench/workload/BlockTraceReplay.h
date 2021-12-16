#pragma once

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class BlockTraceReplay : public ReplayGeneratorBase {
 public:
  explicit BlockTraceReplay(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        timestamp_(0),
        sizes_(1),
        op_(OpType::kGet),
        req_(key_, sizes_.begin(), sizes_.end(), op_, timestamp_) {}

  virtual ~BlockTraceReplay() {}

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file which consists of the fields:
  // timestamp (sec), LBA (512 sector size), OP ("r"/"w"), size (bytes)
  // 
  // TODO: not thread safe, can only work with single threaded stressor
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  private:
    uint64_t timestamp_;
    std::string key_;
    std::vector<size_t> sizes_;
    OpType op_;
    Request req_;
};


const Request& BlockTraceReplay::getReq(uint8_t,
                                      std::mt19937_64&,
                                      std::optional<uint64_t>) {

  std::string row;
  if (!std::getline(infile_, row)) {
    throw cachelib::cachebench::EndOfTrace("");
  }

  std::stringstream row_stream(row);
  std::string token;

  std::getline(row_stream, token, ','); // timestamp in seconds
  timestamp_ = std::stoul(token);
  req_.timestamp = std::stoul(token);

  std::getline(row_stream, token, ','); // LBA  
  key_ = token;

  std::getline(row_stream, token, ','); // op
  if (token == "r") {
    op_ = OpType::kGet;
    req_.setOp(op_);
  }
  else if (token == "w") {
    op_ = OpType::kSet;
    req_.setOp(op_);
  }
  else
    throw std::invalid_argument("Unrecognized Operation in the trace file!");

  std::getline(row_stream, token, ','); // size
  sizes_[0] = std::stoi(token); 

  return req_;
}


} // namespace cachebench
} // namespace cachelib
} // namespace facebook