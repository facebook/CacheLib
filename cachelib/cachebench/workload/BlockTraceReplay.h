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
        sizes_(1),
        op_(OpType::kGet),
        req_(key_, sizes_.begin(), sizes_.end(), OpType::kGet) {}

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
    long timestamp_;
    std::string key_;
    std::vector<size_t> sizes_;
    OpType op_;
    Request req_;
    std::int16_t startPageIndex_ = 0;
    std::int16_t endPageIndex_ = 0;
    std::int16_t lastPageIndex_ = 0;

};


const Request& BlockTraceReplay::getReq(uint8_t,
                                      std::mt19937_64&,
                                      std::optional<uint64_t>) {

  if (lastPageIndex_ == endPageIndex_) {
    std::string row;
    if (!std::getline(infile_, row)) {
      throw cachelib::cachebench::EndOfTrace("");
    }
    std::stringstream s_stream(row);
    std::string token;

    std::getline(s_stream, token, ','); // timestamp in seconds
    timestamp_ = std::stol(token); 

    std::getline(s_stream, token, ','); // LBA  
    //TODO: take secor size (512) and page size (4096) as input 
    int start_byte = 512*std::stoi(token); // start byte based on sector size (Default: 512 bytes)
    startPageIndex_ = floor(start_byte/4096); // compute page index based on page size (Default: 4096 bytes)
    key_ = std::to_string(startPageIndex_); // page index is unique so used as key 

    std::getline(s_stream, token, ','); // op
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

    std::getline(s_stream, token, ','); // size
    int end_byte = start_byte + std::stoi(token) - 1;
    endPageIndex_ = int(ceil(end_byte/4096));
    sizes_[0] = 4096; // 4KB default page size 
    lastPageIndex_ = startPageIndex_;
  } else {
    lastPageIndex_++;
    key_ = std::to_string(lastPageIndex_);
  }

  return req_;
}


} // namespace cachebench
} // namespace cachelib
} // namespace facebook
