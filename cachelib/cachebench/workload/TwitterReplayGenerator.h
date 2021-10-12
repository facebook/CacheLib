#pragma once

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class TwitterReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit TwitterReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        sizes_(1),
        op_(OpType::kGet),
        req_(key_, sizes_.begin(), sizes_.end(), OpType::kGet) {}

  virtual ~TwitterReplayGenerator() {}

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file which consists of the fields:
  // timestamp, key, key size, value size, client ID, operation, TTL
  // 
  // TODO: not thread safe, can only work with single threaded stressor
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  private:
    std::string key_;
    std::vector<size_t> sizes_;
    std::int64_t timestamp_;
    Request req_;
    OpType op_;

};

const Request& TwitterReplayGenerator::getReq(uint8_t,
                                      std::mt19937_64&,
                                      std::optional<uint64_t>) {

  std::string row;
  if (!std::getline(infile_, row)) {
    throw cachelib::cachebench::EndOfTrace("");
  }

  std::stringstream s_stream(row);
  std::string token;
  std::getline(s_stream, token, ',');
  timestamp_ = std::stoi(token); // timestamp in seconds
  std::getline(s_stream, key_, ','); // key
  std::getline(s_stream, token, ','); // key size
  std::getline(s_stream, token, ','); // value size
  sizes_[0] = std::stoi(token); 
  std::getline(s_stream, token, ','); // client ID
  std::getline(s_stream, token, ','); // op

  // TODO: implement support for functions like add 
  // and replace (check and set) just set for now 
  if ((token == "get") or (token == "gets")) {
    req_.setOp(OpType::kGet);
  }
  else if ((token == "set") or (token == "cas") or \
    (token == "add") or (token == "replace") or \
    (token == "incr") or (token == "decr") or 
    (token == "prepend") or (token == "append")) {
    req_.setOp(OpType::kSet);
  }
  else if (token == "delete")
    req_.setOp(OpType::kDel);
  else
    throw std::invalid_argument("Unrecognized Operation in the trace file!");

  std::getline(s_stream, token, ','); // TTL

  return req_;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
