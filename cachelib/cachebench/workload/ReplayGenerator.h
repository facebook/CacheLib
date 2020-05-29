#pragma once

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class ReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit ReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config),
        sizes_(1),
        req_(key_, sizes_.begin(), sizes_.end(), OpType::kGet),
        repeats_(1) {}

  virtual ~ReplayGenerator() {}

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  // fbid,OpType,size,repeats
  //
  // Here, repeats gives a number of times to repeat the request specified on
  // this line before reading the next line of the file.
  // TODO: not thread safe, can only work with single threaded stressor
  const Request& getReq(
      uint8_t,
      std::mt19937&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

 private:
  // current outstanding key
  std::string key_;
  std::vector<size_t> sizes_;
  // current outstanding req object
  Request req_;

  // number of times to issue the current req object
  // before fetching a new line from the trace
  uint32_t repeats_;
};

const Request& ReplayGenerator::getReq(uint8_t,
                                       std::mt19937&,
                                       std::optional<uint64_t>) {
  if (--repeats_ > 0) {
    return req_;
  }
  std::string token;
  if (!std::getline(infile_, key_, ',')) {
    repeats_ = 1;
    throw cachelib::cachebench::EndOfTrace("");
  }
  std::getline(infile_, token, ',');

  std::getline(infile_, token, ',');
  sizes_[0] = std::stoi(token);
  std::getline(infile_, token);
  repeats_ = std::stoi(token);
  // TODO optype parsing
  return req_;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
