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
  explicit ReplayGenerator(StressorConfig config)
      : ReplayGeneratorBase(config),
        sizes_(1),
        req_(key_, sizes_.begin(), sizes_.end()),
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

  OpType getOp(uint8_t,
               std::mt19937&,
               std::optional<uint64_t> requestId = std::nullopt) override;

  void notifyResult(uint64_t, uint8_t) override {}

  template <typename CacheT>
  std::pair<size_t, std::chrono::seconds> prepopulateCache(CacheT& cache);

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
  // TODO optype parsing
  op_ = OpType::kGet;
  std::getline(infile_, token, ',');
  sizes_[0] = std::stoi(token);
  std::getline(infile_, token);
  repeats_ = std::stoi(token);
  return req_;
}

OpType ReplayGenerator::getOp(uint8_t, std::mt19937&, std::optional<uint64_t>) {
  return op_;
}

template <typename CacheT>
std::pair<size_t, std::chrono::seconds> ReplayGenerator::prepopulateCache(
    CacheT& cache) {
  size_t count(0);
  std::mt19937 gen(folly::Random::rand32());
  constexpr size_t batchSize = 1UL << 20;
  auto startTime = std::chrono::steady_clock::now();
  for (auto pid : cache.poolIds()) {
    while (cache.getPoolStats(pid).numEvictions() == 0) {
      for (size_t j = 0; j < batchSize; j++) {
        // we know using pool 0 is safe here, the trace generator doesn't use
        // this parameter
        getReq(0, gen);
        // to speed up prepopulation, don't repeat keys
        repeats_ = 1;
        const auto allocHandle =
            cache.allocate(pid, req_.key, req_.key.size() + *(req_.sizeBegin));
        if (allocHandle) {
          cache.insertOrReplace(allocHandle);
          // We throttle in case we are using flash so that we dont drop
          // evictions to flash by inserting at a very high rate.
          if (!cache.isRamOnly() && count % 8 == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
          }
          count++;
        }
      }
    }
  }

  return std::make_pair(count,
                        std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now() - startTime));
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
