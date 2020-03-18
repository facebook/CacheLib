#pragma once

#include <atomic>

namespace facebook {
namespace cachelib {
namespace cachebench {

class GeneratorBase {
 public:
  virtual ~GeneratorBase() {}

  // Grab the next request given the last request in the sequence or using the
  // random number generator.
  // @param poolId cache pool id for the generated request
  // @param gen random number generator
  // @param lastRequestId generator may generate next request based on last
  // request, e.g., piecewise caching in bigcache.
  virtual const Request& getReq(uint8_t /*poolId*/,
                                std::mt19937& /*gen*/,
                                std::optional<uint64_t> /*lastRequestId*/) = 0;

  // Notify the workload generator about the result of the request.
  // Note the workload generator may release the memory for the request.
  virtual void notifyResult(uint64_t /*requestId*/, OpResultType /*result*/) {}

  virtual const std::vector<std::string>& getAllKeys() const = 0;

  // TODO: check if this can be removed
  virtual void registerThread() {}

  virtual void renderStats(uint64_t /*elapsedTimeNs*/,
                           std::ostream& /*out*/) const {}

  void setIsPrepopulate(bool flag) {
    isPrepopulate_.store(flag, std::memory_order_relaxed);
  }

  bool isPrepopulate() const {
    return isPrepopulate_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<bool> isPrepopulate_{false};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
