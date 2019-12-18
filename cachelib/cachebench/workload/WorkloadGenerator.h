#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/distributions/DiscreteDistribution.h"
#include "cachelib/cachebench/workload/distributions/NormalDistribution.h"
#include "cachelib/cachebench/workload/distributions/RangeDistribution.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

template <typename Distribution = DiscreteDistribution>
class WorkloadGenerator {
 public:
  explicit WorkloadGenerator(const StressorConfig& config);
  void registerThread() {}

  const Request& getReq(uint8_t poolId, std::mt19937& gen);

  OpType getOp(uint8_t pid, std::mt19937& gen);

  const std::vector<std::string>& getAllKeys() const { return keys_; }

  template <typename CacheT>
  std::pair<size_t, std::chrono::seconds> prepopulateCache(CacheT& cache);

 private:
  void generateFirstKeyIndexForPool();
  void generateKeys();
  void generateReqs();
  void generateKeyDistributions();
  // if there is only one workloadDistribution, use it for everything.
  size_t workloadIdx(size_t i) { return workloadDist_.size() > 1 ? i : 0; }

  const StressorConfig& config_;
  std::vector<std::string> keys_;
  std::vector<std::vector<size_t>> sizes_;
  std::vector<Request> reqs_;
  // @firstKeyIndexForPool_ contains the first key in each pool (As represented
  // by key pool distribution). It's a convenient method for us to populate
  // @keyIndicesForPoo_ which contains all the key indices that each operation
  // in a pool. @keyGenForPool_ contains uniform distributions to select indices
  // contained in @keyIndicesForPool_.
  std::vector<uint32_t> firstKeyIndexForPool_;
  std::vector<std::vector<uint32_t>> keyIndicesForPool_;
  std::vector<std::uniform_int_distribution<uint32_t>> keyGenForPool_;

  std::vector<Distribution> workloadDist_;
};

template <typename Distribution>
template <typename CacheT>
std::pair<uint64_t, std::chrono::seconds>
WorkloadGenerator<Distribution>::prepopulateCache(CacheT& cache) {
  auto prePopulateFn = [&](size_t start, size_t end) {
    if (start >= end) {
      return;
    }

    size_t count = start;
    PoolId pid =
        std::distance(firstKeyIndexForPool_.begin(),
                      std::upper_bound(firstKeyIndexForPool_.begin(),
                                       firstKeyIndexForPool_.end(), start)) -
        1;
    std::mt19937 gen(folly::Random::rand32());
    while (count < end) {
      if (count >= firstKeyIndexForPool_[pid + 1]) {
        pid++;
      }
      const std::string& key = keys_.at(count);
      size_t s = workloadDist_[workloadIdx(pid)].sampleValDist(gen);
      const auto allocHandle = cache.allocate(pid, key, s);
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
  };

  auto duration = detail::executeParallel(prePopulateFn, keys_.size());
  return std::make_pair(keys_.size(), duration);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/WorkloadGenerator-inl.h"
