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
class OnlineGenerator {
 public:
  explicit OnlineGenerator(const StressorConfig& config);

  const Request& getReq(uint8_t poolId, std::mt19937& gen);

  OpType getOp(uint8_t pid, std::mt19937& gen);

  const std::vector<std::string>& getAllKeys() const { return allKeys__; }

  template <typename CacheT>
  std::pair<size_t, std::chrono::seconds> prepopulateCache(CacheT& cache);
  void registerThread();

 private:
  void generateFirstKeyIndexForPool();
  void generateKey(uint8_t poolId, size_t idx, std::string& key);
  void generateKeyLengths();

  void generateSizes();
  typename std::vector<std::vector<size_t>>::iterator generateSize(
      uint8_t poolId, size_t idx);
  void generateKeyDistributions();
  // if there is only one workloadDistribution, use it for everything.
  size_t workloadIdx(size_t i) { return workloadDist_.size() > 1 ? i : 0; }

  const StressorConfig config_;
  std::vector<std::vector<size_t>> keyLengths_;
  std::vector<std::vector<std::vector<size_t>>> sizes_;

  std::unordered_map<typename std::thread::id, Request> reqs_;
  std::unordered_map<typename std::thread::id, std::string> keys_;
  // Placeholder
  std::vector<std::string> allKeys__;
  folly::SharedMutex registryLock_;

  // @firstKeyIndexForPool_ contains the first key in each pool (As represented
  // by key pool distribution). It's a convenient method for us to populate
  // @keyIndicesForPoo_ which contains all the key indices that each operation
  // in a pool. @keyGenForPool_ contains uniform distributions to select indices
  // contained in @keyIndicesForPool_.
  std::vector<uint64_t> firstKeyIndexForPool_;
  std::vector<std::vector<uint32_t>> keyIndicesForPool_;
  std::vector<std::uniform_int_distribution<uint32_t>> keyGenForPool_;

  std::vector<Distribution> workloadDist_;
};

template <typename Distribution>
template <typename CacheT>
std::pair<size_t, std::chrono::seconds>
OnlineGenerator<Distribution>::prepopulateCache(CacheT& cache) {
  auto prePopulateFn = [&](size_t start, size_t end) {
    if (start >= end) {
      return;
    }

    size_t count = start;
    uint8_t pid =
        std::distance(firstKeyIndexForPool_.begin(),
                      std::upper_bound(firstKeyIndexForPool_.begin(),
                                       firstKeyIndexForPool_.end(), start)) -
        1;
    std::mt19937 gen(folly::Random::rand32());
    std::string key("");
    while (count < end) {
      // getPoolStats is expensive to fetch on every iteratio and can
      // serialize the cache creation. So check the eviction coutn every 10k
      // insertions
      if (count >= firstKeyIndexForPool_[pid + 1] ||
          ((count % 10000 == 0) &&
           cache.getPoolStats(pid).numEvictions() > 0)) {
        pid++;
        if (pid >= firstKeyIndexForPool_.size() - 1) {
          break;
        }
      }
      generateKey(pid, count, key);
      size_t s = *(generateSize(pid, count)->begin());
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
  auto duration =
      detail::executeParallel(prePopulateFn, firstKeyIndexForPool_.back() - 1);

  return std::make_pair(keys_.size(), duration);
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/OnlineGenerator-inl.h"
