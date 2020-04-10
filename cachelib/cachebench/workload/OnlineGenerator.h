#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/ThreadLocal.h>
#include <folly/logging/xlog.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Config.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"
#include "cachelib/cachebench/workload/distributions/DiscreteDistribution.h"
#include "cachelib/cachebench/workload/distributions/NormalDistribution.h"
#include "cachelib/cachebench/workload/distributions/RangeDistribution.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

template <typename Distribution = DiscreteDistribution>
class OnlineGenerator : public GeneratorBase {
 public:
  explicit OnlineGenerator(const StressorConfig& config);
  virtual ~OnlineGenerator() {}

  const Request& getReq(
      uint8_t poolId,
      std::mt19937& gen,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  const std::vector<std::string>& getAllKeys() const {
    throw std::logic_error("OnlineGenerator has no keys precomputed!");
  }

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
  std::vector<size_t> dummy_;

  // Placeholder
  std::vector<std::string> allKeys__;

  // @firstKeyIndexForPool_ contains the first key in each pool (As represented
  // by key pool distribution). It's a convenient method for us to populate
  // @keyIndicesForPoo_ which contains all the key indices that each operation
  // in a pool. @keyGenForPool_ contains uniform distributions to select indices
  // contained in @keyIndicesForPool_.
  std::vector<uint64_t> firstKeyIndexForPool_;
  std::vector<std::vector<uint32_t>> keyIndicesForPool_;
  std::vector<std::uniform_int_distribution<uint32_t>> keyGenForPool_;

  std::vector<Distribution> workloadDist_;

  // segments the global mutex for the ThreadLocal object
  class Tag;
  folly::ThreadLocal<std::string, Tag> key_;
  folly::ThreadLocal<Request, Tag> req_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/OnlineGenerator-inl.h"
