#pragma once

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/ThreadLocal.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <string>
#include <vector>

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
      std::mt19937_64& gen,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  const std::vector<std::string>& getAllKeys() const {
    throw std::logic_error("OnlineGenerator has no keys precomputed!");
  }

 private:
  uint64_t getKeyIdx(uint8_t poolId, std::mt19937_64& gen);
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

  // size distributions per pool, where each pool has a vector of chain sizes
  // per idx. This is pre-generated to always generate the same size
  // corresponding to a key idx
  std::vector<std::vector<std::vector<size_t>>> sizes_;

  // key sizes prepopulated per pool. These are populated in advance to ensure
  // the size corresponding to a key id is always the same.
  std::vector<std::vector<size_t>> keyLengths_;

  // used for thread local intialization
  std::vector<size_t> dummy_;

  // @firstKeyIndexForPool_ contains the first key in each pool (As represented
  // by key pool distribution).
  std::vector<uint64_t> firstKeyIndexForPool_;

  using PopDistT = typename Distribution::PopDistT;
  // overall workload distribution per pool
  std::vector<Distribution> workloadDist_;

  // popularity distribution for keys per pool
  std::vector<PopDistT> workloadPopDist_;

  // thread local copy of key and  request to return as a part of getReq
  class Tag;
  folly::ThreadLocal<std::string, Tag> key_;
  folly::ThreadLocal<Request, Tag> req_;

  static constexpr size_t kNumUniqueKeyLengths{1ULL << 15};
  static constexpr size_t kNumUniqueSizes{1ULL << 15};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/cachebench/workload/OnlineGenerator-inl.h"
