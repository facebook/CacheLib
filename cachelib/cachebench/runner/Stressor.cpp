#include "cachelib/cachebench/runner/Stressor.h"

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/cachebench/runner/CacheStressor.h"
#include "cachelib/cachebench/runner/FastShutdown.h"
#include "cachelib/cachebench/runner/IntegrationStressor.h"
#include "cachelib/cachebench/runner/OutputStressor.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
ThroughputStats& ThroughputStats::operator+=(const ThroughputStats& other) {
  set += other.set;
  setFailure += other.setFailure;
  get += other.get;
  getMiss += other.getMiss;
  del += other.del;
  delNotFound += other.delNotFound;
  addChained += other.addChained;
  addChainedFailure += other.addChainedFailure;
  ops += other.ops;

  return *this;
}

void ThroughputStats::render(uint64_t elapsedTimeNs, std::ostream& out) const {
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  const uint64_t setPerSec = set / elapsedSecs;
  const double setSuccessRate =
      set == 0 ? 0.0 : 100.0 * (set - setFailure) / set;

  const uint64_t getPerSec = get / elapsedSecs;
  const double getSuccessRate = get == 0 ? 0.0 : 100.0 * (get - getMiss) / get;

  const uint64_t delPerSec = del / elapsedSecs;
  const double delSuccessRate =
      del == 0 ? 0.0 : 100.0 * (del - delNotFound) / del;

  const uint64_t addChainedPerSec = addChained / elapsedSecs;
  const double addChainedSuccessRate =
      addChained == 0 ? 0.0
                      : 100.0 * (addChained - addChainedFailure) / addChained;

  out << std::fixed;
  out << folly::sformat("{:10}: {:.2f} million", "Total Ops", ops / 1e6)
      << std::endl;

  auto outFn = [&out](folly::StringPiece k1, uint64_t v1, folly::StringPiece k2,
                      double v2) {
    out << folly::sformat("{:10}: {:9,}/s, {:10}: {:6.2f}%", k1, v1, k2, v2)
        << std::endl;
  };
  outFn("get", getPerSec, "success", getSuccessRate);
  outFn("set", setPerSec, "success", setSuccessRate);
  outFn("del", delPerSec, "found", delSuccessRate);
  if (addChained > 0) {
    outFn("addChained", addChainedPerSec, "success", addChainedSuccessRate);
  }
}

std::unique_ptr<Stressor> Stressor::makeStressor(
    CacheConfig cacheConfig, StressorConfig stressorConfig) {
  if (stressorConfig.mode == "stress") {
    if (stressorConfig.allocator == "LRU" || stressorConfig.allocator.empty()) {
      // default allocator is LRU, other allocator types should be added here
      if (stressorConfig.generator == "piecewise-replay") {
        return std::make_unique<
            CacheStressor<LruAllocator, PieceWiseReplayGenerator>>(
            cacheConfig, stressorConfig);
      }
      if (stressorConfig.generator == "replay") {
        return std::make_unique<CacheStressor<LruAllocator, ReplayGenerator>>(
            cacheConfig, stressorConfig);
      }
      if (stressorConfig.generator.empty() ||
          stressorConfig.generator == "workload") {
        if (stressorConfig.distribution == "range") {
          return std::make_unique<CacheStressor<
              LruAllocator, WorkloadGenerator<RangeDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "normal") {
          return std::make_unique<CacheStressor<
              LruAllocator, WorkloadGenerator<NormalDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "workload" ||
                   stressorConfig.distribution.empty()) {
          return std::make_unique<
              CacheStressor<LruAllocator, WorkloadGenerator<>>>(cacheConfig,
                                                                stressorConfig);
        }
      } else if (stressorConfig.generator == "online") {
        if (stressorConfig.distribution == "range") {
          return std::make_unique<
              CacheStressor<LruAllocator, OnlineGenerator<RangeDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "normal") {
          return std::make_unique<
              CacheStressor<LruAllocator, OnlineGenerator<NormalDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "workload" ||
                   stressorConfig.distribution.empty()) {
          return std::make_unique<
              CacheStressor<LruAllocator, OnlineGenerator<>>>(cacheConfig,
                                                              stressorConfig);
        }
      } else {
        throw std::invalid_argument("Invalid config");
      }
      return std::make_unique<CacheStressor<LruAllocator>>(cacheConfig,
                                                           stressorConfig);
    } else if (stressorConfig.allocator == "LRU2Q") {
      // default allocator is LRU, other allocator types should be added here
      if (stressorConfig.generator == "piecewise-replay") {
        return std::make_unique<
            CacheStressor<Lru2QAllocator, PieceWiseReplayGenerator>>(
            cacheConfig, stressorConfig);
      }
      if (stressorConfig.generator == "replay") {
        return std::make_unique<CacheStressor<Lru2QAllocator, ReplayGenerator>>(
            cacheConfig, stressorConfig);
      }
      if (stressorConfig.generator.empty() ||
          stressorConfig.generator == "workload") {
        if (stressorConfig.distribution == "range") {
          return std::make_unique<CacheStressor<
              Lru2QAllocator, WorkloadGenerator<RangeDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "normal") {
          return std::make_unique<CacheStressor<
              Lru2QAllocator, WorkloadGenerator<NormalDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "workload" ||
                   stressorConfig.distribution.empty()) {
          return std::make_unique<
              CacheStressor<Lru2QAllocator, WorkloadGenerator<>>>(
              cacheConfig, stressorConfig);
        }
      } else if (stressorConfig.generator == "online") {
        if (stressorConfig.distribution == "range") {
          return std::make_unique<CacheStressor<
              Lru2QAllocator, OnlineGenerator<RangeDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "normal") {
          return std::make_unique<CacheStressor<
              Lru2QAllocator, OnlineGenerator<NormalDistribution>>>(
              cacheConfig, stressorConfig);
        } else if (stressorConfig.distribution == "workload" ||
                   stressorConfig.distribution.empty()) {
          return std::make_unique<
              CacheStressor<Lru2QAllocator, OnlineGenerator<>>>(cacheConfig,
                                                                stressorConfig);
        }
      } else {
        throw std::invalid_argument("Invalid config");
      }
      return std::make_unique<CacheStressor<Lru2QAllocator>>(cacheConfig,
                                                             stressorConfig);
    }
  }
  if (stressorConfig.mode == "stdout") {
    return std::make_unique<OutputStressor<LruAllocator>>(cacheConfig,
                                                          stressorConfig);
  } else {
    if (stressorConfig.name == "high_refcount") {
      return std::make_unique<HighRefcountStressor>(cacheConfig,
                                                    stressorConfig.numOps);
    } else if (stressorConfig.name == "cachelib_map") {
      return std::make_unique<CachelibMapStressor>(cacheConfig,
                                                   stressorConfig.numOps);
    } else if (stressorConfig.name == "cachelib_range_map") {
      return std::make_unique<CachelibRangeMapStressor>(cacheConfig,
                                                        stressorConfig.numOps);
    } else if (stressorConfig.name == "fast_shutdown") {
      return std::make_unique<FastShutdownStressor>(cacheConfig,
                                                    stressorConfig.numOps);
    }
  }
  throw std::invalid_argument("Invalid config");
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
