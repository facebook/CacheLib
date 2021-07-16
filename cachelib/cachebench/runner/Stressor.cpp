#include "cachelib/cachebench/runner/Stressor.h"

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/cachebench/runner/CacheStressor.h"
#include "cachelib/cachebench/runner/FastShutdown.h"
#include "cachelib/cachebench/runner/IntegrationStressor.h"
#include "cachelib/cachebench/workload/OnlineGenerator.h"
#include "cachelib/cachebench/workload/PieceWiseReplayGenerator.h"
#include "cachelib/cachebench/workload/ReplayGenerator.h"
#include "cachelib/cachebench/workload/WorkloadGenerator.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
ThroughputStats& ThroughputStats::operator+=(const ThroughputStats& other) {
  set += other.set;
  setFailure += other.setFailure;
  get += other.get;
  getMiss += other.getMiss;
  del += other.del;
  update += other.update;
  updateMiss += other.updateMiss;
  delNotFound += other.delNotFound;
  addChained += other.addChained;
  addChainedFailure += other.addChainedFailure;
  ops += other.ops;

  return *this;
}

void ThroughputStats::render(uint64_t elapsedTimeNs, std::ostream& out) const {
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  const uint64_t setPerSec = util::narrow_cast<uint64_t>(set / elapsedSecs);
  const double setSuccessRate =
      set == 0 ? 0.0 : 100.0 * (set - setFailure) / set;

  const uint64_t getPerSec = util::narrow_cast<uint64_t>(get / elapsedSecs);
  const double getSuccessRate = get == 0 ? 0.0 : 100.0 * (get - getMiss) / get;

  const uint64_t delPerSec = util::narrow_cast<uint64_t>(del / elapsedSecs);
  const double delSuccessRate =
      del == 0 ? 0.0 : 100.0 * (del - delNotFound) / del;

  const uint64_t updatePerSec =
      util::narrow_cast<uint64_t>(update / elapsedSecs);
  const double updateSuccessRate =
      update == 0 ? 0.0 : 100.0 * (update - updateMiss) / update;

  const uint64_t addChainedPerSec =
      util::narrow_cast<uint64_t>(addChained / elapsedSecs);
  const double addChainedSuccessRate =
      addChained == 0 ? 0.0
                      : 100.0 * (addChained - addChainedFailure) / addChained;

  out << std::fixed;
  out << folly::sformat("{:10}: {:.2f} million", "Total Ops", ops / 1e6)
      << std::endl;
  out << folly::sformat("{:10}: {:,}", "Total sets", set) << std::endl;

  auto outFn = [&out](folly::StringPiece k1, uint64_t v1, folly::StringPiece k2,
                      double v2) {
    out << folly::sformat("{:10}: {:9,}/s, {:10}: {:6.2f}%", k1, v1, k2, v2)
        << std::endl;
  };
  outFn("get", getPerSec, "success", getSuccessRate);
  outFn("set", setPerSec, "success", setSuccessRate);
  outFn("del", delPerSec, "found", delSuccessRate);
  if (update > 0) {
    outFn("update", updatePerSec, "success", updateSuccessRate);
  }
  if (addChained > 0) {
    outFn("addChained", addChainedPerSec, "success", addChainedSuccessRate);
  }
}

void ThroughputStats::render(uint64_t elapsedTimeNs,
                             folly::UserCounters& counters) const {
  const double elapsedSecs = elapsedTimeNs / static_cast<double>(1e9);

  const uint64_t setPerSec = util::narrow_cast<uint64_t>(set / elapsedSecs);
  const double setSuccessRate =
      set == 0 ? 0.0 : 100.0 * (set - setFailure) / set;

  const uint64_t getPerSec = util::narrow_cast<uint64_t>(get / elapsedSecs);
  const double getSuccessRate = get == 0 ? 0.0 : 100.0 * (get - getMiss) / get;

  const uint64_t delPerSec = util::narrow_cast<uint64_t>(del / elapsedSecs);
  const double delSuccessRate =
      del == 0 ? 0.0 : 100.0 * (del - delNotFound) / del;

  const uint64_t addChainedPerSec =
      util::narrow_cast<uint64_t>(addChained / elapsedSecs);
  const double addChainedSuccessRate =
      addChained == 0 ? 0.0
                      : 100.0 * (addChained - addChainedFailure) / addChained;

  counters["ops"] = ops;
  counters["total_sets"] = set;
  counters["get_per_sec"] = getPerSec;
  counters["get_suc_rate"] = util::narrow_cast<uint64_t>(getSuccessRate);
  counters["set_per_sec"] = setPerSec;
  counters["set_suc_rate"] = util::narrow_cast<uint64_t>(setSuccessRate);
  counters["del_per_sec"] = delPerSec;
  counters["del_suc_rate"] = util::narrow_cast<uint64_t>(delSuccessRate);
  counters["addChain_per_sec"] = addChainedPerSec;
  counters["addChain_suc_rate"] =
      util::narrow_cast<uint64_t>(addChainedSuccessRate);
}

namespace {
std::unique_ptr<GeneratorBase> makeGenerator(const StressorConfig& config) {
  if (config.generator == "piecewise-replay") {
    return std::make_unique<PieceWiseReplayGenerator>(config);
  } else if (config.generator == "replay") {
    return std::make_unique<ReplayGenerator>(config);
  } else if (config.generator.empty() || config.generator == "workload") {
    // TODO: Remove the empty() check once we label workload-based configs
    // properly
    return std::make_unique<WorkloadGenerator>(config);
  } else if (config.generator == "online") {
    return std::make_unique<OnlineGenerator>(config);

  } else {
    throw std::invalid_argument("Invalid config");
  }
}
} // namespace

std::unique_ptr<Stressor> Stressor::makeStressor(
    CacheConfig cacheConfig,
    StressorConfig stressorConfig,
    std::unique_ptr<StressorAdmPolicy> admPolicy) {
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
  } else {
    auto generator = makeGenerator(stressorConfig);
    if (cacheConfig.allocator == "LRU") {
      // default allocator is LRU, other allocator types should be added here
      return std::make_unique<CacheStressor<LruAllocator>>(
          cacheConfig, stressorConfig, std::move(generator),
          std::move(admPolicy));
    } else if (cacheConfig.allocator == "LRU2Q") {
      return std::make_unique<CacheStressor<Lru2QAllocator>>(
          cacheConfig, stressorConfig, std::move(generator),
          std::move(admPolicy));
    }
  }
  throw std::invalid_argument("Invalid config");
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
