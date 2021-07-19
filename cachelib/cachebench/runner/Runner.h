#pragma once

#include <folly/Benchmark.h>

#include <string>

#include "cachelib/cachebench/runner/ProgressTracker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class Runner {
 public:
  // @param config                the configuration for the cachebench run. This
  //                              contains both the stressor configuration and
  //                              the cache configuration.
  // @param admPolicy             the implementation of the admission policy
  //                              for cache
  Runner(const CacheBenchConfig& config,
         std::unique_ptr<StressorAdmPolicy> admPolicy);

  // @param progressInterval    the interval at which periodic progress of the
  //                            benchmark run is reported/tracked.
  // @param progressStatsFile   the file to log periodic stats and progress
  //                            to in addition to stdtout. Ignored if empty
  // @return true if the run was successful, false if there is a failure.
  bool run(std::chrono::seconds progressInterval,
           const std::string& progressStatsFile);

  // for testings using folly::Benchmark
  // in addition to running time, cachebench has several metrics
  // (hit rate, throughput, ect.) to be compared, use BENCHMARK_COUNTER
  // and put metrics into folly::UserCounters to show metrics in output results.
  void run(folly::UserCounters&);

  void abort() { stressor_->abort(); }

 private:
  // instance of the stressor.
  std::unique_ptr<Stressor> stressor_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
