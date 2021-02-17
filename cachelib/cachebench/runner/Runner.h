#pragma once

#include <string>

#include "cachelib/cachebench/runner/ProgressTracker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
// User can pass in a config customizer to add additional settings at runtime
using CacheConfigCustomizer = std::function<CacheConfig(CacheConfig)>;

class Runner {
 public:
  // @customizeCacheConfig    User can implement special handling to add custom
  //                          cache configs they desire on each run
  Runner(const CacheBenchConfig& config,
         const std::string& progressStatsFile,
         uint64_t progressInterval,
         CacheConfigCustomizer customizeCacheConfig,
         std::unique_ptr<StressorAdmPolicy> admPolicy);
  bool run();

  void abort() { stressor_->abort(); }

 private:
  CacheBenchConfig config_;
  CacheConfigCustomizer customizeCacheConfig_;

  const std::string& progressStatsFile_;
  const uint64_t progressInterval_;

  std::unique_ptr<Stressor> stressor_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
