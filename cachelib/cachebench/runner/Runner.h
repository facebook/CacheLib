#pragma once

#include <string>

#include "cachelib/cachebench/runner/ProgressTracker.h"
#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
class Runner {
 public:
  explicit Runner(const std::string& configPath,
                  const std::string& progressStatsFile,
                  uint64_t progressInterval);
  bool run();

  void abort() { stressor_->abort(); }

 private:
  CacheBenchConfig config_;
  const std::string& progressStatsFile_;
  const uint64_t progressInterval_;

  std::unique_ptr<Stressor> stressor_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
