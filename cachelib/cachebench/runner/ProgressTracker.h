#pragma once
#include <string>

#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
class ProgressTracker final : public cachelib::PeriodicWorker {
 public:
  ProgressTracker(const Stressor& s, const std::string& detailedStatsFile);
  ~ProgressTracker() override;

 private:
  void work() override { reportProgress(); }
  void reportProgress();

  const Stressor& stressor_;
  std::ofstream statsFile_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
