#pragma once
#include <string>

#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
// independent worker that runs along side the stressor and tracks the
// progress by emitting the stats to an output stream and/or file.
class ProgressTracker final : public cachelib::PeriodicWorker {
 public:
  // @param s                  the stressor that is being tracked
  // @param detailedStatsFile  path to a file where detailed progress stats
  //                           and cache stats are dumped periodically in
  //                           addition to the stdout. If empty, this is
  //                           disabled.
  ProgressTracker(const Stressor& s, const std::string& detailedStatsFile);
  ~ProgressTracker() override;

 private:
  void work() override;

  const Stressor& stressor_; // stressor instance
  std::ofstream statsFile_;  // optional output file stream
  Stats prevStats_; // previous snapshot of cache stats to perform deltas.
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
