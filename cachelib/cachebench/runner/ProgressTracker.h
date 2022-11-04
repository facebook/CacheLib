/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
