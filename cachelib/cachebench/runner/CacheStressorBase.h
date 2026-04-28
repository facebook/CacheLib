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

#include "cachelib/cachebench/runner/Stressor.h"
#include "cachelib/cachebench/workload/GeneratorBase.h"

namespace facebook::cachelib::cachebench {

class CacheStressorBase : public Stressor {
 public:
  CacheStressorBase(StressorConfig config,
                    std::unique_ptr<GeneratorBase>&& generator);

  ~CacheStressorBase() override;

  // Block until all stress workers are finished.
  void finish() override;

  // abort the stress run by indicating to the workload generator and
  // delegating to the base class abort() to stop the test.
  void abort() override;

  // obtain aggregated throughput stats for the stress run so far.
  ThroughputStats aggregateThroughputStats() const override;

  void renderWorkloadGeneratorStats(uint64_t elapsedTimeNs,
                                    std::ostream& out) const override;

  void renderWorkloadGeneratorStats(
      uint64_t elapsedTimeNs, folly::UserCounters& counters) const override;

  uint64_t getTestDurationNs() const override;

 protected:
  const StressorConfig config_; // config for the stress run

  std::vector<ThroughputStats> throughputStats_; // thread local stats

  std::unique_ptr<GeneratorBase> wg_; // workload generator

  // Main stressor thread
  std::thread stressWorker_;

  // string used for generating random payloads
  const std::string hardcodedString_;

  // TODO make all timing stuff private?
  // Mutex to protect reading timestamps
  mutable std::mutex timeMutex_;

  // Start time for the stress test
  std::chrono::time_point<std::chrono::system_clock> startTime_;

  // End time for the stress test
  std::chrono::time_point<std::chrono::system_clock> endTime_;

  void setStartTime();
  void setEndTime();

 private:
  static std::string genHardcodedString();
};

} // namespace facebook::cachelib::cachebench
