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

#include "cachelib/cachebench/runner/CacheStressorBase.h"

namespace facebook::cachelib::cachebench {

CacheStressorBase::CacheStressorBase(StressorConfig config,
                                     std::unique_ptr<GeneratorBase>&& generator)
    : config_(std::move(config)),
      throughputStats_(config_.numThreads),
      wg_(std::move(generator)),
      hardcodedString_(genHardcodedString()),
      endTime_(std::chrono::system_clock::time_point::max()) {}

CacheStressorBase::~CacheStressorBase() { finish(); }

void CacheStressorBase::finish() {
  if (stressWorker_.joinable()) {
    stressWorker_.join();
  }
  wg_->markShutdown();
}

void CacheStressorBase::abort() {
  wg_->markShutdown();
  Stressor::abort();
}

ThroughputStats CacheStressorBase::aggregateThroughputStats() const {
  ThroughputStats res{};
  for (const auto& stats : throughputStats_) {
    res += stats;
  }

  return res;
}

void CacheStressorBase::renderWorkloadGeneratorStats(uint64_t elapsedTimeNs,
                                                     std::ostream& out) const {
  wg_->renderStats(elapsedTimeNs, out);
}

void CacheStressorBase::renderWorkloadGeneratorStats(
    uint64_t elapsedTimeNs, folly::UserCounters& counters) const {
  wg_->renderStats(elapsedTimeNs, counters);
}

uint64_t CacheStressorBase::getTestDurationNs() const {
  std::lock_guard<std::mutex> l(timeMutex_);
  return std::chrono::nanoseconds{
      std::min(std::chrono::system_clock::now(), endTime_) - startTime_}
      .count();
}

void CacheStressorBase::setStartTime() {
  std::lock_guard<std::mutex> l(timeMutex_);
  startTime_ = std::chrono::system_clock::now();
}

void CacheStressorBase::setEndTime() {
  std::lock_guard<std::mutex> l(timeMutex_);
  endTime_ = std::chrono::system_clock::now();
}

/* static */ std::string CacheStressorBase::genHardcodedString() {
  const std::string s = "The quick brown fox jumps over the lazy dog. ";
  std::string val;
  for (int i = 0; i < 4 * 1024 * 1024; i += s.size()) {
    val += s;
  }
  return val;
}

} // namespace facebook::cachelib::cachebench
