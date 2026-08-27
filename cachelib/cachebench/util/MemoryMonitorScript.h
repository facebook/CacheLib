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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace facebook::cachelib::cachebench {

struct MemoryMonitorScriptPhase {
  uint64_t valueGB{0};
  uint64_t durationMs{0};

  bool operator==(const MemoryMonitorScriptPhase&) const = default;
};

class MemoryMonitorScript {
 public:
  enum class Target { MemAvailable, RSS };

  using Clock = std::function<std::chrono::steady_clock::time_point()>;

  explicit MemoryMonitorScript(
      const std::vector<MemoryMonitorScriptPhase>& phases,
      std::chrono::milliseconds pollInterval,
      bool repeat = false,
      Clock clock = std::chrono::steady_clock::now);

  MemoryMonitorScript(const MemoryMonitorScript&) = delete;
  MemoryMonitorScript& operator=(const MemoryMonitorScript&) = delete;
  MemoryMonitorScript(MemoryMonitorScript&&) = delete;
  MemoryMonitorScript& operator=(MemoryMonitorScript&&) = delete;
  ~MemoryMonitorScript();

  // Installs this script as the process-wide provider for the selected metric.
  void install(Target target);

  // A non-repeating script continues returning its final phase value.
  size_t getCurrentValueBytes() const;

 private:
  struct Phase {
    uint64_t valueGB{0};
    uint64_t durationMs{0};
  };

  static size_t getActiveValueBytes();
  void uninstall();

  std::vector<Phase> phases_;
  uint64_t totalDurationMs_{0};
  const bool repeat_{false};
  Clock clock_;
  std::chrono::steady_clock::time_point startTime_;
  Target target_{Target::MemAvailable};
};

} // namespace facebook::cachelib::cachebench
