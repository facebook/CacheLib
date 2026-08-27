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

#include "cachelib/cachebench/util/MemoryMonitorScript.h"

#include <folly/Synchronized.h>

#include <limits>
#include <stdexcept>
#include <utility>

#include "cachelib/common/Utils.h"

namespace facebook::cachelib::cachebench {

namespace {
constexpr size_t kGBytes = 1024ULL * 1024ULL * 1024ULL;

folly::Synchronized<MemoryMonitorScript*>& activeScript() {
  static folly::Synchronized<MemoryMonitorScript*> script{nullptr};
  return script;
}
} // namespace

MemoryMonitorScript::MemoryMonitorScript(
    const std::vector<MemoryMonitorScriptPhase>& phases,
    std::chrono::milliseconds pollInterval,
    bool repeat,
    Clock clock)
    : repeat_(repeat), clock_(std::move(clock)), startTime_(clock_()) {
  if (phases.empty()) {
    throw std::invalid_argument(
        "memory monitor script must contain at least one phase");
  }
  if (pollInterval.count() <= 0) {
    throw std::invalid_argument(
        "memory monitor poll interval must be greater than zero");
  }

  phases_.reserve(phases.size());
  for (const auto& phase : phases) {
    if (phase.valueGB == 0) {
      throw std::invalid_argument(
          "memory monitor script value must be greater than zero");
    }
    if (phase.durationMs < static_cast<uint64_t>(pollInterval.count())) {
      throw std::invalid_argument(
          "memory monitor script duration must be at least one poll interval");
    }
    if (phase.valueGB > std::numeric_limits<size_t>::max() / kGBytes) {
      throw std::invalid_argument("memory monitor script value is too large");
    }
    if (phase.durationMs >
        std::numeric_limits<uint64_t>::max() - totalDurationMs_) {
      throw std::invalid_argument(
          "memory monitor script duration is too large");
    }
    phases_.push_back(Phase{phase.valueGB, phase.durationMs});
    totalDurationMs_ += phase.durationMs;
  }
}

MemoryMonitorScript::~MemoryMonitorScript() { uninstall(); }

void MemoryMonitorScript::install(Target target) {
  auto script = activeScript().wlock();
  if (*script != nullptr) {
    throw std::logic_error("a memory monitor script is already installed");
  }
  *script = this;
  target_ = target;
  startTime_ = clock_();

  if (target == Target::RSS) {
    util::setRSSMemoryAdvising(getActiveValueBytes);
  } else {
    util::setCgroupMemoryAdvising(getActiveValueBytes);
  }
}

void MemoryMonitorScript::uninstall() {
  auto script = activeScript().wlock();
  if (*script != this) {
    return;
  }

  if (target_ == Target::RSS) {
    util::setRSSMemoryAdvising(nullptr);
  } else {
    util::setCgroupMemoryAdvising(nullptr);
  }

  *script = nullptr;
}

size_t MemoryMonitorScript::getActiveValueBytes() {
  auto script = activeScript().rlock();
  return *script == nullptr ? 0 : (*script)->getCurrentValueBytes();
}

size_t MemoryMonitorScript::getCurrentValueBytes() const {
  const auto now = clock_();
  const auto elapsed =
      now > startTime_ ? std::chrono::duration_cast<std::chrono::milliseconds>(
                             now - startTime_)
                             .count()
                       : 0;
  auto elapsedMs = static_cast<uint64_t>(elapsed);
  if (repeat_) {
    elapsedMs %= totalDurationMs_;
  }

  uint64_t phaseEndMs = 0;
  for (const auto& phase : phases_) {
    phaseEndMs += phase.durationMs;
    if (elapsedMs < phaseEndMs) {
      return phase.valueGB * kGBytes;
    }
  }

  return phases_.back().valueGB * kGBytes;
}

} // namespace facebook::cachelib::cachebench
