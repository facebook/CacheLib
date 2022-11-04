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
#include "cachelib/cachebench/consistency/ValueHistory.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class LogEventStream : public EventStream {
 public:
  ~LogEventStream() override = default;

  void event(uint32_t index,
             uint32_t other,
             const char* eventName,
             bool end,
             bool hasValue,
             uint64_t value,
             EventInfo info) override;

  std::string format() const;

 private:
  static constexpr uint32_t kMaxReport{30};

  struct Event {
    const char* eventName{nullptr};
    uint32_t other{};
    bool end{false};
    bool hasValue{false};
    uint64_t value{};
    EventInfo info;
  };

  Event events_[kMaxReport];
  uint32_t count_{};
  bool overflow_{false};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
