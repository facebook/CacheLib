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

#include "cachelib/cachebench/consistency/LogEventStream.h"

#include <folly/Conv.h>
#include <folly/Format.h>
#include <folly/Range.h>

namespace facebook {
namespace cachelib {
namespace cachebench {

void LogEventStream::event(uint32_t index,
                           uint32_t other,
                           const char* eventName,
                           bool end,
                           bool hasValue,
                           uint64_t value,
                           EventInfo info) {
  if (index < kMaxReport) {
    events_[index].eventName = eventName;
    events_[index].other = other;
    events_[index].end = end;
    events_[index].hasValue = hasValue;
    events_[index].value = value;
    events_[index].info = info;
    count_ = index + 1;
  } else {
    overflow_ = true;
  }
}

std::string LogEventStream::format() const {
  std::string rv;
  rv.append("Inconsistency detected:\n");
  constexpr folly::StringPiece kFormat = "{:<8}{:<8}{:<18}{:<12}{:<12}{}\n";
  rv.append(folly::sformat(kFormat, "Index", "Other", "Time", "Thread ID",
                           "Operation", "Value"));
  for (uint32_t i = 0; i < count_; i++) {
    const auto& e = events_[i];
    std::string en{e.eventName};
    if (e.end) {
      en.append(" end");
    }
    rv.append(
        folly::sformat(kFormat, i, e.other, e.info.formatTime(), e.info.tid, en,
                       e.hasValue ? folly::to<std::string>(e.value) : ""));
  }
  if (overflow_) {
    rv.append("... more\n");
  }
  return rv;
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
