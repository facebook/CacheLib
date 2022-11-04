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

#include "cachelib/cachebench/consistency/ValueTracker.h"

#include <glog/logging.h>

#include <limits>

namespace facebook {
namespace cachelib {
namespace cachebench {
std::vector<folly::StringPiece> ValueTracker::wrapStrings(
    const std::vector<std::string>& v) {
  std::vector<folly::StringPiece> rv;
  rv.reserve(v.size());
  for (const auto& s : v) {
    rv.push_back(folly::StringPiece{s.data(), s.size()});
  }
  return rv;
}

ValueTracker::ValueTracker(std::vector<folly::StringPiece> keys)
    : trackers_(keys.size()) {
  if (keys.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::invalid_argument("too many keys");
  }
  for (uint32_t i = 0; i < keys.size(); i++) {
    keyTrackerMap_.emplace(keys[i], i);
  }
}

ValueTracker::Index ValueTracker::beginGet(folly::StringPiece key) {
  auto ti = getTrackerIndexOrThrow(key);
  return Index{ti, trackers_[ti].beginGet(eventInfo())};
}

ValueTracker::Index ValueTracker::beginSet(folly::StringPiece key,
                                           uint64_t value) {
  auto ti = getTrackerIndexOrThrow(key);
  return Index{ti, trackers_[ti].beginSet(eventInfo(), value)};
}

ValueTracker::Index ValueTracker::beginDelete(folly::StringPiece key) {
  auto ti = getTrackerIndexOrThrow(key);
  return Index{ti, trackers_[ti].beginDelete(eventInfo())};
}

bool ValueTracker::endGet(Index beginIdx,
                          uint64_t data,
                          bool found,
                          EventStream* es) {
  return trackers_[beginIdx.trackerIndex].endGet(
      eventInfo(), beginIdx.historyIndex, data, found, es);
}

void ValueTracker::endSet(Index beginIdx) {
  trackers_[beginIdx.trackerIndex].endSet(eventInfo(), beginIdx.historyIndex);
}

void ValueTracker::endDelete(Index beginIdx) {
  trackers_[beginIdx.trackerIndex].endDelete(eventInfo(),
                                             beginIdx.historyIndex);
}

void ValueTracker::evicted(folly::StringPiece /* key */) {
  // TODO: Implement
}

ValueTracker::TrackerIndexT ValueTracker::getTrackerIndexOrThrow(
    folly::StringPiece key) const {
  auto it = keyTrackerMap_.find(key);
  if (it == keyTrackerMap_.end()) {
    throw std::logic_error("unknown key");
  }
  return it->second;
}

EventInfo ValueTracker::eventInfo() const {
  EventInfo info;
  info.tid = shortTids_.getShort(std::this_thread::get_id());
  return info;
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
