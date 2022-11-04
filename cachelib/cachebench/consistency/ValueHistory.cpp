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

#include "cachelib/cachebench/consistency/ValueHistory.h"

#include <folly/logging/xlog.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>

namespace facebook {
namespace cachelib {
namespace cachebench {
std::string EventInfo::formatTime() const {
  // Out format is fixed size. It needs 16 chars with terminating null. For
  // safety (if someone changes the strftime format) I made it double of it.
  char buf[32];
  size_t bufSize{sizeof buf};
  auto tt = std::chrono::system_clock::to_time_t(tp);
  struct tm tm_time;
  ::localtime_r(&tt, &tm_time);
  ::strftime(buf, bufSize, "%H:%M:%S", &tm_time);
  auto len = std::strlen(buf);
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      tp.time_since_epoch());
  std::snprintf(buf + len,
                bufSize - len,
                ".%06d",
                static_cast<int>(micros.count() % 1'000'000));
  return buf;
}

bool ValueHistory::isWriteEnd(Event e) {
  switch (e) {
  case Event::kEndMiss:
  case Event::kEndSet:
  case Event::kEndDelete:
    return true;
  default:
    return false;
  }
}

const char* ValueHistory::toString(Event e) {
  switch (e) {
  case Event::kBeginGet:
  case Event::kEndGet:
    return "GET";
  case Event::kBeginMiss:
  case Event::kEndMiss:
    return "MISS";
  case Event::kBeginSet:
  case Event::kEndSet:
    return "SET";
  case Event::kBeginDelete:
  case Event::kEndDelete:
    return "DELETE";
  }
  return nullptr;
}

bool ValueHistory::isEnd(Event e) {
  switch (e) {
  case Event::kEndGet:
  case Event::kEndMiss:
  case Event::kEndSet:
  case Event::kEndDelete:
    return true;
  default:
    return false;
  }
}

bool ValueHistory::hasValue(Event e) {
  switch (e) {
  case Event::kBeginSet:
  case Event::kEndGet:
  case Event::kEndSet:
    return true;
  default:
    return false;
  }
}

uint32_t ValueHistory::beginGet(EventInfo ei) {
  std::lock_guard<std::mutex> lock{mutex_};
  cleanupIfFull();
  LogRecord rec;
  rec.event = Event::kBeginGet;
  rec.setInfo(ei);
  // If for a record at index i: log_[i].other == i, then record doesn't have
  // link to other (yet).
  rec.other = log_.last();
  return log_.write(rec);
}

uint32_t ValueHistory::beginSet(EventInfo ei, uint64_t value) {
  std::lock_guard<std::mutex> lock{mutex_};
  cleanupIfFull();
  LogRecord rec;
  rec.event = Event::kBeginSet;
  rec.setInfo(ei);
  rec.other = log_.last();
  rec.value = value;
  openSetCount_++;
  return log_.write(rec);
}

uint32_t ValueHistory::beginDelete(EventInfo ei) {
  std::lock_guard<std::mutex> lock{mutex_};
  cleanupIfFull();
  LogRecord rec;
  rec.event = Event::kBeginDelete;
  rec.setInfo(ei);
  rec.other = log_.last();
  return log_.write(rec);
}

bool ValueHistory::endGet(EventInfo ei,
                          uint32_t beginIdx,
                          uint64_t value,
                          bool found,
                          EventStream* eventStream) {
  std::lock_guard<std::mutex> lock{mutex_};
  cleanupIfFull();
  auto begin = log_.getAt(beginIdx);
  XDCHECK_EQ(toInt(begin.event), toInt(Event::kBeginGet));

  LogRecord rec;
  rec.event = Event::kEndGet;
  rec.setInfo(ei);
  rec.other = beginIdx;
  if (found) {
    rec.value = value;
  }
  // Need to store end, because when we cleanup, we can't remove set with
  // overlapping gets that don't have end yet.
  const auto pos = log_.write(rec);

  // Update begin's link
  begin.other = pos;
  log_.setAt(beginIdx, begin);

  // TODO: If the log looks like ..., Gb[i], Ge[i] - remove these last two
  // log entries (if @data is not null, in which case we transform to delete).

  // Check consistency if we got data
  if (found) {
    return isConsistent(pos, eventStream);
  } else {
    // If we have a cache miss, it effectively means that we have a delete from
    // hisotry point of view (eviction in cache) when we check for consistency.
    // Convert to MISS.
    rec.event = Event::kEndMiss;
    log_.setAt(pos, rec);
    begin.event = Event::kBeginMiss;
    log_.setAt(beginIdx, begin);
    return true;
  }
}

void ValueHistory::endSet(EventInfo ei, uint32_t beginIdx) {
  std::lock_guard<std::mutex> lock{mutex_};
  cleanupIfFull();
  auto begin = log_.getAt(beginIdx);
  XDCHECK_EQ(toInt(begin.event), toInt(Event::kBeginSet));

  LogRecord rec;
  rec.event = Event::kEndSet;
  rec.setInfo(ei);
  rec.other = beginIdx;
  rec.value = begin.value; // Store value in the end set too
  const auto pos = log_.write(rec);

  // Update begin's link
  begin.other = pos;
  log_.setAt(beginIdx, begin);
  XDCHECK_GT(openSetCount_, 0u);
  openSetCount_--;
}

void ValueHistory::endDelete(EventInfo ei, uint32_t beginIdx) {
  std::lock_guard<std::mutex> lock{mutex_};
  cleanupIfFull();
  auto begin = log_.getAt(beginIdx);
  XDCHECK_EQ(toInt(begin.event), toInt(Event::kBeginDelete));

  LogRecord rec;
  rec.event = Event::kEndDelete;
  rec.setInfo(ei);
  rec.other = beginIdx;
  const auto pos = log_.write(rec);

  // Update begin's link
  begin.other = pos;
  log_.setAt(beginIdx, begin);
}

bool ValueHistory::isConsistent(uint64_t endGetIdx, EventStream* es) const {
  // Given the idea of what values are considered "possible", described in the
  // header, detection is done with a simple algorithm
  // ("b" for "begin", "e" for "end"):
  //   * Find first "S"et [Sb, Se] such that it is before "G"et [Gb, Ge] and
  //     doesn't intersect it (if any) - "last inorder set".
  //   * All sets, that overlap with [Sb, Ge] (note Sb and Ge) are candidates.
  //   * As we go, we should track sets that come in order. Latest of them
  //     overrides earlier. We track it with @setIdxMin. Only sets outside
  //     of the current get can override others.
  //
  // Delete from this perspective identical to set, but without value to check.
  //
  // We analyze history in reverse chronological order. First pass we find last
  // inorder set. Second pass find all intersection with it and check if @value
  // equals to one of them (with inorder overrides tracking).

  const auto recEndGet = log_.getAt(endGetIdx);
  XDCHECK_EQ(toInt(recEndGet.event), toInt(Event::kEndGet));
  const auto beginGetIdx = recEndGet.other;
  const auto beginSetIdx = findInOrderSetBegin(endGetIdx);
  auto setIdxMin = log_.first();
  uint32_t counter = 0;
  for (uint64_t i = endGetIdx;
       i > log_.first() && (i > beginSetIdx || counter < openSetCount_);) {
    i--;
    const auto rec = log_.getAt(i);
    if (rec.event == Event::kBeginSet && rec.other == i) {
      counter++;
    }
    // Check if this is a completed operation (doesn't matter if one of SETs,
    // or GET) and skip if it is older than in order SET.
    if (i != rec.other && i < setIdxMin) {
      continue;
    }
    // Check only sets, everything else can be ignored
    if (rec.event == Event::kBeginSet || rec.event == Event::kEndSet) {
      // Set events contain a value. Check it.
      if (rec.value == recEndGet.value) {
        return true;
      }
    } else if (i == log_.first() &&
               (rec.event == Event::kBeginGet || rec.event == Event::kEndGet)) {
      return true;
    }
    if (isWriteEnd(rec.event) && i < beginGetIdx) {
      // If SET begins outside of the GET, do not consider SETs before
      // @setIdxMin - they are older in order SETs.
      setIdxMin = std::max<uint32_t>(setIdxMin, rec.other);
    }
  }
  if (es != nullptr) {
    // Find a log record @kHistoryContext from @beginSetIdx, not beyond the
    // first. Do not subtract from @beginSetIdx - it is unsigned.
    const auto historyBeginIdx =
        std::max<uint64_t>(beginSetIdx, log_.first() + kHistoryContext) -
        kHistoryContext;
    const auto numEvents = endGetIdx - historyBeginIdx;
    for (uint32_t i = 0; i <= numEvents; i++) {
      auto rec = log_.getAt(endGetIdx - i);
      es->event(i,
                endGetIdx - rec.other,
                toString(rec.event),
                isEnd(rec.event),
                hasValue(rec.event),
                rec.value,
                rec.getInfo());
    }
  }
  return false;
}

uint64_t ValueHistory::findInOrderSetBegin(uint64_t endGetIdx) const {
  // At least get begin and end are in the log
  XDCHECK_GE(log_.size(), 2u);
  const auto beginGetIdx = log_.getAt(endGetIdx).other;
  for (uint64_t ri = beginGetIdx; ri-- > log_.first();) {
    const auto rec = log_.getAt(ri);
    if (isWriteEnd(rec.event)) {
      // Found the end of last inorder set. Return the beginning.
      return rec.other;
    }
  }
  // Last inorder set not found. It is normal - all sets in flight. But pay
  // attention, it may be a bug too (in history trimming). All log has possible
  // values.
  return log_.first();
}

void ValueHistory::cleanupIfFull() {
  // TODO: Check if cleanup can lead to false detections
  if (log_.size() == kCapacity) {
    (void)log_.read();
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
