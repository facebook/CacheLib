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
#include <mutex>
#include <string>

#include "cachelib/cachebench/consistency/RingBuffer.h"
#include "cachelib/cachebench/consistency/ShortThreadId.h"

/**
 * Value History is a general cache consistency test. It tracks Get/Set/Delete
 * operation to one cache key and report user visible inconsistency if any.
 * Users have to ensure all Get, Set and Delete are logged like CacheWrapper
 * did.
 *
 * Value History chronologically inserts each operation begin and end into a
 * circular buffer. An operation could take effect in any time point between its
 * begin and end. But it must have taken effect after its end and haven't taken
 * effect before its begin. Particulary, if two operations overlap in time,
 * both values possible.
 *
 * Upon a cache hit (a Get end with not null value), Value History will check
 * whether the return value makes sense by looking into operation history. The
 * return value must be either 1) the value of the last operation which took
 * effect before this Get or 2) the values being set during this Get.
 *
 * We call case 1) as last value. There are multiple possible last values. For
 * example,
 *  Set Begin 1
 *  Set Begin 2
 *  Set End   1
 *  Set Begin 3
 *  Set End   3
 *  Set Begin 4
 *  Set End   2
 *  Get Begin
 *  Set End   4
 *  Set Begin 5
 *  Set End   5
 *  Get End   ?
 *
 * Both value 2 and 3 could be the last value to the Get. Although Set End 3 is
 * before Set Begin 4, Set End 4 is after Get Begin, thus value 3 still could be
 * the last value. Value 4 could belong to both case 1) and case 2), but we
 * count it towards case 2). Value 1 couldn't be the last value because Set End
 * 1 is before Set Begin 3, so it must be overridden by value 3.
 *
 * We call case 2) as inflight writes. Value 4 and Value 5 in the above example
 * are inflight writes.
 *
 * Formally, following is a recursive definition of "possible" value:
 *   - If SET is latest before GET and doesn't overlap with GET
 *        => SET is possible,
 *
 *   - If SET overlaps with GET
 *        => SET is possible,
 *
 *   - If SET[e] < SET'[b]
 *        => SET is NOT possible (no matter if SET' possible or not)
 *
 *   - If SET' is possible and SET overlaps with SET'
 *        => SET is possible
 */

namespace facebook {
namespace cachelib {
namespace cachebench {
struct EventInfo {
  using TimePoint = std::chrono::system_clock::time_point;

  EventInfo() : tp{std::chrono::system_clock::now()} {}
  EventInfo(ShortThreadId _tid, TimePoint _tp) : tid{_tid}, tp{_tp} {}

  std::string formatTime() const;

  ShortThreadId tid{};
  TimePoint tp{};
};

// When inconsistency detected, @event method called for every event in the
// chain containing inconsistency. Note that @event is called under the lock
// and keep is cheap.
class EventStream {
 public:
  virtual ~EventStream() = default;

  // @index     Reverse index of event (latest event has index 0)
  // @other     Complement event reverse index (begin->end, end->begin)
  // @eventName Event name as string (one of "GET", "SET", "DELETE", "MISS")
  // @end       True if event ends, false if begins
  // @hasValue  True if @value has meaningful value
  // @value     Value assosiated with event (only for "SET" and "GET" end)
  // @info      Common data (thread ID, time point)
  virtual void event(uint32_t index,
                     uint32_t other,
                     const char* eventName,
                     bool end,
                     bool hasValue,
                     uint64_t value,
                     EventInfo info) = 0;
};

// TODO: Cleanup history
// TODO: Log evictions
class ValueHistory {
 public:
  static constexpr uint32_t kCapacity{480};
  // Number of events to print beyond the minimal history that reveals
  // inconsistency.
  static constexpr uint32_t kHistoryContext{10};

  ValueHistory() {}
  ValueHistory(const ValueHistory&) = delete;
  ValueHistory& operator=(const ValueHistory&) = delete;

  // beginXXX returns new log record index in the log
  uint32_t beginGet(EventInfo ei);
  // Logs SET begin and generates value into the buffer @data
  uint32_t beginSet(EventInfo ei, uint64_t value);
  uint32_t beginDelete(EventInfo ei);

  // Returns false if provided value is inconsistent with the history. Value
  // generated in @beginGet. User shouldn't modify it.
  // If @eventStream is not null, reports events triggered detection.
  bool endGet(EventInfo ei,
              uint32_t beginIdx,
              uint64_t value,
              bool found,
              EventStream* es = nullptr);
  void endSet(EventInfo ei, uint32_t beginIdx);
  void endDelete(EventInfo ei, uint32_t beginIdx);

 private:
  enum class Event : uint8_t {
    kBeginGet,
    kBeginMiss,
    kBeginSet,
    kBeginDelete,
    kEndGet,
    kEndMiss,
    kEndSet,
    kEndDelete,
  };

  struct LogRecord {
    Event event{Event::kBeginGet};

    // Store @EventInfo inlined for better packing and smaller size
    ShortThreadId tid;
    EventInfo::TimePoint tp;

    // Index of "other" end of the operation int the ring buffer (end for
    // begin, begin for end).
    uint32_t other{};

    // Optional generated value for some operations (SET and GET end)
    uint64_t value{};

    void setInfo(EventInfo info) {
      tid = info.tid;
      tp = info.tp;
    }

    EventInfo getInfo() const { return {tid, tp}; }
  };

  static uint8_t toInt(Event e) { return static_cast<uint8_t>(e); }
  static const char* toString(Event e);
  static bool hasValue(Event e);
  static bool isWriteEnd(Event e);
  static bool isEnd(Event e);

  // Checks if GET with end in position @endGetIdx is consistent with history
  bool isConsistent(uint64_t endGetIdx, EventStream* es) const;
  uint64_t findInOrderSetBegin(uint64_t endGetIdx) const;
  void cleanupIfFull();

  mutable std::mutex mutex_;
  RingBuffer<LogRecord, kCapacity> log_;
  // Tracks number of not finished SET ops ("open")
  uint32_t openSetCount_{0};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook
