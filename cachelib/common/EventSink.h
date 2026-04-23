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

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>

#include <deque>
#include <fstream>
#include <magic_enum/magic_enum.hpp>
#include <mutex>
#include <stdexcept>
#include <variant>
#include <vector>

#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/EventInterface.h"

namespace facebook::cachelib {

// Enum for EventInfo fields to avoid string comparisons when selecting columns
enum class EventInfoField {
  Ts,
  Event,
  Result,
  Key,
  Size,
  ExpiryTime,
  TimeToExpire,
  TtlSecs,
  AllocSize,
  PoolId,
  App,
  Usecase,
  UsecaseId,
  AppId,
};

// Convert EventInfoField to header name string using magic_enum
inline std::string_view toHeaderName(EventInfoField field) {
  return magic_enum::enum_name(field);
}

// Default fields to include if none specified
inline const std::vector<EventInfoField>& getDefaultEventInfoFields() {
  static const std::vector<EventInfoField> defaults = {
      EventInfoField::Ts,
      EventInfoField::Event,
      EventInfoField::Result,
      EventInfoField::Key,
  };
  return defaults;
}

struct EventInfo {
  // Use std::string for owned fields (app, usecase), and folly::StringPiece for
  // cacheName, which is managed by the allocator and guaranteed to outlive
  // EventInfo.

  // Default constructor marked noexcept for LockFreeRingBuffer compatibility
  EventInfo() noexcept = default;

  // Copy and move constructors marked noexcept for folly::MPMCQueue
  // compatibility
  EventInfo(const EventInfo&) noexcept = default;
  EventInfo(EventInfo&&) noexcept = default;
  EventInfo& operator=(const EventInfo&) noexcept = default;
  EventInfo& operator=(EventInfo&&) noexcept = default;

  folly::StringPiece cacheName; // Non-owning reference to cache name.
  time_t eventTimestamp{0};     // Timestamp of this event.
  AllocatorApiEvent event{AllocatorApiEvent::INVALID};   // Type of the event.
  AllocatorApiResult result{AllocatorApiResult::FAILED}; // Result of the event.
  std::string key;                                       // The item's key.
  folly::Optional<size_t> size;                          // Size of the value.
  folly::Optional<time_t> expiryTime;     // Expiry time of the item.
  folly::Optional<uint32_t> timeToExpire; // Time to expire in seconds (0 if
                                          // expired).
  folly::Optional<uint32_t> ttlSecs;      // User assigned TTL.
  folly::Optional<uint32_t> allocSize;    // Allocation class size
  folly::Optional<PoolId> poolId;         // The item's pool ID.
  folly::Optional<std::string> app;       // The item's application.
  folly::Optional<std::string> usecase;   // The item's usecase.
  folly::Optional<uint32_t> usecaseId;    // The item's usecase id.
  folly::Optional<uint32_t> appId;        // The item's app id.
  folly::Optional<
      folly::F14FastMap<std::string, std::variant<std::string, int64_t>>>
      metaInfo; // Map with additional metadata.
};

// Abstract event sink interface to allow different types of event log storage.
class EventSink {
 public:
  virtual ~EventSink() = default;
  virtual void recordEvent(const EventInfo& eventInfo) = 0;
};

// Trivial in-memory event sink for testing.
class InMemoryEventSink : public EventSink {
 public:
  void recordEvent(const EventInfo& eventInfo) override {
    records_.wlock()->push_back(eventInfo);
  }

  std::vector<EventInfo> getRecords() const { return *records_.rlock(); }

 private:
  folly::Synchronized<std::vector<EventInfo>> records_;
};

class FifoEventSink : public EventSink {
 public:
  explicit FifoEventSink(size_t capacity) : capacity_(capacity) {}

  void recordEvent(const EventInfo& eventInfo) override {
    records_.withWLock([&](auto& deque) {
      deque.push_back(eventInfo);
      if (deque.size() > capacity_) {
        deque.pop_front();
      }
    });
  }

  std::vector<EventInfo> getRecords() const {
    return records_.withRLock([](const auto& deque) {
      return std::vector<EventInfo>(deque.begin(), deque.end());
    });
  }

  // Iterate over the records and call func on each record.
  template <typename Func>
  void forEachRecord(Func&& func) const {
    records_.withRLock([&](const auto& deque) {
      for (const auto& event : deque) {
        func(event);
      }
    });
  }

  size_t size() const {
    return records_.withRLock([](const auto& deque) { return deque.size(); });
  }

  size_t capacity() const { return capacity_; }

 private:
  folly::Synchronized<std::deque<EventInfo>> records_;
  size_t capacity_;
};

// EventSink that writes events to a file in CSV format.
class FileEventSink : public EventSink {
 public:
  // Constructor with enum-based fields (preferred - avoids string comparisons)
  explicit FileEventSink(
      const std::string& filename,
      std::vector<EventInfoField> fields = getDefaultEventInfoFields())
      : file_(filename, std::ios::out | std::ios::trunc),
        fields_(std::move(fields)) {
    if (!file_.is_open()) {
      throw std::runtime_error("Failed to open event log file: " + filename);
    }
    writeHeader();
  }

  ~FileEventSink() override {
    if (file_.is_open()) {
      file_.flush();
      file_.close();
    }
  }

  FileEventSink(const FileEventSink&) = delete;
  FileEventSink& operator=(const FileEventSink&) = delete;

  FileEventSink(FileEventSink&&) = delete;
  FileEventSink& operator=(FileEventSink&&) = delete;

  void recordEvent(const EventInfo& eventInfo) override {
    std::lock_guard<std::mutex> lock(mutex_);
    writeEvent(eventInfo);
  }

 private:
  std::ofstream file_;
  std::vector<EventInfoField> fields_;
  mutable std::mutex mutex_;

  void writeHeader() {
    bool first = true;
    for (const auto& field : fields_) {
      if (!first) {
        file_ << ",";
      }
      file_ << toHeaderName(field);
      first = false;
    }
    file_ << std::endl;
    file_.flush();
  }

  template <typename T>
  void writeOptionalField(const folly::Optional<T>& field) {
    field.has_value() ? (file_ << *field) : (file_ << "null");
  }

  void writeEvent(const EventInfo& eventInfo) {
    bool first = true;
    for (const auto& field : fields_) {
      if (!first) {
        file_ << ",";
      }
      first = false;
      switch (field) {
      case EventInfoField::Ts:
        file_ << eventInfo.eventTimestamp;
        break;
      case EventInfoField::Event:
        file_ << magic_enum::enum_name(eventInfo.event);
        break;
      case EventInfoField::Result:
        file_ << magic_enum::enum_name(eventInfo.result);
        break;
      case EventInfoField::Key:
        file_ << eventInfo.key;
        break;
      case EventInfoField::Size:
        writeOptionalField(eventInfo.size);
        break;
      case EventInfoField::ExpiryTime:
        writeOptionalField(eventInfo.expiryTime);
        break;
      case EventInfoField::TimeToExpire:
        writeOptionalField(eventInfo.timeToExpire);
        break;
      case EventInfoField::TtlSecs:
        writeOptionalField(eventInfo.ttlSecs);
        break;
      case EventInfoField::AllocSize:
        writeOptionalField(eventInfo.allocSize);
        break;
      case EventInfoField::PoolId:
        writeOptionalField(eventInfo.poolId);
        break;
      case EventInfoField::App:
        writeOptionalField(eventInfo.app);
        break;
      case EventInfoField::Usecase:
        writeOptionalField(eventInfo.usecase);
        break;
      case EventInfoField::UsecaseId:
        writeOptionalField(eventInfo.usecaseId);
        break;
      case EventInfoField::AppId:
        writeOptionalField(eventInfo.appId);
        break;
      default:
        XLOG(WARN) << "Unknown field: " << toHeaderName(field);
        break;
      }
    }
    file_ << std::endl;
  }
};

} // namespace facebook::cachelib
