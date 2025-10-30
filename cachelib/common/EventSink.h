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
#include <mutex>
#include <stdexcept>
#include <variant>
#include <vector>

#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/EventInterface.h"

namespace facebook::cachelib {

struct EventInfo {
  // Use std::string for owned fields (app, usecase), and folly::StringPiece for
  // cacheName, which is managed by the allocator and guaranteed to outlive
  // EventInfo.

  // Default constructor marked noexcept for LockFreeRingBuffer compatibility
  EventInfo() noexcept = default;

  folly::StringPiece cacheName; // Non-owning reference to cache name.
  time_t eventTimestamp{0};     // Timestamp of this event.
  AllocatorApiEvent event{AllocatorApiEvent::INVALID};   // Type of the event.
  AllocatorApiResult result{AllocatorApiResult::FAILED}; // Result of the event.
  std::string key;                                       // The item's key.
  folly::Optional<size_t> size;                          // Size of the value.
  folly::Optional<time_t> expiryTime;   // Expiry time of the item.
  folly::Optional<uint32_t> allocSize;  // Allocation class size
  folly::Optional<PoolId> poolId;       // The item's pool ID.
  folly::Optional<std::string> app;     // The item's application.
  folly::Optional<std::string> usecase; // The item's usecase.
  folly::Optional<uint32_t> usecaseId;  // The item's usecase id.
  folly::Optional<uint32_t> appId;      // The item's app id.
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
  static constexpr const char* kBaseHeader = "ts,event,result,key";
  // additionalHeaders: vector of extra header names to look for in meta_info
  explicit FileEventSink(const std::string& filename,
                         std::vector<std::string> additionalHeaders = {})
      : file_(filename, std::ios::out | std::ios::trunc),
        additionalHeaders_(std::move(additionalHeaders)) {
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
  std::vector<std::string> additionalHeaders_;
  mutable std::mutex mutex_;

  void writeHeader() {
    // Basic headers
    file_ << kBaseHeader;
    for (const auto& h : additionalHeaders_) {
      file_ << "," << h;
    }
    file_ << std::endl;
    file_.flush();
  }

  void writeEvent(const EventInfo& eventInfo) {
    file_ << eventInfo.eventTimestamp << "," << toString(eventInfo.event) << ","
          << toString(eventInfo.result) << "," << eventInfo.key;

    for (const auto& h : additionalHeaders_) {
      file_ << ",";
      if (eventInfo.metaInfo.has_value()) {
        auto it = eventInfo.metaInfo->find(h);
        if (it != eventInfo.metaInfo->end()) {
          if (std::holds_alternative<std::string>(it->second)) {
            file_ << std::get<std::string>(it->second);
          } else {
            file_ << std::get<int64_t>(it->second);
          }
        } else {
          file_ << "null";
        }
      } else {
        file_ << "null";
      }
    }
    file_ << std::endl;
  }
};

} // namespace facebook::cachelib
