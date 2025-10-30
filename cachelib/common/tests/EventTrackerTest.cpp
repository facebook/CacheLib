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

#include <folly/testing/TestUtil.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <numeric>

#include "cachelib/common/EventTracker.h"

using namespace ::testing;
using namespace facebook::cachelib;

class EventTrackerTest : public ::testing::Test {
 protected:
  std::vector<EventInfo> createEvents(const char* key, uint32_t numItems) {
    std::vector<EventInfo> events;
    for (uint64_t i = 0; i < numItems; i++) {
      EventInfo eventInfo;
      eventInfo.eventTimestamp = i;
      eventInfo.key = folly::sformat("{}-{}", key, i);
      if (i % 2 == 0) {
        eventInfo.event = AllocatorApiEvent::FIND;
        eventInfo.result = (i % 2 == 0) ? AllocatorApiResult::NOT_FOUND
                                        : AllocatorApiResult::FOUND;
      } else {
        eventInfo.event = AllocatorApiEvent::INSERT;
        eventInfo.result = AllocatorApiResult::INSERTED;
      }
      events.push_back(eventInfo);
    }
    return events;
  }

  std::vector<std::vector<std::string>> readCsvRows(
      const folly::test::TemporaryFile& tmpFile) {
    std::ifstream fileStream(tmpFile.path().string());
    if (!fileStream.is_open()) {
      throw std::runtime_error("Failed to open file stream");
    }

    std::vector<std::vector<std::string>> csvRows;
    std::string line;
    while (std::getline(fileStream, line)) {
      if (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.pop_back();
      }
      std::vector<std::string> tokens;
      std::stringstream ss(line);
      std::string token;
      while (std::getline(ss, token, ',')) {
        tokens.push_back(token);
      }
      csvRows.push_back(tokens);
    }
    return csvRows;
  }

  bool compareCsvRowWithEvent(const std::vector<std::string>& row,
                              const EventInfo& event) {
    if (row.size() < 4) {
      return false;
    }
    return row.at(0) == std::to_string(event.eventTimestamp) &&
           row.at(1) == toString(event.event) &&
           row.at(2) == toString(event.result) && row.at(3) == event.key;
  }

  void checkCsvHeader(const std::vector<std::vector<std::string>>& csvRows) {
    ASSERT_FALSE(csvRows.empty()) << "CSV has no rows";
    std::string header = std::accumulate(
        csvRows.at(0).begin() + 1, csvRows.at(0).end(), csvRows.at(0).front(),
        [](const std::string& a, const std::string& b) { return a + "," + b; });
    ASSERT_EQ(header, FileEventSink::kBaseHeader);
  }

  void checkCsvRows(const std::vector<std::vector<std::string>>& csvRows,
                    const std::vector<EventInfo>& events,
                    bool checkAllRows = true) {
    checkCsvHeader(csvRows);
    if (checkAllRows) {
      // There is no sampling so we expect each row in CSV to
      // have a corresponding event in events vector.
      ASSERT_EQ(csvRows.size(), events.size() + 1)
          << "Expected " << events.size() + 1
          << " rows (including header), got " << csvRows.size();

      for (uint64_t i = 0; i < events.size(); i++) {
        const auto& row = csvRows.at(i + 1);
        const auto& event = events.at(i);
        ASSERT_TRUE(compareCsvRowWithEvent(row, event))
            << "Row " << i + 1 << " does not match event " << i << ". Row: ["
            << row.at(0) << ", " << row.at(1) << ", " << row.at(2) << ", "
            << row.at(3) << "], Event: [" << event.eventTimestamp << ", "
            << toString(event.event) << ", " << toString(event.result) << ", "
            << event.key << "]";
      }
    } else {
      // There is sampling so we expect less events to be logged. We expect
      // each row in CSV to have a corresponding event in events vector but
      // not the other way around.
      size_t eventIdx = 0;
      for (size_t rowIdx = 1; rowIdx < csvRows.size(); ++rowIdx) {
        while (eventIdx < events.size()) {
          const auto& row = csvRows.at(rowIdx);
          const auto& event = events.at(eventIdx);
          if (compareCsvRowWithEvent(row, event)) {
            ++eventIdx;
            break;
          }
          ++eventIdx;
        }
        ASSERT_LT(eventIdx, events.size() + 1)
            << "Row " << rowIdx << " does not match any remaining event";
      }
    }
  }

  std::unique_ptr<EventTracker> createEventTracker(const std::string& filePath,
                                                   uint32_t samplingRate = 1) {
    EventTracker::Config config;
    config.samplingRate = samplingRate;
    config.eventSink = std::make_unique<FileEventSink>(filePath);
    return std::make_unique<EventTracker>(std::move(config));
  }

  void recordEvents(EventTracker* tracker,
                    const std::vector<EventInfo>& events,
                    uint32_t startIdx = 0,
                    uint32_t endIdx = 0) {
    if (endIdx == 0) {
      endIdx = events.size();
    }
    for (uint32_t i = startIdx; i < endIdx; i++) {
      tracker->record(events.at(i));
    }
  }
};

TEST_F(EventTrackerTest, BasicLogging) {
  const char* key = "sample_k0";
  const uint32_t numItems = 10;

  auto events = createEvents(key, numItems);
  folly::test::TemporaryFile tmpFile;

  {
    auto eventTracker = createEventTracker(tmpFile.path().string());
    recordEvents(eventTracker.get(), events);
  }

  auto csvRows = readCsvRows(tmpFile);
  checkCsvRows(csvRows, events);
}

TEST_F(EventTrackerTest, SamplingRateChange) {
  const char* key = "sample_k1";
  const uint32_t numItems = 100;

  auto events = createEvents(key, numItems);
  folly::test::TemporaryFile tmpFile;

  auto eventTracker = createEventTracker(tmpFile.path().string());
  recordEvents(eventTracker.get(), events, 0, numItems / 2);

  eventTracker->setSamplingRate(3);

  recordEvents(eventTracker.get(), events, numItems / 2, numItems);

  auto csvRows = readCsvRows(tmpFile);

  ASSERT_GE(csvRows.size(), numItems / 3 + 1);
  ASSERT_LE(csvRows.size(), numItems + 1);

  checkCsvRows(csvRows, events, /*checkAllRows=*/false);

  uint32_t lowIndexCount = 0;
  uint32_t highIndexCount = 0;
  for (size_t i = 1; i < csvRows.size(); ++i) {
    const auto& row = csvRows[i];
    if (std::stoul(row.at(0)) < numItems / 2) {
      lowIndexCount++;
    } else {
      highIndexCount++;
    }
  }

  ASSERT_GT(lowIndexCount, 0);
  ASSERT_GT(highIndexCount, 0);
  ASSERT_EQ(lowIndexCount, numItems / 2);
  ASSERT_GT(lowIndexCount, highIndexCount);
  ASSERT_LT(lowIndexCount + highIndexCount, numItems);
}
