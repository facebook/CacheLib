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

#include "cachelib/navy/testing/SeqPoints.h"

#include <folly/Range.h>
#include <glog/logging.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
folly::StringPiece toString(SeqPoints::Event event) {
  switch (event) {
  case SeqPoints::Event::Wait:
    return "Wait";
  case SeqPoints::Event::Reached:
    return "Reached";
  }
}
} // namespace

SeqPoints::Logger SeqPoints::defaultLogger() {
  return [](Event event, uint32_t idx, const std::string& name) {
    LOG(ERROR) << toString(event) << " " << idx << " " << name;
  };
}

void SeqPoints::reached(uint32_t idx) {
  std::lock_guard<std::mutex> lock{mutex_};
  log(Event::Reached, idx, points_[idx].name);
  points_[idx].reached = true;
  cv_.notify_all();
}

void SeqPoints::wait(uint32_t idx) {
  std::unique_lock<std::mutex> lock{mutex_};
  log(Event::Wait, idx, points_[idx].name);
  while (!points_[idx].reached) {
    cv_.wait(lock);
  }
}

bool SeqPoints::waitFor(uint32_t idx, std::chrono::microseconds dur) {
  auto until = std::chrono::steady_clock::now() + dur;
  std::unique_lock<std::mutex> lock{mutex_};
  log(Event::Wait, idx, points_[idx].name);
  while (!points_[idx].reached) {
    if (cv_.wait_until(lock, until) == std::cv_status::timeout) {
      return false;
    }
  }
  return true;
}

void SeqPoints::setName(uint32_t idx, std::string msg) {
  std::lock_guard<std::mutex> lock{mutex_};
  points_[idx].name = std::move(msg);
}

void SeqPoints::log(Event event, uint32_t idx, const std::string& name) {
  if (logger_) {
    logger_(event, idx, name);
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
