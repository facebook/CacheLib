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

#include "cachelib/navy/testing/MockJobScheduler.h"

#include <folly/Format.h>
#include <glog/logging.h>

#include <cassert>

namespace facebook {
namespace cachelib {
namespace navy {
MockJobScheduler::~MockJobScheduler() { XDCHECK(q_.empty()); }

void MockJobScheduler::enqueue(Job job, folly::StringPiece name, JobType type) {
  std::lock_guard<std::mutex> lock{m_};
  switch (type) {
  case JobType::Reclaim:
  case JobType::Flush:
    q_.emplace_front(std::move(job), name);
    break;
  case JobType::Read:
  case JobType::Write:
    q_.emplace_back(std::move(job), name);
    break;
  default:
    XDCHECK(false);
  }
}

void MockJobScheduler::finish() {
  std::unique_lock<std::mutex> lock{m_};
  while (!q_.empty() || processing_) {
    lock.unlock();
    std::this_thread::yield();
    lock.lock();
  }
}

bool MockJobScheduler::runFirstIf(folly::StringPiece expected) {
  std::unique_lock<std::mutex> lock{m_};
  if (q_.empty()) {
    throwLogicError("empty job queue");
  }
  return runFirstIfLocked(expected, lock);
}

bool MockJobScheduler::runFirstIfLocked(folly::StringPiece expected,
                                        std::unique_lock<std::mutex>& lock) {
  XDCHECK(lock.owns_lock());
  auto first = std::move(q_.front());
  q_.pop_front();
  if (!first.nameIs(expected)) {
    q_.push_front(std::move(first));
    throwLogicError(
        folly::sformat("found job '{}', expected '{}'", first.name, expected));
  }
  JobExitCode ec;
  {
    processing_ = true;
    lock.unlock();
    ec = first.job();
    lock.lock();
    processing_ = false;
  }
  if (ec == JobExitCode::Done) {
    doneCount_++;
    return true;
  }
  rescheduleCount_++;
  q_.push_back(std::move(first));
  return false;
}

void MockJobScheduler::throwLogicError(const std::string& what) {
  throw std::logic_error(what);
}

void MockSingleThreadJobScheduler::process() {
  std::unique_lock<std::mutex> lock{m_};
  while (true) {
    if (!q_.empty()) {
      runFirstIfLocked("", lock);
    }
    if (q_.empty() && !processing_) {
      break;
    }
    lock.unlock();
    std::this_thread::yield();
    lock.lock();
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
