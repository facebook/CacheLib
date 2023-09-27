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

#include "inject_pause.h"

#include <folly/Indestructible.h>
#include <folly/Synchronized.h>
#include <folly/fibers/Baton.h>
#include <folly/logging/xlog.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

namespace facebook {
namespace cachelib {

namespace {
struct BatonSet {
  std::deque<folly::fibers::Baton*> batons;
  std::condition_variable cv;
  bool enabled{false};
  size_t limit{0};
  std::shared_ptr<PauseCallback> callback;
};

using PausePointsMap =
    folly::Synchronized<std::unordered_map<std::string, BatonSet>, std::mutex>;
PausePointsMap& pausePoints() {
  static folly::Indestructible<PausePointsMap> pausePoints;
  return *pausePoints;
}

size_t wakeupBatonSet(BatonSet& batonSet, size_t numThreads) {
  size_t numWaked = 0;
  if (numThreads == 0) {
    numThreads = batonSet.batons.size();
  }

  while (!batonSet.batons.empty() && numWaked < numThreads) {
    auto* b = batonSet.batons.front();
    batonSet.batons.pop_front();
    b->post();
    numWaked++;
  }

  return numWaked;
}

} // namespace

bool& injectPauseEnabled() {
  static bool enabled = false;
  return enabled;
}

// Flag controls the debug logging
bool& injectPauseLogEnabled() {
  static bool enabled = false;
  return enabled;
}

namespace detail {

void injectPause(folly::StringPiece name) {
  if (!injectPauseEnabled()) {
    return;
  }
  folly::fibers::Baton baton;
  std::shared_ptr<PauseCallback> callback;
  {
    auto ptr = pausePoints().lock();
    auto it = ptr->find(name.str());
    if (it == ptr->end() || !it->second.enabled ||
        (it->second.limit > 0 &&
         it->second.batons.size() == it->second.limit)) {
      if (injectPauseLogEnabled()) {
        XLOGF(ERR, "[{}] injectPause not set", name);
      }
      return;
    }
    if (injectPauseLogEnabled()) {
      XLOGF(ERR, "[{}] injectPause begin", name);
    }
    callback = it->second.callback;
    if (!callback) {
      it->second.batons.push_back(&baton);
      it->second.cv.notify_one();
    }
  }

  if (callback) {
    (*callback)();
  } else {
    baton.wait();
  }

  /* Avoid potential protect-its-own-lifetime bug:
     Wait for the post() to finish before destroying the Baton. */
  auto ptr = pausePoints().lock();
  if (injectPauseLogEnabled()) {
    XLOGF(ERR, "[{}] injectPause end", name);
  }
}

} // namespace detail

void injectPauseSet(folly::StringPiece name, size_t numThreads) {
  if (!injectPauseEnabled()) {
    return;
  }
  auto ptr = pausePoints().lock();
  if (injectPauseLogEnabled()) {
    XLOGF(ERR, "[{}] injectPauseSet threads {}", name, numThreads);
  }
  auto res = ptr->emplace(
      std::piecewise_construct, std::make_tuple(name.str()), std::make_tuple());
  res.first->second.limit = numThreads;
  res.first->second.enabled = true;
}

void injectPauseSet(folly::StringPiece name, PauseCallback&& callback) {
  if (!injectPauseEnabled()) {
    return;
  }
  auto ptr = pausePoints().lock();
  if (injectPauseLogEnabled()) {
    XLOGF(ERR, "[{}] injectPauseSet callback", name);
  }
  auto res = ptr->emplace(
      std::piecewise_construct, std::make_tuple(name.str()), std::make_tuple());
  res.first->second.limit = 0;
  res.first->second.enabled = true;
  res.first->second.callback =
      std::make_shared<PauseCallback>(std::move(callback));
}

bool injectPauseWait(folly::StringPiece name,
                     size_t numThreads,
                     bool wakeup,
                     uint32_t timeoutMs) {
  if (!injectPauseEnabled() || !numThreads) {
    return false;
  }
  auto ptr = pausePoints().lock();
  if (injectPauseLogEnabled()) {
    XLOGF(ERR, "[{}] injectPauseWait begin {}", name, numThreads);
  }
  auto it = ptr->find(name.str());
  if (it == ptr->end() || !it->second.enabled) {
    if (injectPauseLogEnabled()) {
      XLOGF(ERR, "[{}] injectPauseWait: ERROR not set", name);
    }
    return false;
  }

  if (!timeoutMs || timeoutMs > kInjectPauseMaxWaitTimeoutMs) {
    timeoutMs = kInjectPauseMaxWaitTimeoutMs;
  }

  auto& batonSet = it->second;
  bool status = batonSet.cv.wait_for(
      ptr.as_lock(),
      std::chrono::milliseconds(timeoutMs),
      [&batonSet, numThreads]() {
        return !batonSet.enabled || batonSet.batons.size() >= numThreads;
      });

  if (status && wakeup) {
    wakeupBatonSet(batonSet, numThreads);
  }

  if (injectPauseLogEnabled()) {
    std::string errStr;
    if (!status) {
      errStr = fmt::format(" with ERR (paused {})", batonSet.batons.size());
    }
    XLOGF(ERR, "[{}] injectPauseWait end {}", name, errStr);
  }
  return status;
}

size_t injectPauseClear(folly::StringPiece name) {
  if (!injectPauseEnabled()) {
    return false;
  }

  size_t numPaused = 0;
  auto ptr = pausePoints().lock();
  if (injectPauseLogEnabled()) {
    XLOGF(ERR, "[{}] injectPauseClear ", name);
  }

  if (name.empty()) {
    for (auto& it : *ptr) {
      auto& batonSet = it.second;
      numPaused += wakeupBatonSet(batonSet, 0);
      batonSet.enabled = false;
      batonSet.cv.notify_all();
    }
  } else {
    auto it = ptr->find(name.str());
    if (it != ptr->end()) {
      auto& batonSet = it->second;
      numPaused += wakeupBatonSet(batonSet, 0) > 0 ? 1 : 0;
      batonSet.enabled = false;
      batonSet.cv.notify_all();
    }
  }
  return numPaused;
}

} // namespace cachelib
} // namespace facebook
