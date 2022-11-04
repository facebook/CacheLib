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

#include <folly/SharedMutex.h>

#include "cachelib/common/Hash.h"
#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

/**
 * CCReadHolder and CCWriteHolder is a duplication of ReadHolder and WriteHolder
 * in SharedMutex. They add the funtionality to do
 * try_lock_for/try_lock_shared_for when constructing the lock holder on top of
 * the original lock holder implementation
 */

class CCReadHolder {
 public:
  // construct a read lock holder and grab the lock
  explicit CCReadHolder(folly::SharedMutex& lock) : lock_(&lock) {
    lock_->lock_shared(token_);
  }

  // construct a read lock and
  // 1. try to grab the lock for a duration specified by _timeout_ if _timeout_
  //    is not zero OR
  // 2. just grab the lock
  CCReadHolder(folly::SharedMutex& lock,
               const std::chrono::microseconds& timeout)
      : lock_(&lock) {
    if (timeout == std::chrono::microseconds::zero()) {
      lock_->lock_shared(token_);
    } else {
      if (!lock_->try_lock_shared_for(timeout, token_)) {
        lock_ = nullptr;
      }
    }
  }

  CCReadHolder(CCReadHolder&& rhs) noexcept
      : lock_(rhs.lock_), token_(rhs.token_) {
    rhs.lock_ = nullptr;
  }

  CCReadHolder& operator=(CCReadHolder&& rhs) noexcept {
    std::swap(lock_, rhs.lock_);
    std::swap(token_, rhs.token_);
    return *this;
  }

  CCReadHolder(const CCReadHolder& rhs) = delete;
  CCReadHolder& operator=(const CCReadHolder& rhs) = delete;

  ~CCReadHolder() { unlock(); }

  void unlock() {
    if (lock_) {
      lock_->unlock_shared(token_);
      lock_ = nullptr;
    }
  }

  bool locked() const noexcept { return lock_ != nullptr; }

 private:
  // pointer to the lock
  folly::SharedMutex* lock_;

  // lock token used for faster unlock_shared() to quickly find the right lock
  folly::SharedMutexToken token_;
};

class CCWriteHolder {
 public:
  CCWriteHolder() : lock_(nullptr) {}

  // construct a write lock holder and grab the lock
  explicit CCWriteHolder(folly::SharedMutex& lock) : lock_(&lock) {
    lock_->lock();
  }

  // construct a write lock and
  // 1. try to grab the lock for a duration specified by _timeout_ if _timeout_
  //    is not zero OR
  // 2. just grab the lock
  CCWriteHolder(folly::SharedMutex& lock,
                const std::chrono::microseconds& timeout)
      : lock_(&lock) {
    if (timeout == std::chrono::microseconds::zero()) {
      lock_->lock();
    } else {
      if (!lock_->try_lock_for(timeout)) {
        lock_ = nullptr;
      }
    }
  }

  CCWriteHolder(CCWriteHolder&& rhs) noexcept : lock_(rhs.lock_) {
    rhs.lock_ = nullptr;
  }

  CCWriteHolder& operator=(CCWriteHolder&& rhs) noexcept {
    std::swap(lock_, rhs.lock_);
    return *this;
  }

  CCWriteHolder(const CCWriteHolder& rhs) = delete;
  CCWriteHolder& operator=(const CCWriteHolder& rhs) = delete;

  ~CCWriteHolder() { unlock(); }

  void unlock() {
    if (lock_) {
      lock_->unlock();
      lock_ = nullptr;
    }
  }

  bool locked() const noexcept { return lock_ != nullptr; }

 private:
  // pointer to the lock
  folly::SharedMutex* lock_;
};

using CCRWBucketLocks =
    RWBucketLocks<folly::SharedMutex, CCReadHolder, CCWriteHolder>;
} // namespace cachelib
} // namespace facebook
