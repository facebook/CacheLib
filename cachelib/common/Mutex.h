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
#include <folly/Range.h>
#include <folly/ScopeGuard.h>
#include <folly/SharedMutex.h>
#include <folly/SpinLock.h>
#include <folly/logging/xlog.h>
#include <folly/portability/Asm.h>
#include <pthread.h>

#include <memory>
#include <mutex>
#include <system_error>

#include "cachelib/common/Hash.h"

namespace facebook {
namespace cachelib {

namespace detail {

template <int MutexType = PTHREAD_MUTEX_TIMED_NP>
class PThreadMutexImpl {
 public:
  // create the mutex and initialize it appropriately
  PThreadMutexImpl() {
    mutex_ = std::make_unique<pthread_mutex_t>();

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

    SCOPE_EXIT { pthread_mutexattr_destroy(&attr); };

    pthread_mutexattr_settype(&attr, MutexType);
    const int err = pthread_mutex_init(mutex_.get(), &attr);
    if (err) {
      XDCHECK(false);
      throw std::system_error(err, std::system_category(),
                              "failed to init mutex");
    }
  }

  // destroy the mutex
  ~PThreadMutexImpl() noexcept {
    if (mutex_) {
      pthread_mutex_destroy(mutex_.get());
    }
  }

  // no copying
  PThreadMutexImpl(const PThreadMutexImpl&) = delete;
  PThreadMutexImpl& operator=(const PThreadMutexImpl&) = delete;

  // can be moved
  PThreadMutexImpl(PThreadMutexImpl&&) noexcept = default;
  PThreadMutexImpl& operator=(PThreadMutexImpl&&) noexcept = default;

  // lock the mutex. block if the mutex is owned by another thread.
  //
  // @return none
  // @throw pthread specific system_error that indicates whether there was an
  // error in mutex state and guarantes. See man pthread_mutex_lock for full
  // details.
  void lock() {
    const int err = pthread_mutex_lock(mutex_.get());
    if (err) {
      throw std::system_error(err, std::system_category());
    }
  }

  // unlock the mutex owned by this thread.
  //
  // @return  none
  // @throw   system_error with EINVAL if the mutex is invalid
  //          system_error with EPERM if this thread does not own the mutex.
  void unlock() {
    const int err = pthread_mutex_unlock(mutex_.get());
    if (err) {
      throw std::system_error(err, std::system_category());
    }
  }

  // try to lock the mutex.
  //
  // @return true if the mutex was acquired. false if the  mutex is owned by
  // another thread.
  // @throw   std::system_error if the mutex state is invalid or the mutex is
  //          has exhausted the recursive calls to lock.
  bool try_lock() {
    const int err = pthread_mutex_trylock(mutex_.get());
    if (err == 0) {
      return true;
    } else if (err == EBUSY) {
      return false;
    } else {
      throw std::system_error(err, std::system_category());
    }
  }

 private:
  std::unique_ptr<pthread_mutex_t> mutex_;
};

// This looks very hacky/clowny, but is what memcached today uses in
// production for its LRU mutex. Seems to work very well for CI like workloads
// where all we do is LRU udpates and very few inserts/evictions.
template <unsigned int spinCounter>
class PThreadSpinMutexImpl {
 public:
  PThreadSpinMutexImpl() {}
  // lock the mutex. block if the mutex is owned by another thread.
  //
  // @return none
  // @throw pthread specific system_error that indicates whether there was an
  // error in mutex state and guarantes. See man pthread_mutex_lock for full
  // details.
  void lock() {
    while (!mutex_.try_lock()) {
      spinWait(spinCounter);
    }
  }

  // unlock the mutex owned by this thread.
  //
  // @return  none
  // @throw   system_error with EINVAL if the mutex is invalid
  //          system_error with EPERM if this thread does not own the mutex.
  void unlock() { mutex_.unlock(); }

  // try to lock the mutex.
  //
  // @return true if the mutex was acquired. false if the  mutex is owned by
  // another thread.
  // @throw   std::system_error if the mutex state is invalid or the mutex is
  //          has exhausted the recursive calls to lock.
  bool try_lock() { return mutex_.try_lock(); }

 private:
  static void spinWait(unsigned int count) noexcept {
    while (--count) {
      folly::asm_volatile_pause();
      folly::asm_volatile_memory();
    }
  }

  detail::PThreadMutexImpl<PTHREAD_MUTEX_ADAPTIVE_NP> mutex_{};
};

class PThreadSpinLock {
 public:
  PThreadSpinLock() {
    spinLock_ = std::make_unique<pthread_spinlock_t>();
    const int err = pthread_spin_init(spinLock_.get(), PTHREAD_PROCESS_PRIVATE);
    if (err) {
      throw std::system_error(err, std::system_category(),
                              "Failed to create spinlock");
    }
  }

  PThreadSpinLock(const PThreadSpinLock&) = delete;
  PThreadSpinLock& operator=(const PThreadSpinLock&) = delete;

  PThreadSpinLock(PThreadSpinLock&&) = default;
  PThreadSpinLock& operator=(PThreadSpinLock&&) = default;

  ~PThreadSpinLock() {
    if (spinLock_) {
      pthread_spin_destroy(spinLock_.get());
    }
  }

  void lock() {
    const int err = pthread_spin_lock(spinLock_.get());
    if (err) {
      throw std::system_error(err, std::system_category());
    }
  }

  void unlock() {
    const int err = pthread_spin_unlock(spinLock_.get());
    if (err) {
      throw std::system_error(err, std::system_category());
    }
  }

  // try to lock the mutex.
  //
  // @return true if the mutex was acquired. false if the  mutex is owned by
  // another thread.
  // @throw   std::system_error if the mutex state is invalid or the mutex is
  //          has exhausted the recursive calls to lock.
  bool try_lock() {
    const int err = pthread_spin_trylock(spinLock_.get());
    if (err == 0) {
      return true;
    } else if (err == EBUSY) {
      return false;
    } else {
      throw std::system_error(err, std::system_category());
    }
  }

 private:
  std::unique_ptr<pthread_spinlock_t> spinLock_;
};

// a lock that provides a read and write holder, but both are exclusive. this
// is used to provide the same API to chained hash table so that we dont have
// to template between using a RW mutex and a mutex
struct RWMockLock {
  using Lock = folly::MicroSpinLock;
  using ReadHolder = std::unique_lock<RWMockLock>;
  using WriteHolder = std::unique_lock<RWMockLock>;

  void lock() { l_.lock(); }
  void unlock() { l_.unlock(); }
  bool try_lock() { return l_.try_lock(); }

 private:
  Lock l_;
};

} // namespace detail

using PThreadAdaptiveMutex =
    detail::PThreadMutexImpl<PTHREAD_MUTEX_ADAPTIVE_NP>;
using PThreadMutex = detail::PThreadMutexImpl<>;
using PThreadSpinMutex = detail::PThreadSpinMutexImpl<1024>;
using PThreadSpinLock = detail::PThreadSpinLock;
using RWMockLock = detail::RWMockLock;

template <typename T>
struct DefaultLockAlignment {
 public:
  T* get() { return &l; }

  const T* get() const { return &l; }

  T* operator->() { return get(); }

  const T* operator->() const { return get(); }

  T& operator*() { return *get(); }

  const T& operator*() const { return *get(); }

 private:
  // no alignment
  T l;
};

template <typename LockType,
          template <class> class LockAlignmentType = DefaultLockAlignment>
class BaseBucketLocks {
 public:
  using Lock = LockType;

  BaseBucketLocks(uint32_t locksPower, std::shared_ptr<Hash> hasher)
      : locksMask_((1ULL << locksPower) - 1),
        locks_((1ULL << locksPower)),
        hasher_(std::move(hasher)) {}

  // Check whether two keys are mapped to the same lock
  // This can only support key type that has only one argument,
  // such as StringPiece or void*
  template <typename T>
  bool isSameLock(const T& key1, const T& key2) noexcept {
    return &getLock(key1) == &getLock(key2);
  }

 protected:
  // Get a reference to the lock for this key
  Lock& getLock(const void* data, size_t size) noexcept {
    return getLock((*hasher_)(data, size));
  }

  // Get a reference to the lock for this pointer
  // This function will take the address _ptr_ is pointing to as the hash key
  Lock& getLock(void* ptr) noexcept { return getLock(&ptr, sizeof(ptr)); }

  Lock& getLock(folly::StringPiece key) noexcept {
    return getLock(key.data(), key.size());
  }

  // Get a reference to the lock that will used for this particular hash
  Lock& getLock(uint64_t hash) noexcept { return *locks_[hash & locksMask_]; }

 private:
  // materialized value of (number of locks) - 1
  const size_t locksMask_;

  std::vector<LockAlignmentType<Lock>> locks_;
  std::shared_ptr<Hash> hasher_;
};

template <typename LockType,
          template <class> class LockAlignmentType = DefaultLockAlignment>
class BucketLocks : public BaseBucketLocks<LockType, LockAlignmentType> {
 public:
  using Base = BaseBucketLocks<LockType, LockAlignmentType>;
  using Lock = LockType;
  using LockHolder = std::unique_lock<Lock>;

  BucketLocks(uint32_t locksPower, std::shared_ptr<Hash> hasher)
      : Base::BaseBucketLocks(locksPower, std::move(hasher)) {}

  // Lock for this particular key and return a lock holder
  template <typename... Args>
  LockHolder lock(Args... args) noexcept {
    return LockHolder(Base::getLock(args...));
  }

  template <typename... Args>
  LockHolder tryLock(Args... args) noexcept {
    return LockHolder(Base::getLock(args...), std::try_to_lock);
  }
};

template <typename LockType,
          typename ReadLockHolderType = typename LockType::ReadHolder,
          typename WriteLockHolderType = typename LockType::WriteHolder,
          template <class> class LockAlignmentType = DefaultLockAlignment>
class RWBucketLocks : public BaseBucketLocks<LockType, LockAlignmentType> {
 public:
  using Base = BaseBucketLocks<LockType, LockAlignmentType>;
  using Lock = LockType;
  using ReadLockHolder = ReadLockHolderType;
  using WriteLockHolder = WriteLockHolderType;

  RWBucketLocks(uint32_t locksPower, std::shared_ptr<Hash> hasher)
      : Base::BaseBucketLocks(locksPower, std::move(hasher)) {}

  // Lock for this particular key and return a reader lock
  template <typename... Args>
  ReadLockHolder lockShared(Args... args) {
    return ReadLockHolder{Base::getLock(args...)};
  }

  // Lock for this particular key and return a writer lock
  template <typename... Args>
  WriteLockHolder lockExclusive(Args... args) {
    return WriteLockHolder{Base::getLock(args...)};
  }

  // try to grab the reader lock for a limit _timeout_ duration
  template <typename... Args>
  ReadLockHolder lockShared(const std::chrono::microseconds& timeout,
                            Args... args) {
    return ReadLockHolder(Base::getLock(args...), timeout);
  }

  // try to grab the writer lock for a limit _timeout_ duration
  template <typename... Args>
  WriteLockHolder lockExclusive(const std::chrono::microseconds& timeout,
                                Args... args) {
    return WriteLockHolder(Base::getLock(args...), timeout);
  }
};

using SharedMutexBuckets = RWBucketLocks<folly::SharedMutex>;

// a spinning mutex appearing as a rw mutex
using SpinBuckets = RWBucketLocks<RWMockLock>;

} // namespace cachelib
} // namespace facebook
