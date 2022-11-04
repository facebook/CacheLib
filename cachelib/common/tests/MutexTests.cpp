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

#include <folly/SharedMutex.h>
#include <gtest/gtest.h>

#include <thread>

#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

template <typename Mutex>
void testMutexBasic() {
  Mutex m;
  // lock the mutex and trylock should fail.
  ASSERT_NO_THROW(m.lock());
  ASSERT_FALSE(m.try_lock());

  bool threadAcquired = false;
  // try from another thread.
  auto tryLock = [&m, &threadAcquired]() {
    int tries = 1000;
    threadAcquired = false;
    while (--tries && !m.try_lock()) {
    }
    threadAcquired = tries != 0;
  };

  std::thread thread{tryLock};
  thread.join();
  ASSERT_FALSE(threadAcquired);

  ASSERT_NO_THROW(m.unlock());
  m.unlock();
  ASSERT_TRUE(m.try_lock());
  ASSERT_FALSE(m.try_lock());

  thread = std::thread{tryLock};
  thread.join();
  ASSERT_FALSE(threadAcquired);

  m.unlock();
  thread = std::thread{tryLock};
  thread.join();
  ASSERT_TRUE(threadAcquired);

  m.unlock();
}
TEST(PThreadMutexAdaptive, Basic) { testMutexBasic<PThreadAdaptiveMutex>(); }
TEST(PThreadMutex, Basic) { testMutexBasic<PThreadMutex>(); }
TEST(PThreadSpinMutex, Basic) { testMutexBasic<PThreadSpinMutex>(); }
TEST(PThreadSpinLock, Basic) { testMutexBasic<PThreadSpinLock>(); }

template <typename Mutex>
void testMove() {
  Mutex m1;
  ASSERT_NO_THROW(m1.lock());
  ASSERT_FALSE(m1.try_lock());

  Mutex m2;
  m2 = std::move(m1);
  ASSERT_FALSE(m2.try_lock());
  m2.unlock();
  ASSERT_NO_THROW(m2.lock());

  Mutex m3(std::move(m2));
  ASSERT_FALSE(m3.try_lock());
  m3.unlock();
  ASSERT_NO_THROW(m3.lock());
  m3.unlock();
  ASSERT_TRUE(m3.try_lock());
  ASSERT_FALSE(m3.try_lock());
  m3.unlock();
}
TEST(PThreadMutexAdaptive, TestMove) { testMove<PThreadAdaptiveMutex>(); }
TEST(PThreadMutex, TestMove) { testMove<PThreadMutex>(); }
TEST(PThreadSpinMutex, TestMove) { testMove<PThreadSpinMutex>(); }
TEST(PThreadSpinLock, TestMove) { testMove<PThreadSpinLock>(); }

template <typename Mutex>
void testBucketLocks() {
  using Locks = BucketLocks<Mutex>;
  Locks locks(10, std::make_shared<FNVHash>());

  auto test = [&] {
    auto l1 = locks.lock("helloworld");
    auto l2 = locks.lock(1234 /* some hash value */);

    ASSERT_TRUE(l1.owns_lock());
    ASSERT_TRUE(l2.owns_lock());
  };

  // Run twice. It should succeed both times and not deadlock.
  test();
  test();
}
TEST(PThreadMutexAdaptive, TestBucketLocks) {
  testBucketLocks<PThreadAdaptiveMutex>();
}
TEST(PThreadMutex, TestBucketLocks) { testBucketLocks<PThreadMutex>(); }
TEST(PThreadSpinMutex, TestBucketLocks) { testBucketLocks<PThreadSpinMutex>(); }
TEST(PThreadSpinLock, TestBucketLocks) { testBucketLocks<PThreadSpinLock>(); }

template <typename Lock, typename Reader, typename Writer>
void testRWBucketLocks() {
  using Locks = RWBucketLocks<Lock, Reader, Writer>;
  Locks locks(10, std::make_shared<FNVHash>());

  // It's fine creating multiple shared locks on the same key
  {
    auto s1 = locks.lockShared("helloworld");
    auto s2 = locks.lockShared("helloworld");
    auto s3 = locks.lockShared("helloworld");

    auto l1 = locks.lockShared(1234);
    auto l2 = locks.lockShared(1234);
    auto l3 = locks.lockShared(1234);
  }

  {
    auto l1 = locks.lockExclusive("helloworld");
    auto l2 = locks.lockExclusive(1234);

    l1.unlock();
    l2.unlock();

    // Shouldn't deadlock
    auto l11 = locks.lockExclusive("helloworld");
    auto l22 = locks.lockExclusive(1234);
  }
}
TEST(SharedMutex, TestRWBucketLocks) {
  testRWBucketLocks<folly::SharedMutex, folly::SharedMutex::ReadHolder,
                    folly::SharedMutex::WriteHolder>();
}
} // namespace cachelib
} // namespace facebook
