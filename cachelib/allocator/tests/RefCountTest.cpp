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

#include <folly/Random.h>

#include <thread>

#include "cachelib/allocator/Refcount.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

namespace {
class RefCountTest : public AllocTestBase {
 public:
  static void testMultiThreaded();
  static void testBasic();
  static void testMarkForEvictionAndMoving();
};

void RefCountTest::testMultiThreaded() {
  const uint32_t maxCount = RefcountWithFlags::kAccessRefMask;
  RefcountWithFlags ref;
  ASSERT_EQ(0, ref.getAccessRef());

  const unsigned int nThreads = 32;
  const unsigned int perThread = maxCount / nThreads;

  auto doInThread = [&ref]() {
    // we have perThread number of references. we can try to bump it up and
    // bump it down randomly.
    unsigned int iter = 0;
    unsigned int nLocalRef = 0;
    while (nLocalRef < perThread) {
      if (iter++ % 3 == 0 && nLocalRef > 0) {
        ref.decRef();
        nLocalRef--;
        ref.markAccessible();
      } else {
        ref.incRef();
        nLocalRef++;
        ref.unmarkAccessible();
      }

      if (nLocalRef % 10000) {
        std::this_thread::yield();
      }
    }
  };

  std::vector<std::thread> threads;
  for (unsigned int i = 0; i < nThreads; i++) {
    threads.emplace_back(doInThread);
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  ASSERT_EQ(perThread * nThreads, ref.getAccessRef());
}

void RefCountTest::testBasic() {
  RefcountWithFlags ref;
  ASSERT_EQ(0, ref.getAccessRef());
  ASSERT_EQ(0, ref.getRaw());
  ASSERT_FALSE(ref.isInMMContainer());
  ASSERT_FALSE(ref.isAccessible());
  ASSERT_FALSE(ref.isMoving());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  // set an admin ref bit but ensure the other bits are not affected
  ref.markInMMContainer();
  ASSERT_TRUE(ref.isInMMContainer());
  ASSERT_FALSE(ref.isAccessible());
  ASSERT_FALSE(ref.isMoving());
  ASSERT_EQ(0, ref.getAccessRef());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  // set a flag shouldn't affect admin ref and access ref
  ref.template setFlag<RefcountWithFlags::Flags::kMMFlag0>();
  ASSERT_TRUE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  for (uint32_t i = 0; i < RefcountWithFlags::kAccessRefMask; i++) {
    ASSERT_TRUE(ref.incRef());
  }

  // Incrementing past the max will fail
  auto rawRef = ref.getRaw();
  ASSERT_THROW(ref.incRef(), std::overflow_error);
  ASSERT_EQ(rawRef, ref.getRaw());

  // Bumping up access ref shouldn't affect admin ref and flags
  ASSERT_TRUE(ref.isInMMContainer());
  ASSERT_FALSE(ref.isAccessible());
  ASSERT_FALSE(ref.isMoving());
  ASSERT_EQ(RefcountWithFlags::kAccessRefMask, ref.getAccessRef());
  ASSERT_TRUE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  for (uint32_t i = 0; i < RefcountWithFlags::kAccessRefMask; i++) {
    ref.decRef();
  }

  // Decrementing past the min will fail
  rawRef = ref.getRaw();
  ASSERT_THROW(ref.decRef(), std::underflow_error);
  ASSERT_EQ(rawRef, ref.getRaw());

  // Bumping down access ref shouldn't affect admin ref and flags
  ASSERT_TRUE(ref.isInMMContainer());
  ASSERT_FALSE(ref.isAccessible());
  ASSERT_FALSE(ref.isMoving());
  ASSERT_EQ(0, ref.getAccessRef());
  ASSERT_TRUE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  ref.template unSetFlag<RefcountWithFlags::Flags::kMMFlag0>();
  ASSERT_TRUE(ref.isInMMContainer());
  ASSERT_FALSE(ref.isAccessible());
  ASSERT_FALSE(ref.isMoving());
  ASSERT_EQ(0, ref.getAccessRef());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  ref.unmarkInMMContainer();
  ASSERT_EQ(0, ref.getRaw());
  ASSERT_FALSE(ref.isInMMContainer());
  ASSERT_FALSE(ref.isAccessible());
  ASSERT_FALSE(ref.isMoving());
  ASSERT_EQ(0, ref.getAccessRef());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag0>());
  ASSERT_FALSE(ref.template isFlagSet<RefcountWithFlags::Flags::kMMFlag1>());

  // conditionally set flags
  ASSERT_FALSE((ref.markMoving()));
  ref.markInMMContainer();
  // only first one succeeds
  ASSERT_TRUE((ref.markMoving()));
  ASSERT_FALSE((ref.markMoving()));
  ref.unmarkInMMContainer();

  ref.template setFlag<RefcountWithFlags::Flags::kMMFlag0>();
  // Have no other admin refcount but with a flag still means "isOnlyMoving"
  ASSERT_TRUE((ref.isOnlyMoving()));

  // Set some flags and verify that "isOnlyMoving" does not care about flags
  ref.markIsChainedItem();
  ASSERT_TRUE(ref.isChainedItem());
  ASSERT_TRUE((ref.isOnlyMoving()));
  ref.unmarkIsChainedItem();
  ASSERT_FALSE(ref.isChainedItem());
  ASSERT_TRUE((ref.isOnlyMoving()));
}

void RefCountTest::testMarkForEvictionAndMoving() {
  {
    // cannot mark for eviction when not in MMContainer
    RefcountWithFlags ref;
    ASSERT_FALSE(ref.markForEviction());
  }

  {
    // can mark for eviction when in MMContainer
    // and unmarkForEviction return value contains admin bits
    RefcountWithFlags ref;
    ref.markInMMContainer();
    ASSERT_TRUE(ref.markForEviction());
    ASSERT_TRUE(ref.unmarkForEviction() > 0);
  }

  {
    // cannot mark for eviction when moving
    RefcountWithFlags ref;
    ref.markInMMContainer();

    ASSERT_TRUE(ref.markMoving());
    ASSERT_FALSE(ref.markForEviction());

    ref.unmarkInMMContainer();
    auto ret = ref.unmarkMoving();
    ASSERT_EQ(ret, 0);
  }

  {
    // cannot mark moving when marked for eviction
    RefcountWithFlags ref;
    ref.markInMMContainer();

    ASSERT_TRUE(ref.markForEviction());
    ASSERT_FALSE(ref.markMoving());

    ref.unmarkInMMContainer();
    auto ret = ref.unmarkForEviction();
    ASSERT_EQ(ret, 0);
  }

  {
    // can mark moving when ref count > 0
    RefcountWithFlags ref;
    ref.markInMMContainer();

    ref.incRef();

    ASSERT_TRUE(ref.markMoving());

    ref.unmarkInMMContainer();
    auto ret = ref.unmarkMoving();
    ASSERT_EQ(ret, 1);
  }

  {
    // cannot mark for eviction when ref count > 0
    RefcountWithFlags ref;
    ref.markInMMContainer();

    ref.incRef();
    ASSERT_FALSE(ref.markForEviction());
  }
}
} // namespace

TEST_F(RefCountTest, MutliThreaded) { testMultiThreaded(); }
TEST_F(RefCountTest, Basic) { testBasic(); }
TEST_F(RefCountTest, MarkForEvictionAndMoving) {
  testMarkForEvictionAndMoving();
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
