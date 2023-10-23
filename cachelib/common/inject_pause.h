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

#include <folly/Function.h>
#include <folly/Range.h>

/*
 * inject_pause framework provides a rendezvous-style syncronization
 * for debugging and testing purposes. This allows us to control
 * executions of multiple threads deterministically.
 *
 * The basic idea is to have a named point in code where we want to
 * block execution of some threads. We then allow other threads,
 * probably, the test threads, to synchronize with the main threads
 * at the pause points and signal the threads to proceed.
 *
 * The basic usage is: (e.g., RegionManager.ReadWrite unit test)
 * 1. Insert a call to INJECT_PAUSE() in the code
 *    void RegionManager::doReclaim() {
 *      ...
 *      INJECT_PAUSE("pause_reclaim_done");
 *    }
 *
 * 2. Enable INJECT_PAUSE() using injectPauseEnabled() in unit test
 *    TEST(RegionManager, ReadWrite) {
 *        ...
 *        injectPauseEnabled() = true;
 *
 * 3. In the test, we can wait for the threads to reach the pause point
 *
 *        ...
 *        rm->startReclaim();
 *        EXPECT_TRUE(injectPauseWait("pause_reclaim_done"));
 *        ASSERT_EQ(OpenStatus::Ready, rm->getCleanRegion(rid, false).first);
 *        ASSERT_EQ(0, rid.index());
 *
 * 4. Once done, clear the pause point
 *
 *        ASSERT_EQ(0, injectPauseClear("pause_reclaim_done"));
 *        ...
 *    }
 *
 * In above example, the region reclaim thread and test thread are synchronized
 * at the pause point and injectPauseWait() and continue.
 *
 * The inject_pause framework also supports setting a callback function where
 * the threads can register a callback function to be called when they hit the
 * INJECT_PAUSE() point.
 *
 * The example usage with the callback function is as follows:
 *
 *     folly::fibers::TimedMutex mutex;
 *     bool reclaimStarted = false;
 *     size_t numCleanRegions = 0;
 *     util::ConditionVariable cv;
 *
 *     injectPauseSet("pause_blockcache_clean_alloc", [&]() {
 *       std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
 *       XDCHECK_GT(numCleanRegions, 0u);
 *       numCleanRegions--;
 *     });
 *
 *     injectPauseSet("pause_blockcache_clean_free", [&]() {
 *       std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
 *       if (numCleanRegions++ == 0u) {
 *         cv.notifyAll();
 *       }
 *       reclaimStarted = true;
 *     });
 *
 *     injectPauseSet("pause_block_cache_insert_entry", [&]() {
 *       std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
 *       if (numCleanRegions == 0u && reclaimStarted) {
 *         cv.wait(lk);
 *       }
 *     });
 *
 * In the above example, the callbacks count the number of clean regions
 * and allows pause_blockcache_clean_alloc to continue only if there is
 * at least one clean region.
 */
namespace facebook {
namespace cachelib {
namespace detail {
void injectPause(folly::StringPiece name);
}

/**
 * Allow stopping the process at this named point.
 */
#define INJECT_PAUSE(name) facebook::cachelib::detail::injectPause(#name)

/**
 * Test helper which enables INJECT_PAUSE and asserts
 * for no pending inject points at scope exit
 *
 * @def ENABLE_INJECT_PAUSE_IN_SCOPE
 */
#define ENABLE_INJECT_PAUSE_IN_SCOPE() \
  injectPauseEnabled() = true;         \
  SCOPE_EXIT { EXPECT_EQ(injectPauseClear(), 0); }

// Callback that can be executed optionally for INJECT_PAUSE point
using PauseCallback = folly::Function<void()>;

// Default timeout for INJECT_PAUSE wait
constexpr uint32_t kInjectPauseMaxWaitTimeoutMs = 60000;

/**
 * Toggle INJECT_PAUSE() logic on and off
 */
bool& injectPauseEnabled();

/**
 * Stop any thread at this named INJECT_PAUSE point from now on.
 * @param numThreads  If nonzero, only stop the first N threads.
 */
void injectPauseSet(folly::StringPiece name, size_t numThreads = 0);

/**
 * The thread will execute the callback at INJECT_PAUSE point and go.
 * @param callback Callback to be executed at INJECT_PAUSE point
 */
void injectPauseSet(folly::StringPiece name, PauseCallback&& callback);

/**
 * If the named INJECT_PAUSE point was set, blocks until the requested
 * number of threads is stopped at it including those already stopped
 * and returns true.
 * @param name       Name of the INJECT_PAUSE point
 * @param numThreads The number of threads to wait for
 * @param wakeup     If true, wakes up any threads blocked at the point
 * @param timeoutMs  If nonzero, waits no longer than this many milliseconds
 * @return           true if waited for requested number of threads successfully
 *                   false otherwise or timed out
 */
bool injectPauseWait(folly::StringPiece name,
                     size_t numThreads = 1,
                     bool wakeup = true,
                     uint32_t timeoutMs = 0);

/**
 * Stop blocking threads at this INJECT_PAUSE point and unblock any
 * currently waiting threads. If name is not given, all of
 * INJECT_PAUSE points are cleared.
 * Returns the number of INJECT_PAUSE points where threads were stopped
 */
size_t injectPauseClear(folly::StringPiece name = "");

} // namespace cachelib
} // namespace facebook
