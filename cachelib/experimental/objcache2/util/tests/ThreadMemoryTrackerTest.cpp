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

#include <folly/Benchmark.h>
#include <gtest/gtest.h>

#include "cachelib/experimental/objcache2/util/ThreadMemoryTracker.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {
namespace test {
using Object = std::array<char, 16>;

static const size_t expected =
    folly::usingJEMalloc() ? folly::goodMallocSize(sizeof(Object)) : 0;

TEST(ThreadMemoryTrackerTest, Basic) {
  ThreadMemoryTracker tMemTracker;
  auto beforeMemUsage = tMemTracker.getMemUsageBytes();
  auto ptr = std::make_unique<Object>();
  folly::doNotOptimizeAway(ptr);
  auto afterMemUsage = tMemTracker.getMemUsageBytes();
  auto objectSize = LIKELY(afterMemUsage > beforeMemUsage)
                        ? (afterMemUsage - beforeMemUsage)
                        : 0;
  EXPECT_EQ(objectSize, expected);
}

TEST(ThreadMemoryTrackerTest, Iteration) {
  ThreadMemoryTracker tMemTracker;
  for (auto i = 0; i < 100; i++) {
    auto beforeMemUsage = tMemTracker.getMemUsageBytes();
    auto ptr = std::make_unique<Object>();
    folly::doNotOptimizeAway(ptr);
    auto afterMemUsage = tMemTracker.getMemUsageBytes();
    auto objectSize = LIKELY(afterMemUsage > beforeMemUsage)
                          ? (afterMemUsage - beforeMemUsage)
                          : 0;
    EXPECT_EQ(objectSize, expected);
  }
}

} // namespace test
} // namespace objcache2
} // namespace cachelib
} // namespace facebook
