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

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Collect.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/WithCancellation.h>
#include <gtest/gtest.h>

#include "cachelib/interface/utils/ShardedSerializer.h"

using namespace facebook::cachelib;
using facebook::cachelib::interface::Key;
using facebook::cachelib::interface::utils::ShardedSerializer;

class ShardedSerializerTest : public testing::Test {
 public:
  void SetUp() override {
    serializer_ = std::make_unique<ShardedSerializer>(
        std::make_unique<MurmurHash2>(), /* shardsPower */ 2);
  }

  size_t getShard(const std::string& inKey) const {
    return MurmurHash2()(inKey.data(), inKey.size()) % 4;
  }

  static constexpr Key key = "test_key";
  std::unique_ptr<ShardedSerializer> serializer_;
};

TEST(ShardedSerializerTestStandalone, InvalidArgs) {
  EXPECT_THROW(ShardedSerializer(nullptr, 2), std::invalid_argument);
  EXPECT_THROW(ShardedSerializer(std::make_unique<MurmurHash2>(), 0),
               std::invalid_argument);
  EXPECT_THROW(ShardedSerializer(std::make_unique<MurmurHash2>(),
                                 ShardedSerializer::kMaxShardsPower + 1),
               std::invalid_argument);
}

// ============================================================================
// API tests for rlock() & wlock()
// ============================================================================

CO_TEST_F(ShardedSerializerTest, RlockWlock) {
  {
    auto lock = co_await serializer_->rlock(key);
    EXPECT_TRUE(lock);
  }
  {
    auto lock = co_await serializer_->wlock(key);
    EXPECT_TRUE(lock);
  }
}

// ============================================================================
// API tests for all variants of withRlock & withWlock
// ============================================================================

CO_TEST_F(ShardedSerializerTest, WithRlockCoro) {
  auto res = co_await serializer_->withRlock(
      key, []() -> folly::coro::Task<int> { co_return 42; });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithRlockVoidCoro) {
  int res;
  co_await serializer_->withRlock(key, [&]() -> folly::coro::Task<void> {
    res = 42;
    co_return;
  });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithRlockFunc) {
  auto res = co_await serializer_->withRlock(key, []() { return 42; });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithRlockVoidFunc) {
  int res;
  co_await serializer_->withRlock(key, [&]() { res = 42; });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithWlockCoro) {
  auto res = co_await serializer_->withWlock(
      key, []() -> folly::coro::Task<int> { co_return 42; });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithWlockVoidCoro) {
  int res;
  co_await serializer_->withWlock(key, [&]() -> folly::coro::Task<void> {
    res = 42;
    co_return;
  });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithWlockFunc) {
  auto res = co_await serializer_->withWlock(key, []() { return 42; });
  EXPECT_EQ(res, 42);
}

CO_TEST_F(ShardedSerializerTest, WithWlockVoidFunc) {
  int res;
  co_await serializer_->withWlock(key, [&]() { res = 42; });
  EXPECT_EQ(res, 42);
}

// Test correctness of type trait filtering - we don't want to co_await
// folly::Expected even though it satisfies folly::coro::is_semi_awaitable
CO_TEST_F(ShardedSerializerTest, Expected) {
  auto res = co_await serializer_->withRlock(
      key, []() -> folly::Expected<int, float> { return 42; });
  EXPECT_TRUE(res.hasValue() && res.value() == 42);
  res = co_await serializer_->withWlock(
      key, []() -> folly::Expected<int, float> { return 84; });
  EXPECT_TRUE(res.hasValue() && res.value() == 84);
}

// ============================================================================
// Sharding tests
// ============================================================================

CO_TEST_F(ShardedSerializerTest, ConcurrentSameShard) {
  // Generate several keys that hash to the same shard
  std::vector<std::string> keys;
  keys.reserve(3);
  size_t idx = 0;
  while (keys.size() < 3) {
    std::string tmpKey = "key" + std::to_string(idx++);
    if (getShard(tmpKey) == 0) {
      keys.emplace_back(std::move(tmpKey));
    }
  }

  size_t currentReader = 0;
  size_t currentWriter = 0;
  size_t maxConcurrentReaders = 0;

  // Multiple readers to the same shard are allowed to grab the lock
  auto sharedReadTask = [&](Key key) -> folly::coro::Task<void> {
    co_await serializer_->withRlock(key, [&]() -> folly::coro::Task<void> {
      auto reader = ++currentReader;
      maxConcurrentReaders = std::max(maxConcurrentReaders, reader);
      co_await folly::coro::co_reschedule_on_current_executor;
      --currentReader;
    });
  };
  co_await folly::coro::collectAll(sharedReadTask(keys[0]),
                                   sharedReadTask(keys[1]),
                                   sharedReadTask(keys[2]));
  EXPECT_EQ(maxConcurrentReaders, 3);

  // Readers and writers to the same shard should block each other
  auto readTask = [&](Key key) -> folly::coro::Task<void> {
    co_await serializer_->withRlock(key, [&]() -> folly::coro::Task<void> {
      ++currentReader;
      EXPECT_EQ(currentWriter, 0);
      co_await folly::coro::co_reschedule_on_current_executor;
      --currentReader;
    });
  };
  auto writeTask = [&](Key key) -> folly::coro::Task<void> {
    co_await serializer_->withWlock(key, [&]() -> folly::coro::Task<void> {
      ++currentWriter;
      EXPECT_EQ(currentReader, 0);
      co_await folly::coro::co_reschedule_on_current_executor;
      --currentWriter;
    });
  };
  co_await folly::coro::collectAll(readTask(keys[0]), writeTask(keys[1]));
  co_await folly::coro::collectAll(writeTask(keys[0]), readTask(keys[1]));
}

CO_TEST_F(ShardedSerializerTest, ConcurrentDifferentShards) {
  // Generate several keys that hash to different shards
  std::vector<std::string> keys;
  keys.reserve(2);
  size_t curShard = 0, idx = 0;
  while (keys.size() < 2) {
    std::string tmpKey = "key" + std::to_string(idx++);
    if (getShard(tmpKey) == curShard) {
      keys.emplace_back(std::move(tmpKey));
      curShard++;
    }
  }

  // Counters are safe because test is single-threaded (although tasks are
  // concurrent)
  size_t currentReader = 0;
  size_t currentWriter = 0;

  // Readers and writers to different shards are allowed to grab the lock
  auto readTask = [&](Key key) -> folly::coro::Task<void> {
    co_await serializer_->withRlock(key, [&]() -> folly::coro::Task<void> {
      ++currentReader;
      bool sawWriter = currentWriter > 0;
      co_await folly::coro::co_reschedule_on_current_executor;
      sawWriter |= currentWriter > 0;
      EXPECT_TRUE(sawWriter);
      --currentReader;
    });
  };
  auto writeTask = [&](Key key) -> folly::coro::Task<void> {
    co_await serializer_->withWlock(key, [&]() -> folly::coro::Task<void> {
      ++currentWriter;
      bool sawReader = currentReader > 0;
      co_await folly::coro::co_reschedule_on_current_executor;
      sawReader |= currentReader > 0;
      EXPECT_TRUE(sawReader);
      --currentWriter;
    });
  };

  co_await folly::coro::collectAll(readTask(keys[0]), writeTask(keys[1]));
  co_await folly::coro::collectAll(writeTask(keys[0]), readTask(keys[1]));
}

// ============================================================================
// Cancellation tests
// ============================================================================

// Cancelling a task that is waiting for a lock will prevent it from grabbing
// the lock (and let subsequent tasks grab the lock)
CO_TEST_F(ShardedSerializerTest, CancelledTaskWaitingForLockExits) {
  folly::coro::Baton release;
  folly::CancellationSource cancelSource;
  bool aCompleted = false, bCompleted = false, cCompleted = false;

  auto taskA = [&, this]() -> folly::coro::Task<void> {
    co_await serializer_->withWlock(key, [&]() -> folly::coro::Task<void> {
      co_await release;
      aCompleted = true;
    });
  };
  auto taskB = [&, this]() -> folly::coro::Task<void> {
    co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
        cancelSource.getToken(),
        serializer_->withWlock(key, [&]() { bCompleted = true; })));
  };
  auto taskC = [&, this]() -> folly::coro::Task<void> {
    co_await serializer_->withWlock(key, [&]() { cCompleted = true; });
  };
  auto orchestrator = [&]() -> folly::coro::Task<void> {
    cancelSource.requestCancellation();
    release.post();
    co_return;
  };

  co_await folly::coro::collectAll(taskA(), taskB(), taskC(), orchestrator());
  EXPECT_TRUE(aCompleted);
  EXPECT_FALSE(bCompleted);
  EXPECT_TRUE(cCompleted);
}

// Cancelling a task that has acquired a lock and is executing inside the
// critical section (but not at a cancellation point) does not prevent the task
// from completing
CO_TEST_F(ShardedSerializerTest, CancelledTaskWithLockFinishes) {
  folly::coro::Baton releaseBaton;
  folly::CancellationSource cancelSource;
  bool workCompleted{false};

  auto task = [&]() -> folly::coro::Task<void> {
    co_await folly::coro::co_withCancellation(
        cancelSource.getToken(),
        serializer_->withWlock(key, [&]() -> folly::coro::Task<void> {
          co_await releaseBaton; // doesn't support cancellation
          workCompleted = true;
        }));
  };
  auto orchestrator = [&]() -> folly::coro::Task<void> {
    cancelSource.requestCancellation();
    releaseBaton.post();
    co_return;
  };

  co_await folly::coro::collectAll(task(), orchestrator());
  EXPECT_TRUE(workCompleted);
}

// Cancelling a task that has acquired a lock and is at a cancellation point
// will release the lock (and let subsequent tasks grab the lock)
CO_TEST_F(ShardedSerializerTest, CancelledTaskWithLockExits) {
  folly::CancellationSource cancelSource;
  bool aStarted{false}, aCompleted{false}, bCompleted{false};

  auto taskA = [&]() -> folly::coro::Task<void> {
    co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
        cancelSource.getToken(),
        serializer_->withWlock(key, [&]() -> folly::coro::Task<void> {
          aStarted = true;
          // supports cancellation, token automatically linked to parent
          co_await folly::coro::sleep(std::chrono::seconds(3600));
          aCompleted = true;
        })));
  };
  auto taskB = [&]() -> folly::coro::Task<void> {
    co_await serializer_->withWlock(key, [&]() { bCompleted = true; });
  };
  auto orchestrator = [&]() -> folly::coro::Task<void> {
    cancelSource.requestCancellation();
    co_return;
  };

  co_await folly::coro::collectAll(taskA(), taskB(), orchestrator());
  EXPECT_TRUE(aStarted && !aCompleted);
  EXPECT_TRUE(bCompleted);
}
