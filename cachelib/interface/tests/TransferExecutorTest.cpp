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

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include "cachelib/interface/MemcpyConnector.h"
#include "cachelib/interface/TransferExecutor.h"
#include "cachelib/interface/components/tests/CacheComponentFactory.h"
#include "cachelib/interface/tests/TransferTestUtils.h"
#include "cachelib/interface/tests/Utils.h"

using namespace facebook::cachelib::interface;
using namespace facebook::cachelib::interface::test;

namespace {

std::string valueOf(const ReadDescriptor& descriptor) {
  return std::string(static_cast<const char*>(descriptor.data()),
                     descriptor.size());
}

class BlockingConnector final : public Connector {
 public:
  explicit BlockingConnector(size_t expectedConcurrentTransfers)
      : expectedConcurrentTransfers_(expectedConcurrentTransfers) {}

  folly::coro::Task<Result<AllocatedDescriptor>> transfer(
      ReadDescriptor source, AllocatedDescriptor destination) override {
    {
      std::unique_lock lock{mutex_};
      ++startedTransfers_;
      startedCv_.notify_all();
      releaseCv_.wait(lock, [this] { return released_; });
    }

    std::memcpy(destination.mutableData(), source.data(), source.size());
    co_return std::move(destination);
  }

  bool waitForConcurrentTransfers() {
    std::unique_lock lock{mutex_};
    return startedCv_.wait_for(lock, std::chrono::seconds(5), [this] {
      return startedTransfers_ >= expectedConcurrentTransfers_;
    });
  }

  void release() {
    {
      std::lock_guard lock{mutex_};
      released_ = true;
    }
    releaseCv_.notify_all();
  }

 private:
  const size_t expectedConcurrentTransfers_;
  std::mutex mutex_;
  std::condition_variable startedCv_;
  std::condition_variable releaseCv_;
  size_t startedTransfers_{0};
  bool released_{false};
};

class TransferExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    destination_ = factory_.create();
    ASSERT_NE(destination_, nullptr);
  }

  void TearDown() override { EXPECT_OK(destination_->shutdown()); }

  RAMCacheFactory factory_;
  std::unique_ptr<CacheComponent> destination_;
  MemcpyConnector connector_;
};

TEST_F(TransferExecutorTest, TransfersItem) {
  TransferExecutor executor;

  EXPECT_OK(executor.submit(
      makeDetachedItem("key", "value", 100, 0), *destination_, connector_));
  executor.closeAndDrain();

  auto found = ASSERT_OK(folly::coro::blockingWait(destination_->find("key")));
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->key(), "key");
  EXPECT_EQ(valueOf(*found), "value");
  EXPECT_EQ(found->creationTime(), 100);
  EXPECT_EQ(found->expiryTime(), 0);
}

TEST_F(TransferExecutorTest, UsesConfiguredThreadCount) {
  TransferExecutor executor(2);
  BlockingConnector connector(2);

  EXPECT_OK(executor.submit(
      makeDetachedItem("key1", "value1"), *destination_, connector));
  EXPECT_OK(executor.submit(
      makeDetachedItem("key2", "value2"), *destination_, connector));

  EXPECT_TRUE(connector.waitForConcurrentTransfers());
  connector.release();
  executor.closeAndDrain();
}

TEST(TransferExecutorConstructionTest, RejectsZeroThreads) {
  EXPECT_THROW((void)TransferExecutor(0), std::invalid_argument);
}

TEST_F(TransferExecutorTest, DropsItemWithInvalidExpiry) {
  TransferExecutor executor;

  EXPECT_OK(executor.submit(
      makeDetachedItem("key", "value", 100, 99), *destination_, connector_));
  executor.closeAndDrain();

  auto found = ASSERT_OK(folly::coro::blockingWait(destination_->find("key")));
  EXPECT_FALSE(found.has_value());
}

TEST_F(TransferExecutorTest, DestructorDrainsAcceptedWork) {
  {
    TransferExecutor executor;
    EXPECT_OK(executor.submit(
        makeDetachedItem("key", "value"), *destination_, connector_));
  }

  auto found = ASSERT_OK(folly::coro::blockingWait(destination_->find("key")));
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(valueOf(*found), "value");
}

TEST_F(TransferExecutorTest, RejectsWorkAfterClose) {
  TransferExecutor executor;
  executor.closeAndDrain();

  EXPECT_ERROR(executor.submit(
                   makeDetachedItem("key", "value"), *destination_, connector_),
               Error::Code::SHUTDOWN_FAILED);

  auto found = ASSERT_OK(folly::coro::blockingWait(destination_->find("key")));
  EXPECT_FALSE(found.has_value());
}

} // namespace
