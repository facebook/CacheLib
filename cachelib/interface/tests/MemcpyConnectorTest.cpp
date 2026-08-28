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

#include <folly/coro/GtestHelpers.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cachelib/common/Time.h"
#include "cachelib/interface/DetachedItem.h"
#include "cachelib/interface/MemcpyConnector.h"
#include "cachelib/interface/components/tests/CacheComponentFactory.h"
#include "cachelib/interface/tests/Utils.h"

using namespace ::testing;
using namespace facebook::cachelib;
using namespace facebook::cachelib::interface;
using namespace facebook::cachelib::interface::test;

namespace {

constexpr uint32_t kTtlSecs{3600};

class ReleaseTrackingCacheItem final : public CacheItem {
 public:
  ReleaseTrackingCacheItem(uint32_t size, size_t& releaseCount)
      : memory_(size), releaseCount_(releaseCount) {}

  uint32_t getCreationTime() const noexcept override { return 0; }
  uint32_t getExpiryTime() const noexcept override { return 0; }
  UnitResult incrementRefCount(CacheComponent&) noexcept override {
    return folly::unit;
  }
  bool decrementRefCount(CacheComponent&) noexcept override {
    ++releaseCount_;
    return false;
  }
  Key getKey() const noexcept override { return "release_tracking"; }
  void* getMemory() const noexcept override {
    return const_cast<char*>(memory_.data());
  }
  uint32_t getMemorySize() const noexcept override {
    return static_cast<uint32_t>(memory_.size());
  }
  uint32_t getTotalSize() const noexcept override {
    return static_cast<uint32_t>(sizeof(*this) + memory_.size());
  }

 private:
  std::vector<char> memory_;
  size_t& releaseCount_;

  void move(void* dest) noexcept override {
    new (dest) ReleaseTrackingCacheItem(static_cast<uint32_t>(memory_.size()),
                                        releaseCount_);
    this->~ReleaseTrackingCacheItem();
  }
};

// The connector is agnostic to which components sit on either end; the fixture
// builds a RAM and a flash component only because descriptors have to come from
// somewhere real.
class MemcpyConnectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ram_ = ramFactory_.create();
    ASSERT_NE(ram_, nullptr) << "Failed to create RAM cache";
    flash_ = flashFactory_.create();
    ASSERT_NE(flash_, nullptr) << "Failed to create flash cache";
  }

  void TearDown() override {
    EXPECT_OK(ram_->shutdown());
    EXPECT_OK(flash_->shutdown());
  }

  folly::coro::Task<void> put(CacheComponent& cache,
                              const std::string& key,
                              const std::string& data) {
    auto descriptor = CO_ASSERT_OK(co_await cache.allocate(
        key, data.size(), util::getCurrentTimeSec(), kTtlSecs));
    std::memcpy(descriptor.mutableData(), data.data(), data.size());
    EXPECT_OK(co_await cache.insert(std::move(descriptor).release()));
  }

  // find -> allocate -> transfer -> insert. An unset @destSize sizes the
  // destination to fit the source; tests pass a mismatched value to force a
  // rejection.
  folly::coro::Task<UnitResult> transferDescriptor(
      ReadDescriptor source,
      CacheComponent& dst,
      std::optional<uint32_t> destSize = std::nullopt) {
    const auto key = source.key().str();
    const auto creationTime = source.creationTime();
    const auto expiryTime = source.expiryTime();
    const auto ttlSecs = expiryTime == 0 ? 0 : expiryTime - creationTime;

    auto allocated = co_await dst.allocate(
        key, destSize.value_or(source.size()), creationTime, ttlSecs);
    if (allocated.hasError()) {
      co_return folly::makeUnexpected(std::move(allocated).error());
    }

    auto moved = co_await connector_.transfer(std::move(source),
                                              std::move(allocated.value()));
    if (moved.hasError()) {
      co_return folly::makeUnexpected(std::move(moved).error());
    }

    co_return co_await dst.insert(std::move(moved.value()).release());
  }

  folly::coro::Task<UnitResult> transfer(
      CacheComponent& src,
      CacheComponent& dst,
      const std::string& key,
      std::optional<uint32_t> destSize = std::nullopt) {
    auto found = co_await src.find(key);
    if (found.hasError()) {
      co_return folly::makeUnexpected(std::move(found).error());
    }
    if (!found->has_value()) {
      co_return makeError(Error::Code::FIND_FAILED, "source miss");
    }

    auto source = std::move(found->value());
    co_return co_await transferDescriptor(std::move(source), dst, destSize);
  }

  folly::coro::Task<void> expectValue(CacheComponent& cache,
                                      const std::string& key,
                                      const std::string& data) {
    auto found = CO_ASSERT_OK(co_await cache.find(key));
    CO_ASSERT_TRUE(found.has_value());
    EXPECT_EQ(
        std::string(static_cast<const char*>(found->data()), found->size()),
        data);
  }

  MemcpyConnector connector_;
  RAMCacheFactory ramFactory_;
  FlashCacheFactory flashFactory_;
  std::unique_ptr<CacheComponent> ram_;
  std::unique_ptr<CacheComponent> flash_;
};

CO_TEST_F(MemcpyConnectorTest, MovesHandleToHandle) {
  const std::string key = "ram_to_flash";
  const std::string data = "bytes_moved_to_flash";

  co_await put(*ram_, key, data);
  EXPECT_OK(co_await transfer(*ram_, *flash_, key));
  co_await expectValue(*flash_, key, data);
}

CO_TEST_F(MemcpyConnectorTest, MovesDetachedItemToHandle) {
  const std::string key = "detached_ram_to_flash";
  const std::string data = "bytes_owned_after_ram_removal";

  co_await put(*ram_, key, data);

  std::optional<DetachedItem> detachedItem;
  {
    auto found = CO_ASSERT_OK(co_await ram_->find(key));
    CO_ASSERT_TRUE(found.has_value());
    auto source = std::move(found.value());
    auto handle = std::move(source).release();
    detachedItem.emplace(*handle);
  }

  const auto creationTime = detachedItem->getCreationTime();
  const auto expiryTime = detachedItem->getExpiryTime();
  EXPECT_TRUE(CO_ASSERT_OK(co_await ram_->remove(key)));

  ReadDescriptor detached(std::move(*detachedItem));
  EXPECT_FALSE(detached.isHandleBacked());
  EXPECT_OK(co_await transferDescriptor(std::move(detached), *flash_));

  auto found = CO_ASSERT_OK(co_await flash_->find(key));
  CO_ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->creationTime(), creationTime);
  EXPECT_EQ(found->expiryTime(), expiryTime);
  EXPECT_EQ(std::string(static_cast<const char*>(found->data()), found->size()),
            data);
}

// An empty value is a real value, not a broken descriptor: the transfer has no
// bytes to copy but must still land an item in the destination.
CO_TEST_F(MemcpyConnectorTest, MovesZeroLengthValue) {
  const std::string key = "zero_length";

  co_await put(*ram_, key, "");
  EXPECT_OK(co_await transfer(*ram_, *flash_, key));

  auto found = CO_ASSERT_OK(co_await flash_->find(key));
  CO_ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->size(), 0u);
}

// Truncating would leave a silently short item that is corrupt when read back,
// so an undersized destination is rejected outright.
CO_TEST_F(MemcpyConnectorTest, RejectsUndersizedDestination) {
  const std::string key = "undersized";
  const std::string data = "a much longer payload than the destination";

  co_await put(*ram_, key, data);
  EXPECT_ERROR(co_await transfer(*ram_, *flash_, key, /* destSize */ 8),
               Error::Code::INVALID_ARGUMENTS);
}

CO_TEST_F(MemcpyConnectorTest, FailedTransferReleasesDestination) {
  size_t sourceReleaseCount{0};
  size_t destinationReleaseCount{0};
  ReleaseTrackingCacheItem sourceItem(16, sourceReleaseCount);
  ReleaseTrackingCacheItem destinationItem(8, destinationReleaseCount);
  auto sourceHandle =
      CO_ASSERT_OK(tryCreateHandle<ReadHandle>(*ram_, sourceItem));
  auto destinationHandle =
      CO_ASSERT_OK(tryCreateHandle<AllocatedHandle>(*ram_, destinationItem));

  EXPECT_ERROR(co_await connector_.transfer(
                   ReadDescriptor(std::move(sourceHandle)),
                   AllocatedDescriptor(std::move(destinationHandle))),
               Error::Code::INVALID_ARGUMENTS);

  EXPECT_EQ(sourceReleaseCount, 1);
  EXPECT_EQ(destinationReleaseCount, 1);
}

// The mirror image of truncation: an item's size comes from its allocation, so
// an oversized destination would report the tail the transfer never wrote as
// value bytes, with no way for a reader to recover the real length.
CO_TEST_F(MemcpyConnectorTest, RejectsOversizedDestination) {
  const std::string key = "oversized";
  const std::string data = "short_payload";

  co_await put(*ram_, key, data);
  EXPECT_ERROR(co_await transfer(
                   *ram_, *flash_, key, static_cast<uint32_t>(data.size() + 8)),
               Error::Code::INVALID_ARGUMENTS);
}

// Reading through a moved-from descriptor would dereference its empty handle,
// so transfer() rejects one instead of crashing.
CO_TEST_F(MemcpyConnectorTest, RejectsMovedFromSource) {
  const std::string key = "moved_from_source";
  const std::string data = "payload";
  co_await put(*ram_, key, data);

  auto found = CO_ASSERT_OK(co_await ram_->find(key));
  CO_ASSERT_TRUE(found.has_value());
  ReadDescriptor source(std::move(found.value()));
  const ReadDescriptor stolen(std::move(source));

  auto allocated = CO_ASSERT_OK(co_await flash_->allocate(
      key, data.size(), util::getCurrentTimeSec(), kTtlSecs));

  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_ERROR(
      co_await connector_.transfer(std::move(source), std::move(allocated)),
      Error::Code::INVALID_ARGUMENTS);
}

// The destination half of the same guard: writing through a moved-from
// descriptor would dereference its empty handle.
CO_TEST_F(MemcpyConnectorTest, RejectsMovedFromDestination) {
  const std::string key = "moved_from_destination";
  const std::string data = "payload";
  co_await put(*ram_, key, data);

  auto found = CO_ASSERT_OK(co_await ram_->find(key));
  CO_ASSERT_TRUE(found.has_value());

  auto allocated = CO_ASSERT_OK(co_await flash_->allocate(
      key, data.size(), util::getCurrentTimeSec(), kTtlSecs));
  AllocatedDescriptor destination(std::move(allocated));
  const AllocatedDescriptor stolen(std::move(destination));

  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_ERROR(
      co_await connector_.transfer(ReadDescriptor(std::move(found.value())),
                                   std::move(destination)),
      Error::Code::INVALID_ARGUMENTS);
}

} // namespace
