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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <stdexcept>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/TestUtils.h"
#include "cachelib/persistence/PersistenceManager.h"
#include "cachelib/persistence/tests/PersistenceManagerMock.h"

namespace facebook::cachelib::tests {

class PersistenceManagerMockTest : public ::testing::Test {
 public:
  PersistenceManagerMockTest()
      : cacheDir_("/tmp/persistence_test" +
                  folly::to<std::string>(folly::Random::rand32())) {
    util::makeDir(cacheDir_);
    config_
        .setCacheSize(kCacheSize) // 100MB
        .setCacheName("test")
        .enableCachePersistence(cacheDir_)
        .usePosixForShm()
        // Disable slab rebalancing
        .enablePoolRebalancing(nullptr, std::chrono::seconds{0})
        .validate(); // will throw if bad config
  }

  ~PersistenceManagerMockTest() {
    Cache::ShmManager::cleanup(cacheDir_, config_.usePosixShm);
    util::removePath(cacheDir_);
  }

 protected:
  folly::IOBuf makeHeader(PersistenceManager& manager,
                          PersistenceType type,
                          int32_t len) {
    return manager.makeHeader(type, len);
  }

  const size_t kCacheSize = 100 * 1024 * 1024; // 100MB
  std::string cacheDir_;
  CacheConfig config_;
};

using ::testing::AtLeast;
using ::testing::InSequence;

TEST_F(PersistenceManagerMockTest, testNoCache) {
  MockPersistenceStreamWriter writer;
  {
    InSequence seq;
    EXPECT_CALL(writer, write(PersistenceManager::DATA_BEGIN_CHAR));
    EXPECT_CALL(writer, write(PersistenceManager::DATA_MARK_CHAR));
  }

  PersistenceManager manager(config_);

  // ShmManager::attachShm will throw since we didn't build cache instance
  EXPECT_THROW(manager.saveCache(writer), std::invalid_argument);
}

TEST_F(PersistenceManagerMockTest, testSaveCache) {
  std::vector<folly::IOBuf> bufs;

  MockPersistenceStreamWriter writer;
  writer.saveBuffers(bufs);
  {
    InSequence seq;
    EXPECT_CALL(writer, write(PersistenceManager::DATA_BEGIN_CHAR));
    EXPECT_CALL(writer, write(PersistenceManager::DATA_MARK_CHAR))
        .Times(AtLeast(1));
    EXPECT_CALL(writer, write(PersistenceManager::DATA_END_CHAR));
  }

  EXPECT_CALL(writer, write(An<folly::IOBuf>())).Times(AtLeast(5));

  {
    Cache cache(Cache::SharedMemNew, config_);
    cache.shutDown();
  }

  PersistenceManager manager(config_);

  manager.saveCache(writer);

  // the serialized header must have same size regardless
  // which PersistenceType and Length
  ASSERT_EQ(makeHeader(manager, PersistenceType::ShmInfo,
                       std::numeric_limits<int32_t>::max())
                .length(),
            *reinterpret_cast<const size_t*>(bufs[0].data()));

  Deserializer deserializer(bufs[1].data(), bufs[1].tail());
  auto header = deserializer.deserialize<PersistenceHeader>();
  ASSERT_EQ(PersistenceType::Versions, header.type().value());
  ASSERT_EQ(bufs[2].length(), header.length().value());

  Deserializer deserializerCfg(bufs[4].data(), bufs[4].tail());
  PersistCacheLibConfig cfg =
      deserializerCfg.deserialize<PersistCacheLibConfig>();
  ASSERT_EQ(config_.getCacheName(), cfg.cacheName().value());
}

TEST_F(PersistenceManagerMockTest, testWriteFail) {
  Cache cache(Cache::SharedMemNew, config_);
  cache.shutDown();

  {
    std::vector<folly::IOBuf> bufs;
    PersistenceManager manager(config_);
    MockPersistenceStreamWriter writer;
    writer.saveBuffers(bufs);
    writer.ExpectCallAt(
        Invoke([&](folly::IOBuf) { throw std::runtime_error("mock error"); }),
        5); // the fifth write call throws exception
    ASSERT_THROW_WITH_MSG(manager.saveCache(writer), std::runtime_error,
                          "mock error");
    ASSERT_EQ(bufs.size(), 4);
  }

  {
    std::vector<folly::IOBuf> bufs;
    PersistenceManager manager(config_);
    MockPersistenceStreamWriter writer;
    writer.saveBuffers(bufs);
    writer.ExpectCallAt(
        Invoke([&](folly::IOBuf) { throw std::runtime_error("mock error"); }),
        9); // the ninth write call throws exception
    ASSERT_THROW_WITH_MSG(manager.saveCache(writer), std::runtime_error,
                          "mock error");
    ASSERT_EQ(bufs.size(), 8);
  }
}

TEST_F(PersistenceManagerMockTest, testRestoreCache) {
  std::unique_ptr<folly::IOBuf> buffer = folly::IOBuf::create(kCacheSize * 2);
  {
    Cache cache(Cache::SharedMemNew, config_);
    cache.shutDown();
    PersistenceManager manager(config_);
    MockPersistenceStreamWriter writer(buffer.get());
    manager.saveCache(writer);
  }

  Cache::ShmManager::cleanup(cacheDir_, config_.usePosixShm);
  util::removePath(cacheDir_);

  {
    // reader returns wrong DATA_CHAR
    PersistenceManager manager(config_);
    MockPersistenceStreamReader reader(buffer->data(), buffer->length());
    reader.ExpectCharCallAt(
        Invoke([&]() -> char { return PersistenceManager::DATA_BEGIN_CHAR; }),
        2);
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::invalid_argument,
                          "Unknown character: \x1C"); // DATA_BEGIN_CHAR
  }

  {
    // reader throw exceptions on the fifth call
    PersistenceManager manager(config_);
    MockPersistenceStreamReader reader(buffer->data(), buffer->length());
    reader.ExpectCallAt(Invoke([&](uint32_t) -> folly::IOBuf {
                          throw std::runtime_error("mock error");
                        }),
                        5); // the fifth read call throws exception
    ASSERT_THROW_WITH_MSG(manager.restoreCache(reader), std::runtime_error,
                          "mock error");
  }

  {
    PersistenceManager manager(config_);
    MockPersistenceStreamReader reader(buffer->data(), buffer->length());
    manager.restoreCache(reader);
    Cache cache(Cache::SharedMemAttach, config_);
  }
}

} // namespace facebook::cachelib::tests
