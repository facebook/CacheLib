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

#include <future>
#include <mutex>
#include <thread>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/tests/TestBase.h"

using Item = facebook::cachelib::LruAllocator::Item;
using ChainedItem = facebook::cachelib::LruAllocator::ChainedItem;

namespace facebook {
namespace cachelib {

TEST(ItemTest, NonStringKey) {
  constexpr uint32_t bufferSize = 100;
  char buffer[bufferSize];

  const uint32_t valueSize = bufferSize / 2;

  struct KeyType {
    uint32_t id1;
    uint32_t id2;
    uint32_t id3;
  };

  auto keyType = KeyType{1, 2, 3};

  const auto key = util::castToKey(keyType);

  auto item = new (buffer) Item(key, valueSize, 0, 0);

  ASSERT_EQ(key, item->getKey());
}

TEST(ItemTest, CreationTime) {
  constexpr uint32_t bufferSize = 100;
  char buffer[bufferSize];

  const uint32_t valueSize = bufferSize / 2;

  const folly::StringPiece key = "helloworld";
  const uint32_t now = util::getCurrentTimeSec();

  auto item = new (buffer) Item(key, valueSize, now, 0);
  ASSERT_EQ(now, item->getCreationTime());
}

TEST(ItemTest, ExpiryTime) {
  constexpr uint32_t bufferSize = 100;
  char buffer[bufferSize];

  const uint32_t valueSize = bufferSize / 2;

  const folly::StringPiece key = "helloworld";
  const uint32_t now = util::getCurrentTimeSec();
  const auto tenMins = std::chrono::seconds(600);
  const uint32_t tenMinutesLater = now + tenMins.count();

  auto item = new (buffer) Item(key, valueSize, now, 0);
  item->markInMMContainer();
  EXPECT_EQ(0, item->getExpiryTime());
  EXPECT_EQ(0, item->getConfiguredTTL().count());

  // Test that the write went through
  bool result = item->updateExpiryTime(tenMinutesLater);
  EXPECT_TRUE(result);
  EXPECT_EQ(tenMins, item->getConfiguredTTL());

  // Test that writes fail while the item is moving
  result = item->markMoving();
  EXPECT_TRUE(result);
  result = item->updateExpiryTime(0);
  EXPECT_FALSE(result);
  item->unmarkMoving();

  // Test that writes fail while the item is marked for eviction
  item->markAccessible();
  result = item->markForEviction();
  EXPECT_TRUE(result);
  result = item->updateExpiryTime(0);
  EXPECT_FALSE(result);
  item->unmarkForEviction();
  item->unmarkAccessible();

  // Test that writes fail while the item is not in an MMContainer
  item->unmarkInMMContainer();
  result = item->updateExpiryTime(0);
  EXPECT_FALSE(result);
  item->markInMMContainer();

  // Test that writes fail while the item is a chained item
  item->markIsChainedItem();
  result = item->updateExpiryTime(0);
  EXPECT_FALSE(result);
  item->unmarkIsChainedItem();

  // Test that the newly written value is what was expected
  EXPECT_EQ(tenMinutesLater, item->getExpiryTime());

  // Extend expiry time (5 minutes)
  auto timeBeforeExtend = util::getCurrentTimeSec();
  auto fiveMins = std::chrono::seconds(300);
  item->extendTTL(fiveMins);
  EXPECT_LE(timeBeforeExtend + fiveMins.count(), item->getExpiryTime());
  EXPECT_EQ(fiveMins, item->getConfiguredTTL());

  result = item->updateExpiryTime(0);
  EXPECT_TRUE(result);
  EXPECT_EQ(0, item->getConfiguredTTL().count());
}

// Make a normal item and verify it's not a chained item
// Then make a chained item and verify it is a chained item
// Finally pass invalid size when constructing another chained item,
// it should throw
TEST(ItemTest, ChainedItemConstruction) {
  constexpr uint32_t bufferSize = 100;
  char buffer1[bufferSize];
  char buffer2[bufferSize];

  const uint32_t valueSize = bufferSize / 2;

  const folly::StringPiece key = "helloworld";
  auto regularItem = new (buffer1) Item(key, valueSize, 0, 0);
  ASSERT_FALSE(regularItem->isChainedItem());
  ASSERT_FALSE(regularItem->hasChainedItem());

  const CompressedPtr dummyCompressedPtr;
  auto chainedItem =
      new (buffer2) ChainedItem(dummyCompressedPtr, valueSize, 0);
  ASSERT_TRUE(chainedItem->isChainedItem());

  // hasChainedItem is set in CacheAllocator::addChainedItem if it's
  // not already set, so here it should still be false
  ASSERT_FALSE(regularItem->hasChainedItem());
}

TEST(ItemTest, ChangeKey) {
  constexpr uint32_t bufferSize = 100;
  char buffer[bufferSize];

  const uint32_t valueSize = bufferSize / 2;

  const folly::StringPiece key = "helloworld";
  const uint32_t now = util::getCurrentTimeSec();

  auto item = new (buffer) Item(key, valueSize, now, 0);
  ASSERT_EQ(now, item->getCreationTime());

  ASSERT_THROW(item->changeKey("helloworl1"), std::invalid_argument);
  ASSERT_THROW(item->changeKey("helloworl12"), std::invalid_argument);

  CompressedPtr dummyCompressedPtr;
  auto chainedItem =
      new (buffer) ChainedItem(dummyCompressedPtr, valueSize, now);
  const auto size = item->getSize();

  ++*reinterpret_cast<uint8_t*>(&dummyCompressedPtr);
  ASSERT_NO_THROW(chainedItem->changeKey(dummyCompressedPtr));

  ASSERT_EQ(size, item->getSize());
}

TEST(ItemTest, ToString) {
  constexpr uint32_t bufferSize = 100;
  char buffer[bufferSize];

  const uint32_t valueSize = bufferSize / 2;

  const folly::StringPiece key = "helloworld";
  const uint32_t now = util::getCurrentTimeSec();

  auto item = new (buffer) Item(key, valueSize, now, 0);
  auto keyStr = item->toString();
  EXPECT_NE(keyStr.find("key=helloworld"), std::string::npos);
  EXPECT_NE(keyStr.find(folly::hexlify("helloworld")), std::string::npos);

  struct CACHELIB_PACKED_ATTR HexKey {
    char _[5];
  };
  HexKey hexKey{{0, 1, 2, 3, 4}};
  auto hexItem = new (buffer)
      Item(folly::StringPiece{reinterpret_cast<const char*>(&hexKey),
                              sizeof(HexKey)},
           valueSize, now, 0);
  auto hexKeyStr = hexItem->toString();
  EXPECT_NE(hexKeyStr.find(folly::humanify(std::string{
                reinterpret_cast<const char*>(&hexKey), sizeof(HexKey)})),
            std::string::npos);
  EXPECT_NE(hexKeyStr.find(folly::hexlify(std::string{
                reinterpret_cast<const char*>(&hexKey), sizeof(HexKey)})),
            std::string::npos);

  CompressedPtr dummyCompressedPtr;
  auto chainedItem =
      new (buffer) ChainedItem(dummyCompressedPtr, valueSize, now);
  chainedItem->toString();
}
} // namespace cachelib
} // namespace facebook
