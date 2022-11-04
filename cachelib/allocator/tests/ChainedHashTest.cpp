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

#include "cachelib/allocator/ChainedHashTable.h"
#include "cachelib/allocator/tests/AccessTypeTest.h"

namespace facebook {
namespace cachelib {
namespace tests {

using facebook::cachelib::ChainedHashTable;
using ChainedHashTest = AccessTypeTest<ChainedHashTable>;

TEST(ChainedHashTableConfigTest, Size) {
  using HashConfig = ChainedHashTable::Config;
  HashConfig config{};
  config.sizeBucketsPowerAndLocksPower(1000000);
  EXPECT_EQ(config.getBucketsPower(), 21);
  EXPECT_EQ(config.getLocksPower(), 11);

  ASSERT_THROW(config.sizeBucketsPowerAndLocksPower(2700000000),
               std::invalid_argument);
}

TEST_F(ChainedHashTest, Insert) { testInsert(); }

TEST_F(ChainedHashTest, Replace) { testReplace(); }

TEST_F(ChainedHashTest, Remove) { testRemove(); }

TEST_F(ChainedHashTest, Find) { testFind(); }

TEST_F(ChainedHashTest, HandleIteration) {
  testHandleIterationWithExceptions();
}

TEST_F(ChainedHashTest, RemoveIf) { testRemoveIf(); }

TEST_F(ChainedHashTest, Chaining) {
  using HashConfig = ChainedHashTable::Config;
  const unsigned int bucketsPower = 5;
  const unsigned int locksPower = 3;
  HashConfig config{bucketsPower, locksPower};

  Container c{std::move(config), typename Node::PtrCompressor()};
  std::vector<std::unique_ptr<Node>> nodes;

  // try to insert elements far more than the number of buckets.
  const unsigned int numNodes = 10000;

  for (unsigned int i = 0; i < numNodes; i++) {
    auto key = getRandomNewKey(c);
    nodes.emplace_back(new Node(key));
    auto& node = *nodes.back();
    c.insert(node);
  }

  // should be able to fetch all the nodes back.
  for (const auto& node : nodes) {
    auto handle = c.find(node->getKey());
    ASSERT_TRUE(handle);
  }
}

/* this is a fun test and quite important and notoriously significant which
 * cause mc-cachelib to be rolledback 100%. ChainedHashTable api expect nodes
 * to be right state when calling the APIs. When a corrupt node which is not
 * in hashtable but marked as accessible is removed from the hashtable under
 * certain scenario, it will introduce a loop in the hash chain causing A->A
 * loops.
 */
TEST_F(ChainedHashTest, InsertOrReplaceDeadlock) {
  using HashConfig = ChainedHashTable::Config;
  // single bucket and single lock
  const unsigned int bucketsPower = 0;
  const unsigned int locksPower = 0;
  HashConfig config{bucketsPower, locksPower};

  const auto failReplace = [](Node* n) {
    using Handle = typename Node::Handle;
    if (!n) {
      return Handle{nullptr};
    }

    if (n->shouldTriggerHandleException()) {
      throw std::exception();
    }

    n->incRef();
    return Handle{n};
  };

  Container c{std::move(config), typename Node::PtrCompressor(), failReplace};
  std::vector<std::unique_ptr<Node>> nodes;
  nodes.reserve(3);

  // try to insert elements far more than the number of buckets.
  const std::string firstNodeKey = "first";
  const std::string secondNodeKey = "second";

  Node firstNode(firstNodeKey);
  Node secondNode(secondNodeKey);
  c.insert(firstNode);
  c.insert(secondNode);

  auto& nodeToReplace = secondNode;
  const auto& nodeLeft = firstNode;

  // the second node is at the head and pointing to the first node. Now try
  // to max out the refcount on the second node and call insertOrReplace which
  // would fail to return the old node since it cant grab a handle on the
  // node.
  Node thirdNode(nodeToReplace.getKey());
  auto& replaceMentNode = thirdNode;

  // make the handle maker throw for this insert or replace.
  nodeToReplace.triggerHandleException(true);
  EXPECT_THROW(c.insertOrReplace(replaceMentNode), std::exception);
  nodeToReplace.triggerHandleException(false);
  EXPECT_TRUE(nodeToReplace.isAccessible());
  EXPECT_FALSE(replaceMentNode.isAccessible());
  EXPECT_EQ(&nodeToReplace, c.find(nodeToReplace.getKey()).get());

  // remove the node that was replaced. Since it was never in the hashtable,
  // we should not be able to replace it.
  ASSERT_FALSE(c.remove(replaceMentNode));
  ASSERT_FALSE(replaceMentNode.isAccessible());

  // try to remove the old node and that should not mess up the container.
  EXPECT_TRUE(c.remove(nodeToReplace));

  // this would have dead locked with the bug fixed in D4433961.
  ASSERT_EQ(nullptr, c.find("foobar"));
  ASSERT_EQ(&nodeLeft, c.find(nodeLeft.getKey()).get());
}

TEST_F(ChainedHashTest, Stats) {
  using HashConfig = ChainedHashTable::Config;
  const unsigned int bucketsPower = 5;
  const unsigned int locksPower = 3;
  HashConfig config{bucketsPower, locksPower};

  Container c{std::move(config), typename Node::PtrCompressor()};
  std::vector<std::unique_ptr<Node>> nodes;

  // try to insert elements far more than the number of buckets.
  const unsigned int numNodes = 10000;

  for (unsigned int i = 0; i < numNodes; i++) {
    auto key = getRandomNewKey(c);
    nodes.emplace_back(new Node(key));
    auto& node = *nodes.back();
    c.insert(node);

    ASSERT_EQ(nodes.size(), c.getNumKeys());
  }

  for (unsigned int i = 0; i < numNodes; i++) {
    auto& node = *nodes.back();
    c.remove(node);
    nodes.pop_back();

    ASSERT_EQ(nodes.size(), c.getNumKeys());
  }
}

TEST_F(ChainedHashTest, Config) {
  using HashConfig = ChainedHashTable::Config;
  HashConfig c{10, 5};

  ASSERT_EQ(1 << 10, c.getNumBuckets());
  ASSERT_EQ(1 << 5, c.getNumLocks());

  ASSERT_THROW((HashConfig{33, 20}), std::invalid_argument);
  ASSERT_THROW((HashConfig{32, 33}), std::invalid_argument);

  HashConfig c2{32, 10};
  ASSERT_EQ(static_cast<size_t>(1) << 32, c2.getNumBuckets());
}

TEST_F(ChainedHashTest, Serialization) { testSerialization(); }

TEST_F(ChainedHashTest, IteratorBasic) { testIteratorBasic(); }

TEST_F(ChainedHashTest, IteratorWithInserts) { testIteratorWithInserts(); }

TEST_F(ChainedHashTest, IteratorWithSerialization) {
  testIteratorWithSerialization();
}

TEST_F(ChainedHashTest, IteratorRefCount) {
  Container c;
  std::vector<std::unique_ptr<Node>> nodes;

  // try to insert elements far more than the number of buckets.
  const unsigned int numNodes = 10000;
  for (unsigned int i = 0; i < numNodes; i++) {
    auto key = getRandomNewKey(c);
    nodes.emplace_back(new Node(key));
    auto& node = *nodes.back();
    c.insert(node);
  }

  // iterating should hold a reference.
  for (auto& item : c) {
    ASSERT_EQ(1, item.getRefCount());
  }
}

TEST_F(ChainedHashTest, IteratorWithThrottler) {
  Container c;
  std::vector<std::unique_ptr<Node>> nodes;

  std::set<std::string> existingKeys;

  const unsigned int numNodes = 20000;
  for (unsigned int i = 0; i < numNodes; i++) {
    auto key = getRandomNewKey(c);
    nodes.emplace_back(new Node(key));
    auto& node = *nodes.back();
    c.insert(node);
    existingKeys.insert(key);
  }

  std::set<std::string> visitedKeys;

  facebook::cachelib::util::Throttler::Config config;
  config.sleepMs = 2000;
  config.workMs = 5;
  auto iter1 = c.begin(config);

  // get total time of iteration with throttler
  uint64_t time1 = util::getCurrentTimeMs();
  for (; iter1 != c.end(); ++iter1) {
    visitedKeys.insert(iter1->getKey().str());
  }
  time1 = util::getCurrentTimeMs() - time1;

  ASSERT_EQ(existingKeys, visitedKeys);

  auto iter2 = c.begin();
  visitedKeys.clear();

  // get total time of iteration without throttler
  uint64_t time2 = util::getCurrentTimeMs();
  for (; iter2 != c.end(); ++iter2) {
    visitedKeys.insert(iter2->getKey().str());
  }
  time2 = util::getCurrentTimeMs() - time2;

  ASSERT_EQ(existingKeys, visitedKeys);
  ASSERT_TRUE((time1 > time2 * 5));
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
