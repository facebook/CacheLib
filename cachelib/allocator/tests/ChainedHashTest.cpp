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

  config.sizeBucketsPowerAndLocksPower(1);
  EXPECT_EQ(config.getBucketsPower(), 1);
  EXPECT_EQ(config.getLocksPower(), 1);
}

TEST_F(ChainedHashTest, Insert) { testInsert(); }

TEST_F(ChainedHashTest, Replace) { testReplace(); }

TEST_F(ChainedHashTest, Remove) { testRemove(); }

TEST_F(ChainedHashTest, Find) { testFind(); }

TEST_F(ChainedHashTest, HandleIteration) {
  testHandleIterationWithExceptions();
}

TEST_F(ChainedHashTest, RemoveIf) { testRemoveIf(); }

TEST_F(ChainedHashTest, testIteratorMayContainNull) {
  testIteratorMayContainNull();
}

TEST_F(ChainedHashTest, Chaining) {
  using HashConfig = ChainedHashTable::Config;
  const unsigned int bucketsPower = 5;
  const unsigned int locksPower = 3;
  HashConfig config{bucketsPower, locksPower};

  Container c{config, typename Node::PtrCompressor()};
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

  Container c{config, typename Node::PtrCompressor(), failReplace};
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

  Container c{config, typename Node::PtrCompressor()};
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
// Helper to create a TryHandleMakerFn for tests. All acquisitions succeed.
auto makeTryHandleMaker() {
  using Node = ChainedHashTest::Node;
  using Handle = Node::Handle;
  using Container = ChainedHashTest::Container;
  using TryAcquireResult = Container::TryAcquireResult;
  return [](Node* n) -> std::pair<Handle, TryAcquireResult> {
    if (!n) {
      return {Handle{nullptr}, TryAcquireResult::kSkip};
    }
    n->incRef();
    return {Handle{n}, TryAcquireResult::kSuccess};
  };
}

// Helper to create a FindByKeyFn for tests.
auto makeFindByKey(ChainedHashTest::Container& c) {
  using Node = ChainedHashTest::Node;
  using Handle = Node::Handle;
  return [&c](folly::StringPiece key) -> Handle { return c.find(key); };
}

TEST_F(ChainedHashTest, LockGroupIteratorEmpty) {
  Container c;
  auto it = c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c));
  ASSERT_EQ(it, c.endLockGroup());
}

TEST_F(ChainedHashTest, LockGroupIteratorBasic) {
  Container c;
  auto nodes = createSimpleContainer(c);

  std::set<std::string> existingKeys;
  for (const auto& node : nodes) {
    existingKeys.insert(node->getKey().str());
  }

  // All keys should be visited.
  std::set<std::string> visitedKeys;
  for (auto it = c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c));
       it != c.endLockGroup();
       ++it) {
    visitedKeys.insert(it->getKey().str());
  }
  ASSERT_EQ(existingKeys, visitedKeys);
}

TEST_F(ChainedHashTest, LockGroupIteratorRefCount) {
  Container c;
  auto nodes = createSimpleContainer(c);

  // Each item should have refcount 1 while the iterator holds it.
  for (auto it = c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c));
       it != c.endLockGroup();
       ++it) {
    ASSERT_EQ(1, it->getRefCount());
  }
}

TEST_F(ChainedHashTest, LockGroupIteratorAsHandle) {
  Container c;
  auto nodes = createSimpleContainer(c);

  // asHandle() should return a valid handle.
  auto it = c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c));
  ASSERT_NE(it, c.endLockGroup());
  const auto& handle = it.asHandle();
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(handle->getKey(), it->getKey());
}

TEST_F(ChainedHashTest, LockGroupIteratorReset) {
  Container c;
  auto nodes = createSimpleContainer(c);

  auto it = c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c));

  // Advance partway through.
  for (size_t i = 0; i < nodes.size() / 2 && it != c.endLockGroup(); ++i) {
    ++it;
  }

  // Reset and iterate fully.
  it.reset();
  std::set<std::string> afterReset;
  for (; it != c.endLockGroup(); ++it) {
    afterReset.insert(it->getKey().str());
  }

  // After reset, should visit all keys.
  std::set<std::string> existingKeys;
  for (const auto& node : nodes) {
    existingKeys.insert(node->getKey().str());
  }
  ASSERT_EQ(existingKeys, afterReset);
}

TEST_F(ChainedHashTest, LockGroupIteratorMatchesIterator) {
  Container c;
  auto nodes = createSimpleContainer(c);

  // Both iterators should visit exactly the same set of keys.
  std::set<std::string> iterKeys;
  for (auto it = c.begin(); it != c.end(); ++it) {
    iterKeys.insert(it->getKey().str());
  }

  std::set<std::string> lockGroupKeys;
  for (auto it = c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c));
       it != c.endLockGroup();
       ++it) {
    lockGroupKeys.insert(it->getKey().str());
  }

  ASSERT_EQ(iterKeys, lockGroupKeys);
}

TEST_F(ChainedHashTest, LockGroupIteratorRetriesMovingItem) {
  using Handle = Node::Handle;
  using TryAcquireResult = Container::TryAcquireResult;
  using HashConfig = ChainedHashTable::Config;

  HashConfig config{0, 0};
  Container c{config, typename Node::PtrCompressor()};

  Node movingNode(std::string{"moving"});
  Node stableNode(std::string{"stable"});
  ASSERT_TRUE(c.insert(movingNode));
  ASSERT_TRUE(c.insert(stableNode));

  size_t numMovingRetries{0};
  auto tryHandleMaker = [&movingNode, &numMovingRetries](
                            Node* n) -> std::pair<Handle, TryAcquireResult> {
    if (!n) {
      return {Handle{nullptr}, TryAcquireResult::kSkip};
    }
    if (n == &movingNode) {
      ++numMovingRetries;
      return {Handle{nullptr}, TryAcquireResult::kMoving};
    }
    n->incRef();
    return {Handle{n}, TryAcquireResult::kSuccess};
  };

  size_t numFindRetries{0};
  auto findByKey = [&c, &numFindRetries](folly::StringPiece key) -> Handle {
    ++numFindRetries;
    return c.find(key);
  };

  std::set<std::string> visitedKeys;
  for (auto it = c.beginLockGroup(tryHandleMaker, findByKey);
       it != c.endLockGroup();
       ++it) {
    visitedKeys.insert(it->getKey().str());
  }

  const std::set<std::string> expectedKeys{"moving", "stable"};
  ASSERT_EQ(expectedKeys, visitedKeys);
  ASSERT_EQ(1u, numMovingRetries);
  ASSERT_EQ(1u, numFindRetries);
}

TEST_F(ChainedHashTest, LockGroupIteratorStatsSurviveMove) {
  using Handle = Node::Handle;
  using TryAcquireResult = Container::TryAcquireResult;
  using HashConfig = ChainedHashTable::Config;

  HashConfig config{0, 0};
  Container c{config, typename Node::PtrCompressor()};

  Node movingNode(std::string{"moving"});
  Node stableNode(std::string{"stable"});
  ASSERT_TRUE(c.insert(movingNode));
  ASSERT_TRUE(c.insert(stableNode));

  auto tryHandleMaker =
      [&movingNode](Node* n) -> std::pair<Handle, TryAcquireResult> {
    if (!n) {
      return {Handle{nullptr}, TryAcquireResult::kSkip};
    }
    if (n == &movingNode) {
      return {Handle{nullptr}, TryAcquireResult::kMoving};
    }
    n->incRef();
    return {Handle{n}, TryAcquireResult::kSuccess};
  };

  auto it = c.beginLockGroup(tryHandleMaker, makeFindByKey(c));
  const auto statsBeforeMove = it.getStats();
  ASSERT_EQ(2u, statsBeforeMove.visited);
  ASSERT_EQ(0u, statsBeforeMove.skipped);
  ASSERT_EQ(1u, statsBeforeMove.retried);

  auto moved = std::move(it);
  const auto statsAfterMove = moved.getStats();
  ASSERT_EQ(statsBeforeMove.visited, statsAfterMove.visited);
  ASSERT_EQ(statsBeforeMove.skipped, statsAfterMove.skipped);
  ASSERT_EQ(statsBeforeMove.retried, statsAfterMove.retried);
  ASSERT_NE(moved, c.endLockGroup());
}

TEST_F(ChainedHashTest, LockGroupIteratorSkipsTryHandleMakerExceptions) {
  using Handle = Node::Handle;
  using TryAcquireResult = Container::TryAcquireResult;
  using HashConfig = ChainedHashTable::Config;

  HashConfig config{0, 0};
  Container c{config, typename Node::PtrCompressor()};

  Node throwingNode(std::string{"throwing"});
  Node stableNode(std::string{"stable"});
  ASSERT_TRUE(c.insert(throwingNode));
  ASSERT_TRUE(c.insert(stableNode));

  auto tryHandleMaker =
      [&throwingNode](Node* n) -> std::pair<Handle, TryAcquireResult> {
    if (!n) {
      return {Handle{nullptr}, TryAcquireResult::kSkip};
    }
    if (n == &throwingNode) {
      throw exception::RefcountOverflow("");
    }
    n->incRef();
    return {Handle{n}, TryAcquireResult::kSuccess};
  };

  std::set<std::string> visitedKeys;
  EXPECT_NO_THROW({
    for (auto it = c.beginLockGroup(tryHandleMaker, makeFindByKey(c));
         it != c.endLockGroup();
         ++it) {
      visitedKeys.insert(it->getKey().str());
    }
  });

  const std::set<std::string> expectedKeys{"stable"};
  ASSERT_EQ(expectedKeys, visitedKeys);
}

TEST_F(ChainedHashTest, LockGroupIteratorSkipsFindByKeyExceptions) {
  using Handle = Node::Handle;
  using TryAcquireResult = Container::TryAcquireResult;
  using HashConfig = ChainedHashTable::Config;

  HashConfig config{0, 0};
  Container c{config, typename Node::PtrCompressor()};

  Node movingNode(std::string{"moving"});
  Node stableNode(std::string{"stable"});
  ASSERT_TRUE(c.insert(movingNode));
  ASSERT_TRUE(c.insert(stableNode));

  auto tryHandleMaker =
      [&movingNode](Node* n) -> std::pair<Handle, TryAcquireResult> {
    if (!n) {
      return {Handle{nullptr}, TryAcquireResult::kSkip};
    }
    if (n == &movingNode) {
      return {Handle{nullptr}, TryAcquireResult::kMoving};
    }
    n->incRef();
    return {Handle{n}, TryAcquireResult::kSuccess};
  };

  const auto movingKey = movingNode.getKey().str();
  auto findByKey = [&c, &movingKey](folly::StringPiece key) -> Handle {
    if (key == movingKey) {
      throw exception::RefcountOverflow("");
    }
    return c.find(key);
  };

  std::set<std::string> visitedKeys;
  EXPECT_NO_THROW({
    for (auto it = c.beginLockGroup(tryHandleMaker, findByKey);
         it != c.endLockGroup();
         ++it) {
      visitedKeys.insert(it->getKey().str());
    }
  });

  const std::set<std::string> expectedKeys{"stable"};
  ASSERT_EQ(expectedKeys, visitedKeys);
}

TEST_F(ChainedHashTest, LockGroupIteratorFilter) {
  Container c;
  auto nodes = createSimpleContainer(c);

  // Pick a character that some keys contain — use the first char of the
  // first key so we're guaranteed at least one match.
  const char target = nodes.front()->getKey().data()[0];

  auto filter = [target](folly::StringPiece key) {
    return key.size() > 0 && key.data()[0] == target;
  };

  // Collect expected keys by scanning all nodes with the same predicate.
  std::set<std::string> expectedKeys;
  for (const auto& node : nodes) {
    if (filter(node->getKey())) {
      expectedKeys.insert(node->getKey().str());
    }
  }
  ASSERT_FALSE(expectedKeys.empty());

  // Iterate with the filter — only matching items should appear.
  std::set<std::string> filteredKeys;
  for (auto it =
           c.beginLockGroup(makeTryHandleMaker(), makeFindByKey(c), filter);
       it != c.endLockGroup();
       ++it) {
    filteredKeys.insert(it->getKey().str());
  }

  ASSERT_EQ(expectedKeys, filteredKeys);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
