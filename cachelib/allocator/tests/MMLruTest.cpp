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
#include <folly/logging/xlog.h>

#include "cachelib/allocator/MMLru.h"
#include "cachelib/allocator/tests/MMTypeTest.h"

namespace facebook {
namespace cachelib {
using MMLruTest = MMTypeTest<MMLru>;

TEST_F(MMLruTest, AddBasic) { testAddBasic(MMLru::Config{}); }

TEST_F(MMLruTest, RemoveBasic) { testRemoveBasic(MMLru::Config{}); }

TEST_F(MMLruTest, RecordAccessBasic) {
  MMLru::Config c;
  // Change lruRefreshTime to make sure only the first recordAccess bumps
  // the node and subsequent recordAccess invocations do not.
  c.lruRefreshTime = 100;
  testRecordAccessBasic(std::move(c));
}

TEST_F(MMLruTest, RecordAccessWrites) {
  using Nodes = std::vector<std::unique_ptr<Node>>;
  // access the nodes in the container randomly with the given access mode and
  // ensure that nodes are updated in lru with access mode write (read) only
  // when updateOnWrite (updateOnRead) is enabled.

  auto testWithAccessMode = [this](Container& c_, const Nodes& nodes_,
                                   AccessMode mode, bool updateOnWrites,
                                   bool updateOnReads) {
    // accessing must at least update the update time. to do so, first set the
    // updateTime of the node to be in the past.
    const uint32_t timeInPastStart = 100;
    std::vector<uint32_t> prevNodeTime;
    int i = 0;
    for (auto& node : nodes_) {
      auto time = timeInPastStart + i;
      node->setUpdateTime(time);
      ASSERT_EQ(node->getUpdateTime(), time);
      prevNodeTime.push_back(time);
      i++;
    }

    std::vector<int> nodeOrderPrev;
    for (auto itr = c_.getEvictionIterator(); itr; ++itr) {
      nodeOrderPrev.push_back(itr->getId());
    }
    verifyIterationVariants(c_);

    int nAccess = 1000;
    std::set<int> accessedNodes;
    while (nAccess-- || accessedNodes.size() < nodes_.size()) {
      auto& node = *(nodes_.begin() + folly::Random::rand32() % nodes_.size());
      accessedNodes.insert(node->getId());
      c_.recordAccess(*node, mode);
    }

    i = 0;
    const auto now = util::getCurrentTimeSec();
    for (const auto& node : nodes_) {
      if ((mode == AccessMode::kWrite && updateOnWrites) ||
          (mode == AccessMode::kRead && updateOnReads)) {
        ASSERT_GT(node->getUpdateTime(), prevNodeTime[i++]);
        ASSERT_LE(node->getUpdateTime(), now);
      } else {
        ASSERT_EQ(node->getUpdateTime(), prevNodeTime[i++]);
      }
    }

    // after a random set of recordAccess, test the order of the nodes in the
    // lru.
    std::vector<int> nodeOrderCurr;
    for (auto itr = c_.getEvictionIterator(); itr; ++itr) {
      nodeOrderCurr.push_back(itr->getId());
    }
    verifyIterationVariants(c_);

    if ((mode == AccessMode::kWrite && updateOnWrites) ||
        (mode == AccessMode::kRead && updateOnReads)) {
      ASSERT_NE(nodeOrderCurr, nodeOrderPrev);
    } else {
      ASSERT_EQ(nodeOrderCurr, nodeOrderPrev);
    }
  };

  auto createNodes = [](Container& c, Nodes& nodes) {
    // put some nodes in the container and ensure that the recordAccess does not
    // change the fact that the node is still in container.
    const int numNodes = 10;
    for (int i = 0; i < numNodes; i++) {
      nodes.emplace_back(new Node{i});
      auto& node = nodes.back();
      ASSERT_TRUE(c.add(*node));
    }
  };

  MMLru::Config config1{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ false,
                        /* updateOnRead */ false,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c1{config1, {}};

  MMLru::Config config2{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ false,
                        /* updateOnRead */ true,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c2{config2, {}};

  MMLru::Config config3{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ true,
                        /* updateOnRead */ false,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c3{config3, {}};

  MMLru::Config config4{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ true,
                        /* updateOnRead */ true,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c4{config4, {}};

  Nodes nodes1, nodes2, nodes3, nodes4;
  createNodes(c1, nodes1);
  createNodes(c2, nodes2);
  createNodes(c3, nodes3);
  createNodes(c4, nodes4);

  testWithAccessMode(c1, nodes1, AccessMode::kWrite, config1.updateOnWrite,
                     config1.updateOnRead);
  testWithAccessMode(c1, nodes1, AccessMode::kRead, config1.updateOnWrite,
                     config1.updateOnRead);
  testWithAccessMode(c2, nodes2, AccessMode::kWrite, config2.updateOnWrite,
                     config2.updateOnRead);
  testWithAccessMode(c2, nodes2, AccessMode::kRead, config2.updateOnWrite,
                     config2.updateOnRead);
  testWithAccessMode(c3, nodes3, AccessMode::kWrite, config3.updateOnWrite,
                     config3.updateOnRead);
  testWithAccessMode(c3, nodes3, AccessMode::kRead, config3.updateOnWrite,
                     config3.updateOnRead);
  testWithAccessMode(c4, nodes4, AccessMode::kWrite, config4.updateOnWrite,
                     config4.updateOnRead);
  testWithAccessMode(c4, nodes4, AccessMode::kRead, config4.updateOnWrite,
                     config4.updateOnRead);
}

TEST_F(MMLruTest, InsertionPointBasic) {
  MMLru::Config config;
  config.lruRefreshTime = 0;
  config.updateOnWrite = false;
  config.lruInsertionPointSpec = 1;
  Container c{config, {}};

  constexpr auto nNodes = 8;
  using Nodes = std::vector<std::unique_ptr<Node>>;
  Nodes nodes;
  for (int i = 0; i < nNodes; i++) {
    nodes.emplace_back(new Node{i});
  }

  auto checkLruConfig = [&](Container& container, std::vector<int> order) {
    verifyIterationVariants(container);
    auto it = container.getEvictionIterator();
    int i = 0;
    while (it) {
      ASSERT_LT(i, order.size());
      EXPECT_EQ(order[i], it->getId());
      i++;
      ++it;
    }
    ASSERT_EQ(i, order.size());
  };

  // Insert all nodes at head and verify insertion points
  for (size_t i = 0; i < nodes.size(); i++) {
    c.add(*nodes[i]);
  }

  // only half should be tail
  for (size_t i = 0; i < nodes.size(); i++) {
    // expectedTail 0, 3, 5, 7
    bool expectedTail = i == 0 || i == 3 || i == 5 || i == 7;
    EXPECT_EQ(expectedTail, nodes[i]->isTail());
  }

  // verify configuration
  checkLruConfig(c, {0, 3, 5, 7, 6, 4, 2, 1});

  for (int i = 0; i < 4; i++) {
    c.recordAccess(*nodes[i], AccessMode::kRead);
  }
  for (size_t i = 0; i < nodes.size(); i++) {
    bool expectedTail = i >= nodes.size() / 2;
    EXPECT_EQ(expectedTail, nodes[i]->isTail());
  }

  checkLruConfig(c, {5, 7, 6, 4, 0, 1, 2, 3});

  Node midNode{8};
  c.add(midNode);

  checkLruConfig(c, {5, 7, 6, 4, 8, 0, 1, 2, 3});

  // remove all nodes
  for (size_t i = 0; i < nodes.size(); i++) {
    c.remove(*nodes[i]);
  }
  c.remove(midNode);

  // add two nodes, remove last node, add again, replace
  // remove and add again. These operations should excercise
  // the replace code path as well as fixInsetionPointForRemove
  c.add(*nodes[0]);
  c.add(*nodes[1]);
  checkLruConfig(c, {0, 1});

  c.remove(*nodes[1]);
  checkLruConfig(c, {0});

  c.add(*nodes[1]);
  checkLruConfig(c, {0, 1});

  c.replace(*nodes[1], *nodes[2]);
  checkLruConfig(c, {0, 2});

  c.add(*nodes[1]);
  checkLruConfig(c, {0, 1, 2});

  c.remove(*nodes[1]);
  c.remove(*nodes[2]);
  checkLruConfig(c, {0});

  c.add(*nodes[1]);
  checkLruConfig(c, {0, 1});

  // clear nodes
  c.remove(*nodes[1]);
  c.remove(*nodes[0]);

  // add all nodes
  for (size_t i = 0; i < nodes.size(); i++) {
    c.add(*nodes[i]);
  }
  checkLruConfig(c, {0, 3, 5, 7, 6, 4, 2, 1});
  c.remove(*nodes[3]);
  checkLruConfig(c, {0, 5, 7, 6, 4, 2, 1});

  ////////////////// save restore ////////////////
  {
    // save state and restore
    const auto sizeBefore = c.getStats().size;
    auto serializedData = c.saveState();

    // newC should behave the same as c
    Container newC(serializedData, {});
    ASSERT_EQ(sizeBefore, newC.getStats().size);

    // try adding at insertion point
    newC.add(*nodes[3]);
    checkLruConfig(c, {0, 5, 7, 3, 6, 4, 2, 1});

    // few more operations
    newC.remove(*nodes[5]);
    checkLruConfig(c, {0, 7, 3, 6, 4, 2, 1});
    newC.add(*nodes[5]);
    checkLruConfig(c, {0, 7, 3, 5, 6, 4, 2, 1});

    // clear nodes
    for (int i = 0; i < 8; i++) {
      newC.remove(*nodes[i]);
    }
  }

  ////////////////// test 1/4th //////////////////
  {
    config.lruInsertionPointSpec = 2;
    Container newC(config, {});

    for (int i = 0; i < 4; i++) {
      newC.add(*nodes[i]);
    }
    checkLruConfig(newC, {0, 3, 2, 1});
    newC.add(*nodes[4]);
    checkLruConfig(newC, {0, 4, 3, 2, 1});
    newC.add(*nodes[5]);
    checkLruConfig(newC, {0, 5, 4, 3, 2, 1});

    newC.add(*nodes[6]);
    newC.add(*nodes[7]);
    checkLruConfig(newC, {0, 7, 6, 5, 4, 3, 2, 1});

    // now insertion point at 2nd node from tail
    newC.add(midNode);
    checkLruConfig(newC, {0, 7, 8, 6, 5, 4, 3, 2, 1});

    // clear nodes
    for (int i = 0; i < 8; i++) {
      newC.remove(*nodes[i]);
    }
  }

  ////////// test update of insertion point spec //////////
  {
    config.lruInsertionPointSpec = 0;
    Container newC(config, {});

    for (int i = 0; i < 6; i++) {
      newC.add(*nodes[i]);
    }
    checkLruConfig(newC, {0, 1, 2, 3, 4, 5});
    config.lruInsertionPointSpec = 1;
    newC.setConfig(config);
    // after next add, insertion point will be adjusted
    newC.add(*(nodes[6]));
    checkLruConfig(newC, {0, 1, 2, 3, 4, 5, 6});
    newC.add(*(nodes[7]));
    checkLruConfig(newC, {0, 1, 2, 7, 3, 4, 5, 6});
    // verify that nodes are correctly marked as tail
    for (int i = 0; i < 8; i++) {
      bool expectedTail = i == 0 || i == 1 || i == 2 || i == 7;
      EXPECT_EQ(expectedTail, nodes[i]->isTail());
    }
    config.lruInsertionPointSpec = 0;
    // this will have the effect of setting insertion point to nullptr and
    // unmarking tail bits
    newC.setConfig(config);
    // none of the nodes should have the tail bit marked anymore
    for (int i = 0; i < 8; i++) {
      bool expectedTail = false;
      EXPECT_EQ(expectedTail, nodes[i]->isTail());
    }
    newC.remove(*(nodes[5]));
    checkLruConfig(newC, {0, 1, 2, 7, 3, 4, 6});
    newC.add(*(nodes[5]));
    checkLruConfig(newC, {0, 1, 2, 7, 3, 4, 6, 5});
    // clear nodes
    for (int i = 0; i < 8; i++) {
      newC.remove(*nodes[i]);
    }
  }
}

TEST_F(MMLruTest, InsertionPointStress) {
  // given an insertion point spec, run a finite number of random operations
  // on it and ensure that the tail sizes match what we expect it to be.
  auto doStressTest = [&](uint8_t lruInsertionPointSpec) {
    MMLru::Config config;
    config.lruRefreshTime = 0;
    config.lruInsertionPointSpec = lruInsertionPointSpec;
    Container c{config, {}};
    const size_t tailProportion = 1 << lruInsertionPointSpec;

    constexpr auto nNodes = 500;
    using Nodes = std::vector<std::unique_ptr<Node>>;
    Nodes nodes;
    for (int i = 0; i < nNodes; i++) {
      nodes.emplace_back(new Node{i});
    }

    auto getTailCount = [&]() {
      size_t ntail = 0;
      verifyIterationVariants(c);
      auto it = c.getEvictionIterator();
      while (it) {
        if (it->isTail()) {
          ntail++;
        }
        ++it;
      }
      return ntail;
    };

#define PRINT_DEBUG_STR
#undef PRINT_DEBUG_STR

#ifdef PRINT_DEBUG_STR
    auto printLru = [&]() {
      std::string out;
      auto it = c.begin();
      while (it != c.end()) {
        out += folly::sformat("{}:{} ", it->getId(), it->isTail() ? 'T' : 'H');
        ++it;
      }
      XLOG(INFO) << out;
    };

#endif

    // list of nodes currently in lru
    std::unordered_set<int> inLru;

    // add a random node into the lru that is not present.
    // the lru must not be full at this point.
    auto addRandomNode = [&]() {
      if (inLru.size() >= nNodes) {
        return;
      }
      auto n = folly::Random::rand32() % nNodes;
      while (inLru.count(n) != 0) {
        n = folly::Random::rand32() % nNodes;
      }
      c.add(*nodes[n]);
      assert(inLru.count(n) == 0);
      inLru.insert(n);
#ifdef PRINT_DEBUG_STR
      XLOG(INFO) << "add " << n;
      printLru();
#endif
    };

    // removes a random node that is present in the lru.
    auto removeRandomNode = [&]() {
      if (inLru.empty()) {
        return;
      }

      auto n = folly::Random::rand32() % nNodes;
      while (inLru.count(n) == 0) {
        n = folly::Random::rand32() % nNodes;
      }
      c.remove(*nodes[n]);
      assert(inLru.count(n) != 0);
      inLru.erase(n);
#ifdef PRINT_DEBUG_STR
      XLOG(INFO) << "remove " << n;
      printLru();
#endif
    };

    // on a non-empty lru, bump up a random node
    auto recordRandomNode = [&]() {
      if (inLru.empty()) {
        return;
      }

      auto n = folly::Random::rand32() % nNodes;
      while (inLru.count(n) == 0) {
        n = folly::Random::rand32() % nNodes;
      }
      c.recordAccess(*nodes[n], AccessMode::kRead);
#ifdef PRINT_DEBUG_STR
      XLOG(INFO) << "record " << n;
      printLru();
#endif
    };

    int opsToComplete = 100000;
    folly::ThreadLocalPRNG prng = folly::ThreadLocalPRNG();
    std::mt19937 gen(folly::Random::rand32(prng));
    std::uniform_real_distribution<> opDis(0, 1);

    // probabilities for different operation.
    const double addPct = 0.4;
    const double recordAccesPct = 0.9;
    const double removePct = 0.2;

    while (opsToComplete > 0) {
      auto op = opDis(gen);
      if (inLru.size() < nNodes && op < addPct) {
        --opsToComplete;
        addRandomNode();
      }

      if (op < removePct && !inLru.empty()) {
        --opsToComplete;
        removeRandomNode();
      }

      if (op < recordAccesPct && !inLru.empty()) {
        --opsToComplete;
        recordRandomNode();
      }

      // if the lru is not empty, check that the tail size matches what we
      // expect.
      if (!inLru.empty()) {
        const size_t expectedTailSize =
            std::max(size_t(1), inLru.size() / tailProportion);
        ASSERT_EQ(expectedTailSize, getTailCount());
      }
    }
  };

  doStressTest(1);
  doStressTest(2);
  doStressTest(3);
  doStressTest(4);
  doStressTest(8);
}

TEST_F(MMLruTest, Serialization) { testSerializationBasic(MMLru::Config{}); }

TEST_F(MMLruTest, Reconfigure) {
  Container container(MMLru::Config{}, {});
  auto config = container.getConfig();
  config.defaultLruRefreshTime = 1;
  config.lruRefreshTime = 1;
  config.lruRefreshRatio = 0.8;
  config.mmReconfigureIntervalSecs = std::chrono::seconds(4);
  container.setConfig(config);
  std::vector<std::unique_ptr<Node>> nodes;
  nodes.emplace_back(new Node{0});
  container.add(*nodes[0]);
  sleep(1);
  nodes.emplace_back(new Node{1});
  container.add(*nodes[1]);
  sleep(2);

  // node 0 (age 3) gets promoted
  // current time (age 3) < reconfigure interval (4),
  // so refresh time not updated in reconfigureLocked()
  EXPECT_TRUE(container.recordAccess(*nodes[0], AccessMode::kRead));
  EXPECT_FALSE(nodes[0]->isTail());

  sleep(2);
  // current time (age 5) > reconfigure interval (4),
  // so refresh time set to 4 * 0.8 = 3.2 = 3 in reconfigureLocked()
  EXPECT_TRUE(container.recordAccess(*nodes[0], AccessMode::kRead));
  nodes.emplace_back(new Node{2});
  container.add(*nodes[2]);

  // node 0 (age 2) does not get promoted
  EXPECT_FALSE(container.recordAccess(*nodes[0], AccessMode::kRead));
}

TEST_F(MMLruTest, CombinedLockingIteration) {
  MMLruTest::Config config{};
  config.useCombinedLockForIterators = true;
  config.lruRefreshTime = 0;
  Container c(config, {});
  std::vector<std::unique_ptr<Node>> nodes;
  createSimpleContainer(c, nodes);

  // access to move items from cold to warm
  for (auto& node : nodes) {
    ASSERT_TRUE(c.recordAccess(*node, AccessMode::kRead));
  }

  // trying to remove through iterator should work as expected.
  // no need of iter++ since remove will do that.
  verifyIterationVariants(c);
  for (auto iter = c.getEvictionIterator(); iter;) {
    auto& node = *iter;
    ASSERT_TRUE(node.isInMMContainer());

    // this will move the iter.
    c.remove(iter);
    ASSERT_FALSE(node.isInMMContainer());
    if (iter) {
      ASSERT_NE((*iter).getId(), node.getId());
    }
  }
  verifyIterationVariants(c);

  ASSERT_EQ(c.getStats().size, 0);
  for (const auto& node : nodes) {
    ASSERT_FALSE(node->isInMMContainer());
  }
}
} // namespace cachelib
} // namespace facebook
