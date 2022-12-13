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
#include <cachelib/allocator/Cache.h>
#include <cachelib/allocator/datastruct/DList.h>
#include <cachelib/common/Time.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace facebook {
namespace cachelib {

// Tests for a given MMType. See MMLruTest.cpp for an example.
template <typename MMType>
class MMTypeTest : public testing::Test {
 public:
  // our own implementation of node that will be put inside the container.
  struct Node {
    enum Flags {
      kMMFlag0 = 0,
      kMMFlag1 = 1,
      kMMFlag2 = 2,
    };

    explicit Node(int id, std::string key)
        : id_(id), key_(key), inContainer_{false} {}

    explicit Node(int id)
        : Node(id, std::to_string(util::getCurrentTimeNs())) {}

    Node(const Node&) = delete;
    Node(const Node&&) = delete;
    void operator=(const Node&) = delete;
    void operator=(const Node&&) = delete;

    int getId() const noexcept { return id_; }

    folly::StringPiece getKey() const noexcept { return key_; }

    void setUpdateTime(uint32_t time) noexcept { mmHook_.setUpdateTime(time); }

    uint32_t getUpdateTime() noexcept { return mmHook_.getUpdateTime(); }

    // Mock size, no memory is actually allocated
    // In real Nodes, getSize() is size of the requested allocation.
    // That is, [getMemory(), getMemory() + getSize()) is usable.
    uint32_t getSize() const noexcept { return 1 << 10; }

    template <Flags flagBit>
    void setFlag() {
      __sync_fetch_and_or(
          &flags_, static_cast<uint8_t>(1) << static_cast<uint8_t>(flagBit));
    }

    template <Flags flagBit>
    void unSetFlag() {
      __sync_fetch_and_and(
          &flags_, ~(static_cast<uint8_t>(1) << static_cast<uint8_t>(flagBit)));
    }

    template <Flags flagBit>
    bool isFlagSet() const {
      return flags_ &
             (static_cast<uint8_t>(1) << static_cast<uint8_t>(flagBit));
    }

    bool isTail() { return isFlagSet<kMMFlag0>(); }

    bool isInMMContainer() const noexcept { return inContainer_; }

   protected:
    void markInMMContainer() {
      ASSERT_FALSE(inContainer_);
      inContainer_ = true;
    }

    void unmarkInMMContainer() {
      ASSERT_TRUE(inContainer_);
      inContainer_ = false;
    }

   public:
    // Node does not perform pointer compression, but it needs to supply a dummy
    // PtrCompressor
    struct CACHELIB_PACKED_ATTR CompressedPtr {
     public:
      // default construct to nullptr.
      CompressedPtr() = default;

      explicit CompressedPtr(int64_t ptr) : ptr_(ptr) {}

      int64_t saveState() const noexcept { return ptr_; }

      int64_t ptr_{};
    };

    struct PtrCompressor {
      CompressedPtr compress(const Node* uncompressed) const noexcept {
        return CompressedPtr{reinterpret_cast<int64_t>(uncompressed)};
      }

      Node* unCompress(CompressedPtr compressed) const noexcept {
        return reinterpret_cast<Node*>(compressed.ptr_);
      }
    };

    typename MMType::template Hook<Node> mmHook_;

   private:
    int id_{-1};
    std::string key_;
    bool inContainer_{false};
    uint8_t flags_{0};
    friend typename MMType::template Container<Node, &Node::mmHook_>;
    friend class MMTypeTest<MMType>;
  };

  // container definition for the tests.
  using Container = typename MMType::template Container<Node, &Node::mmHook_>;

  using Config = typename MMType::Config;

  void createSimpleContainer(Container& c,
                             std::vector<std::unique_ptr<Node>>& nodes);

  void testAddBasic(Container& c, std::vector<std::unique_ptr<Node>>& nodes);

  void testAddBasic(Config c);
  void testRemoveBasic(Config c);
  void testRecordAccessBasic(Config c);
  void testSerializationBasic(Config c);
  void testIterate(std::vector<std::unique_ptr<Node>>& nodes, Container& c);
  void testMatch(std::string expected, Container& c);
  size_t getListSize(const Container& c, typename MMType::LruType list);
  void verifyIterationVariants(Container& c);
};

template <typename MMType>
void MMTypeTest<MMType>::createSimpleContainer(
    Container& c, std::vector<std::unique_ptr<Node>>& nodes) {
  const int numNodes = 10;
  for (int i = 0; i < numNodes; i++) {
    nodes.emplace_back(new Node{i});
    ASSERT_FALSE(nodes.back()->isInMMContainer());
  }

  ASSERT_EQ(c.getStats().size, 0);

  for (auto& node : nodes) {
    ASSERT_TRUE(c.add(*node));
    ASSERT_TRUE(node->isInMMContainer());
  }

  ASSERT_EQ(c.getStats().size, nodes.size());
}

template <typename MMType>
void MMTypeTest<MMType>::testAddBasic(
    Container& c, std::vector<std::unique_ptr<Node>>& nodes) {
  // adding elements that are already added to the container should error out.
  for (auto& node : nodes) {
    ASSERT_TRUE(node->isInMMContainer());
    ASSERT_FALSE(c.add(*node));
    // this should not change the node state.
    ASSERT_TRUE(node->isInMMContainer());
  }

  std::set<int> foundNodes;
  for (auto itr = c.getEvictionIterator(); itr; ++itr) {
    foundNodes.insert(itr->getId());
  }
  EXPECT_EQ(foundNodes.size(), c.getStats().size);
  EXPECT_EQ(foundNodes.size(), c.size());
  verifyIterationVariants(c);
}

template <typename MMType>
void MMTypeTest<MMType>::testAddBasic(Config config) {
  // add some nodes to the container and ensure that they are marked
  // accessible and can be traversed through the eviction iterator.

  Container c(config, {});
  std::vector<std::unique_ptr<Node>> nodes;
  createSimpleContainer(c, nodes);

  testAddBasic(c, nodes);
}

template <typename MMType>
void MMTypeTest<MMType>::testRemoveBasic(Config config) {
  Container c(config, {});
  std::vector<std::unique_ptr<Node>> nodes;
  createSimpleContainer(c, nodes);
  const unsigned int numNodes = c.getStats().size;
  EXPECT_EQ(c.size(), numNodes);

  // try to remove some random nodes and ensure that they are not in the
  // contianer anymore and that they are marked as not in MMContainer.
  std::vector<std::unique_ptr<Node>> removedNodes;
  for (unsigned int i = 0; i < numNodes / 3; i++) {
    auto iter = nodes.begin() + folly::Random::rand32() % nodes.size();
    auto node = std::move(*iter);
    ASSERT_TRUE(node->isInMMContainer());
    ASSERT_TRUE(c.remove(*node));
    ASSERT_FALSE(node->isInMMContainer());
    nodes.erase(iter);
    removedNodes.push_back(std::move(node));
  }

  // removing nodes that are not in the container should just error out.
  for (auto& node : removedNodes) {
    ASSERT_FALSE(node->isInMMContainer());
    ASSERT_FALSE(c.remove(*node));
  }

  std::set<int> foundNodes;
  for (auto itr = c.getEvictionIterator(); itr; ++itr) {
    foundNodes.insert(itr->getId());
  }
  verifyIterationVariants(c);

  for (const auto& node : removedNodes) {
    ASSERT_EQ(foundNodes.find(node->getId()), foundNodes.end());
  }
  // trying to remove through iterator should work as expected as well.
  // no need of iter++ since remove will do that.
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

  EXPECT_EQ(c.getStats().size, 0);
  EXPECT_EQ(c.size(), 0);
  for (const auto& node : nodes) {
    ASSERT_FALSE(node->isInMMContainer());
  }
}

template <typename MMType>
void MMTypeTest<MMType>::testRecordAccessBasic(Config config) {
  Container c(config, {});
  std::vector<std::unique_ptr<Node>> nodes;
  createSimpleContainer(c, nodes);

  // accessing must at least update the update time. to do so, first set the
  // updateTime of the node to be in the past.
  const uint32_t timeInPastStart = 100;
  std::vector<uint32_t> prevNodeTime;
  int i = 0;
  for (auto& node : nodes) {
    auto time = timeInPastStart + i;
    node->setUpdateTime(time);
    ASSERT_EQ(node->getUpdateTime(), time);
    prevNodeTime.push_back(time);
    i++;
  }

  int nAccess = 1000;
  std::set<int> accessedNodes;
  while (nAccess-- || accessedNodes.size() < nodes.size()) {
    auto& node = *(nodes.begin() + folly::Random::rand32() % nodes.size());
    auto it = accessedNodes.find(node->getId());
    ASSERT_TRUE(node->isInMMContainer());
    // If the node is in accessedNodes, it means recordAccess() was previously
    // called, and hence second time recordAccess should return FALSE. The
    // first time recordAccess should have bumped the node and return TRUE.
    // This change is done to make sure both return values are tested.
    if (it != accessedNodes.end()) {
      ASSERT_FALSE(c.recordAccess(*node, AccessMode::kRead));
    } else {
      accessedNodes.insert(node->getId());
      ASSERT_TRUE(c.recordAccess(*node, AccessMode::kRead));
    }
    ASSERT_TRUE(node->isInMMContainer());
  }

  i = 0;
  const auto now = util::getCurrentTimeSec();
  for (const auto& node : nodes) {
    ASSERT_GT(node->getUpdateTime(), prevNodeTime[i++]);
    ASSERT_LE(node->getUpdateTime(), now);
  }
}

template <typename MMType>
void MMTypeTest<MMType>::testSerializationBasic(Config config) {
  // add some nodes to the container and ensure that they are marked
  // accessible and can be traversed through the eviction iterator.
  Container c1(config, {});
  std::vector<std::unique_ptr<Node>> nodes;
  createSimpleContainer(c1, nodes);

  testAddBasic(c1, nodes);

  // save state and restore
  auto serializedData = c1.saveState();

  // c2 should behave the same as c1
  Container c2(serializedData, {});
  ASSERT_EQ(c2.getStats().size, nodes.size());

  for (auto& node : nodes) {
    bool foundNode = false;
    for (auto it = c2.getEvictionIterator(); it; ++it) {
      if (&*node == &*it) {
        foundNode = true;
        break;
      }
    }
    verifyIterationVariants(c2);
    ASSERT_TRUE(foundNode);
    foundNode = false;
  }

  testAddBasic(c2, nodes);
}

template <typename MMType>
size_t MMTypeTest<MMType>::getListSize(const Container& c,
                                       typename MMType::LruType list) {
  if (list < MMType::LruType::NumTypes) {
    return c.lru_.getList(list).size();
  }
  throw std::invalid_argument("LruType not existing");
}

// Verifies that using getEvictionIterator and withEvictionIterator
// yields the same elements, in the same order.
template <typename MMType>
void MMTypeTest<MMType>::verifyIterationVariants(Container& c) {
  std::vector<Node*> nodes;
  for (auto iter = c.getEvictionIterator(); iter; ++iter) {
    nodes.push_back(&(*iter));
  }

  size_t i = 0;
  c.withEvictionIterator([&nodes, &i](auto&& iter) {
    while (iter) {
      auto& node = *iter;
      ASSERT_EQ(&node, nodes[i]);

      ++iter;
      ++i;
    }

    ASSERT_EQ(i, nodes.size());
  });
}
} // namespace cachelib
} // namespace facebook
