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

#include <gtest/gtest.h>

#include <memory>

#include "cachelib/allocator/datastruct/DList.h"

namespace facebook {
namespace cachelib {
namespace tests {

class DListTest : public ::testing::Test {};

class DListNode {
 public:
  DListNode(const DListNode&) = delete;
  DListNode& operator=(const DListNode&) = delete;

  DListNode(DListNode&&) = default;
  DListNode& operator=(DListNode&&) = default;

  explicit DListNode() noexcept {}

  using CompressedPtr = DListNode*;

  struct PtrCompressor {
    constexpr CompressedPtr compress(DListNode* uncompressed) const noexcept {
      return uncompressed;
    }

    constexpr DListNode* unCompress(CompressedPtr compressed) const noexcept {
      return compressed;
    }
  };

  DListHook<DListNode> hook_{};
};
} // namespace tests
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib;
using namespace facebook::cachelib::tests;

using DListImpl = DList<DListNode, &DListNode::hook_>;

TEST_F(DListTest, ReplaceHeadTail) {
  DListImpl list{DListNode::PtrCompressor{}};

  DListNode oldNode;
  list.linkAtHead(oldNode);

  DListNode newNode;
  list.replace(oldNode, newNode);

  ASSERT_EQ(list.getHead(), &newNode);
  ASSERT_EQ(list.getTail(), &newNode);

  ASSERT_EQ(list.getPrev(oldNode), nullptr);
  ASSERT_EQ(list.getNext(oldNode), nullptr);

  ASSERT_EQ(list.getPrev(newNode), nullptr);
  ASSERT_EQ(list.getNext(newNode), nullptr);
}

TEST_F(DListTest, ReplaceMid) {
  DListImpl list{DListNode::PtrCompressor{}};

  DListNode node1, node2, node3;
  list.linkAtHead(node3);
  list.linkAtHead(node2);
  list.linkAtHead(node1);

  DListNode newNode;
  list.replace(node2, newNode);

  ASSERT_EQ(list.getHead(), &node1);
  ASSERT_EQ(list.getTail(), &node3);

  ASSERT_EQ(list.getNext(node1), &newNode);
  ASSERT_EQ(list.getNext(newNode), &node3);
  ASSERT_EQ(list.getNext(node3), nullptr);
  ASSERT_EQ(list.getNext(node2), nullptr);

  ASSERT_EQ(list.getPrev(node1), nullptr);
  ASSERT_EQ(list.getPrev(newNode), &node1);
  ASSERT_EQ(list.getPrev(node3), &newNode);
  ASSERT_EQ(list.getPrev(node2), nullptr);
}

TEST_F(DListTest, InsertBefore) {
  DListImpl list{DListNode::PtrCompressor{}};

  std::vector<std::unique_ptr<DListNode>> nodes;
  for (int i = 0; i < 5; i++) {
    nodes.emplace_back(new DListNode{});
  }

  list.linkAtHead(*nodes[4]);
  list.linkAtHead(*nodes[2]);
  list.linkAtHead(*nodes[0]);

  list.insertBefore(*nodes[2], *nodes[1]);
  list.insertBefore(*nodes[4], *nodes[3]);

  ASSERT_EQ(list.getHead(), nodes.front().get());
  ASSERT_EQ(list.getTail(), nodes.back().get());

  for (size_t i = 0; i < nodes.size(); i++) {
    auto expectedPrev = i == 0 ? nullptr : nodes[i - 1].get();
    auto expectedNext = i == nodes.size() - 1 ? nullptr : nodes[i + 1].get();
    ASSERT_EQ(expectedPrev, list.getPrev(*nodes[i]));
    ASSERT_EQ(expectedNext, list.getNext(*nodes[i]));
  }
}

TEST_F(DListTest, LinkTail) {
  DListImpl list{DListNode::PtrCompressor{}};

  DListNode node1, node2, node3;
  list.linkAtTail(node1);

  ASSERT_EQ(list.getHead(), &node1);
  ASSERT_EQ(list.getTail(), &node1);
  ASSERT_EQ(list.getPrev(node1), nullptr);
  ASSERT_EQ(list.getNext(node1), nullptr);

  list.linkAtTail(node2);

  ASSERT_EQ(list.getHead(), &node1);
  ASSERT_EQ(list.getTail(), &node2);
  ASSERT_EQ(list.getPrev(node2), &node1);
  ASSERT_EQ(list.getNext(node2), nullptr);

  list.linkAtTail(node3);

  ASSERT_EQ(list.getHead(), &node1);
  ASSERT_EQ(list.getTail(), &node3);
  ASSERT_EQ(list.getPrev(node3), &node2);
  ASSERT_EQ(list.getNext(node3), nullptr);
}
