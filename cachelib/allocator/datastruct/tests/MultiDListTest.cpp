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

#include "cachelib/allocator/datastruct/MultiDList.h"

namespace facebook {
namespace cachelib {
namespace tests {

class MultiDListTest : public ::testing::Test {};

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

using MultiDListImpl = MultiDList<DListNode, &DListNode::hook_>;

void testIterate(MultiDListImpl& list, std::vector<DListNode>& nodes) {
  int idx = 0;
  int backIdx = 0;
  auto it = list.rbegin();
  while (it != list.rend()) {
    ASSERT_EQ(&nodes[idx], it.get()) << "Index: " << idx;
    ++idx;
    ++it;
    ++backIdx;
    // Should we support backing out of the end?
    if (backIdx == 3 && it != list.rend()) {
      backIdx = 0;
      for (int i = 0; i < 2; i++) {
        --it;
        --idx;
        ASSERT_EQ(&nodes[idx], it.get()) << "Index: " << idx;
      }
    }
  }
}

TEST_F(MultiDListTest, IterateTest) {
  DListNode::PtrCompressor compressor;
  MultiDListImpl list{3, compressor};
  std::vector<DListNode> nodes(20);

  // Iterate empty multi list
  testIterate(list, nodes);

  int idx = 0;
  for (int i = 0; i < 4; i++) {
    list.getList(2).linkAtHead(nodes[idx++]);
    testIterate(list, nodes);
  }

  for (int i = 0; i < 6; i++) {
    list.getList(1).linkAtHead(nodes[idx++]);
    testIterate(list, nodes);
  }

  for (int i = 0; i < 10; i++) {
    list.getList(0).linkAtHead(nodes[idx++]);
    testIterate(list, nodes);
  }
}
