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

#include "cachelib/allocator/datastruct/SList.h"
#include "cachelib/allocator/datastruct/tests/gen-cpp2/test_objects_types.h"
#include "cachelib/common/Serialization.h"

namespace facebook {
namespace cachelib {
namespace tests {

const int kAllocSize = 32;

class SListTest : public ::testing::Test {};

class CACHELIB_PACKED_ATTR SListNode {
 public:
  struct CACHELIB_PACKED_ATTR CompressedPtr {
   public:
    // default construct to nullptr.
    CompressedPtr() = default;

    explicit CompressedPtr(int64_t ptr) : ptr_(ptr) {}

    int64_t saveState() const noexcept { return ptr_; }

    int64_t ptr_{};
  };

  struct PtrCompressor {
    CompressedPtr compress(const SListNode* uncompressed) const noexcept {
      return CompressedPtr{reinterpret_cast<int64_t>(uncompressed)};
    }

    SListNode* unCompress(CompressedPtr compressed) const noexcept {
      return reinterpret_cast<SListNode*>(compressed.ptr_);
    }
  };

  SListNode(const SListNode&) = delete;
  SListNode& operator=(const SListNode&) = delete;

  SListNode(SListNode&& other) noexcept {
    this->hook_ = other.hook_;
    this->content_ = other.content_;
    other.hook_.setNext(nullptr, PtrCompressor());
    other.content_ = nullptr;
  }

  SListNode& operator=(SListNode&& other) noexcept {
    this->hook_ = other.hook_;
    this->content_ = other.content_;
    other.hook_.setNext(nullptr, PtrCompressor());
    other.content_ = nullptr;
    return *this;
  }

  SListNode() { content_ = malloc(kAllocSize); }

  ~SListNode() { free(content_); }

  bool operator==(const SListNode& other) const noexcept {
    return this->content_ == other.content_;
  }

  SListHook<SListNode> hook_{};

 private:
  void* content_;
};
} // namespace tests
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib;
using namespace facebook::cachelib::tests;
using namespace std;

using SListImpl = SList<SListNode, &SListNode::hook_>;

TEST_F(SListTest, TestEmpty) {
  SListImpl list{SListNode::PtrCompressor{}};
  EXPECT_EQ(list.size(), 0);
  EXPECT_TRUE(list.empty());
}

// Try linking and unlinking a single element
TEST_F(SListTest, TestSingleElement) {
  SListImpl list{SListNode::PtrCompressor{}};

  SListNode node;
  list.insert(node);

  EXPECT_EQ(list.getHead(), &node);
  EXPECT_FALSE(list.empty());
  EXPECT_EQ(list.size(), 1);

  list.pop();
  EXPECT_EQ(list.size(), 0);
  EXPECT_TRUE(list.empty());
}

// head -> node1 -> node2 -> nullptr
TEST_F(SListTest, TestTwoElements) {
  SListImpl list{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list.insert(node2);
  list.insert(node1);

  EXPECT_FALSE(list.empty());
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(list.getHead(), &node1);

  list.pop();
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.getHead(), &node2);

  list.pop();
  EXPECT_EQ(list.size(), 0);
  EXPECT_EQ(list.getHead(), nullptr);
  EXPECT_TRUE(list.empty());
}

TEST_F(SListTest, TestSplice) {
  SListImpl source{SListNode::PtrCompressor{}};
  SListImpl dest{SListNode::PtrCompressor{}};
  EXPECT_TRUE(source.empty());
  EXPECT_TRUE(dest.empty());

  // Splicing an empty list into an empty list is fine.
  dest.splice(std::move(source));
  EXPECT_TRUE(source.empty());
  EXPECT_TRUE(dest.empty());

  // Splicing an empty list into a non-empty list is fine.
  SListNode node1, node2;
  dest.insert(node2);
  dest.insert(node1);
  EXPECT_EQ(dest.size(), 2);
  dest.splice(std::move(source));
  EXPECT_TRUE(source.empty());
  EXPECT_EQ(dest.size(), 2);

  EXPECT_EQ(dest.getHead(), &node1);
  dest.pop();
  EXPECT_EQ(dest.size(), 1);
  EXPECT_EQ(dest.getHead(), &node2);

  dest.pop();
  EXPECT_EQ(dest.size(), 0);
  EXPECT_EQ(dest.getHead(), nullptr);
  EXPECT_TRUE(dest.empty());

  // Splicing a non-empty list into an empty list is fine and doesn't change the
  // order of elements.
  source.insert(node2);
  source.insert(node1);
  EXPECT_EQ(source.size(), 2);
  dest.splice(std::move(source));
  EXPECT_TRUE(source.empty());
  EXPECT_EQ(dest.size(), 2);

  EXPECT_EQ(dest.getHead(), &node1);
  dest.pop();
  EXPECT_EQ(dest.size(), 1);
  EXPECT_EQ(dest.getHead(), &node2);

  dest.pop();
  EXPECT_EQ(dest.size(), 0);
  EXPECT_EQ(dest.getHead(), nullptr);
  EXPECT_TRUE(dest.empty());

  // Splicing two non-empty lists is fine and doesn't change the order of
  // elements.
  source.insert(node2);
  source.insert(node1);
  EXPECT_EQ(source.size(), 2);
  SListNode node3, node4;
  dest.insert(node4);
  dest.insert(node3);
  EXPECT_EQ(dest.size(), 2);

  dest.splice(std::move(source));
  EXPECT_TRUE(source.empty());
  EXPECT_EQ(dest.size(), 4);

  EXPECT_EQ(dest.getHead(), &node1);
  dest.pop();
  EXPECT_EQ(dest.size(), 3);
  EXPECT_EQ(dest.getHead(), &node2);
  dest.pop();
  EXPECT_EQ(dest.size(), 2);
  EXPECT_EQ(dest.getHead(), &node3);
  dest.pop();
  EXPECT_EQ(dest.size(), 1);
  EXPECT_EQ(dest.getHead(), &node4);

  dest.pop();
  EXPECT_EQ(dest.size(), 0);
  EXPECT_EQ(dest.getHead(), nullptr);
  EXPECT_TRUE(dest.empty());
}

TEST_F(SListTest, TestIterator) {
  SListImpl list{SListNode::PtrCompressor{}};

  const int sz = 2;
  vector<SListNode> vec;
  vec.reserve(sz);
  for (int i = 0; i < sz; i++) {
    vec.emplace_back();
    list.insert(vec[i]);
  }
  ASSERT_EQ(vec.size(), sz);
  ASSERT_EQ(list.size(), sz);

  int idx = 0;
  for (const auto& element : list) {
    ASSERT_LT(idx, sz);
    EXPECT_EQ(element, vec[sz - idx - 1]);
    idx++;
  }
  ASSERT_EQ(idx, sz);
}

TEST_F(SListTest, TestIteratorRemove) {
  SListImpl list1{SListNode::PtrCompressor{}};

  ASSERT_THROW(list1.remove(list1.end()), std::logic_error);

  SListImpl list2{SListNode::PtrCompressor{}};

  const int sz = 5;
  vector<SListNode> vec;
  vec.reserve(sz);
  int i = 0;
  for (; i < 3; i++) {
    vec.emplace_back();
    list1.insert(vec[i]);
  }

  for (; i < sz; i++) {
    vec.emplace_back();
    list2.insert(vec[i]);
  }

  ASSERT_EQ(vec.size(), sz);
  ASSERT_EQ(list1.size() + list2.size(), sz);

  ASSERT_THROW(list1.remove(list2.begin()), std::logic_error);
  ASSERT_THROW(list2.remove(list1.begin()), std::logic_error);

  auto it = list1.begin();
  while (it != list1.end()) {
    ASSERT_THROW(list2.remove(it), std::logic_error);
    it = list1.remove(it);
  }

  it = list2.begin();
  while (it != list2.end()) {
    ASSERT_THROW(list1.remove(it), std::logic_error);
    it = list2.remove(it);
  }
}

TEST_F(SListTest, TestRemoveSingleElement) {
  SListImpl list{SListNode::PtrCompressor{}};

  SListNode node;
  list.insert(node);

  SListImpl::Iterator it = list.begin();
  EXPECT_EQ(*it, node);
  EXPECT_EQ(1, list.size());

  // Test the new iterator
  SListImpl::Iterator ret = list.remove(it);
  EXPECT_EQ(ret, list.end());

  // Test the old iterator
  EXPECT_NE(it, list.end());
  EXPECT_EQ(*it, node);
  EXPECT_EQ(++it, list.end());
  EXPECT_EQ(0, list.size());
}

// head -> node1 -> node2 -> nullptr
// Remove node1
TEST_F(SListTest, TestRemoveFirstElement) {
  SListImpl list{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list.insert(node2);
  list.insert(node1);

  SListImpl::Iterator it = list.begin();
  EXPECT_EQ(*it, node1);

  SListImpl::Iterator ret = list.remove(it);
  EXPECT_NE(ret, list.end());
  EXPECT_EQ(*ret, node2);

  EXPECT_NE(it, list.end());
  EXPECT_EQ(*it, node1);
  EXPECT_EQ(++it, list.end());
}

// head -> node1 -> node2 -> nullptr
// Remove node2
TEST_F(SListTest, TestRemoveLastElement) {
  SListImpl list{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list.insert(node2);
  list.insert(node1);

  SListImpl::Iterator it = list.begin();
  EXPECT_EQ(*it, node1);
  ++it;
  EXPECT_EQ(*it, node2);

  SListImpl::Iterator ret = list.remove(it);
  EXPECT_EQ(ret, list.end());

  EXPECT_NE(it, list.end());
  EXPECT_EQ(*it, node2);
  EXPECT_EQ(++it, list.end());
}

// head -> node1 -> node2 -> node3 -> nullptr
// Remove node2
TEST_F(SListTest, TestRemoveMidElement) {
  SListImpl list{SListNode::PtrCompressor{}};

  SListNode node1, node2, node3;
  list.insert(node3);
  list.insert(node2);
  list.insert(node1);

  SListImpl::Iterator it = list.begin();
  EXPECT_EQ(*it, node1);
  ++it;
  EXPECT_EQ(*it, node2);

  SListImpl::Iterator ret = list.remove(it);
  EXPECT_NE(ret, list.end());
  EXPECT_EQ(*ret, node3);

  EXPECT_NE(it, list.end());
  EXPECT_EQ(*it, node2);
  EXPECT_EQ(++it, list.end());
}

TEST_F(SListTest, TestRestoreEmptyList) {
  SListImpl list1{SListNode::PtrCompressor{}};

  auto state = list1.saveState();
  SListImpl list2{state, SListNode::PtrCompressor{}};
  EXPECT_EQ(list2.begin(), list2.end());
  EXPECT_EQ(list2.size(), 0);
}

TEST_F(SListTest, TestRestoreList) {
  SListImpl list1{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list1.insert(node2);
  list1.insert(node1);

  auto state = list1.saveState();
  SListImpl list2{state, SListNode::PtrCompressor{}};
  auto it = list2.begin();
  EXPECT_EQ(*it, node1);
  ++it;
  EXPECT_EQ(*it, node2);
  ++it;
  EXPECT_EQ(it, list2.end());
  EXPECT_EQ(list2.size(), 2);

  // Now splice the nodes out just to be sure tail_ is set properly.
  SListImpl list3{SListNode::PtrCompressor{}};
  list3.splice(std::move(list2));
  EXPECT_TRUE(list2.empty());
  EXPECT_EQ(list3.size(), 2);
}

TEST_F(SListTest, TestRestoreListWarmRollFromOldFormat) {
  SListImpl list1{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list1.insert(node2);
  list1.insert(node1);

  // Save the state, then copy it into the old format (dropping compressedTail).
  const auto state = list1.saveState();
  test_serialization::SListObjectNoCompressedTail oldState;
  *oldState.size() = *state.size();
  *oldState.compressedHead() = *state.compressedHead();

  // Now serialize the old object and deserialize it with the latest format.
  auto oldStateBuf = Serializer::serializeToIOBuf(oldState);
  EXPECT_FALSE(oldStateBuf->isChained()) << "should already be coalesced";
  Deserializer deserializer(oldStateBuf->data(),
                            oldStateBuf->data() + oldStateBuf->length());
  const auto newFromOld =
      deserializer.deserialize<serialization::SListObject>();
  EXPECT_EQ(*state.size(), *newFromOld.size());
  EXPECT_EQ(*state.compressedHead(), *newFromOld.compressedHead());
  EXPECT_EQ(-1, *newFromOld.compressedTail());

  SListImpl list2{newFromOld, SListNode::PtrCompressor{}};
  auto it = list2.begin();
  EXPECT_EQ(*it, node1);
  ++it;
  EXPECT_EQ(*it, node2);
  ++it;
  EXPECT_EQ(it, list2.end());
  EXPECT_EQ(list2.size(), 2);

  // Now splice the nodes out just to be sure tail_ is set properly.
  SListImpl list3{SListNode::PtrCompressor{}};
  list3.splice(std::move(list2));
  EXPECT_TRUE(list2.empty());
  EXPECT_EQ(list3.size(), 2);
}

TEST_F(SListTest, TestRestoreListNoTail) {
  SListImpl list1{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list1.insert(node2);
  list1.insert(node1);

  auto state = list1.saveState();
  *state.compressedTail() = -1;
  SListImpl list2{state, SListNode::PtrCompressor{}};
  auto it = list2.begin();
  EXPECT_EQ(*it, node1);
  ++it;
  EXPECT_EQ(*it, node2);
  ++it;
  EXPECT_EQ(it, list2.end());
  EXPECT_EQ(list2.size(), 2);

  // Now splice the nodes out just to be sure tail_ is set properly.
  SListImpl list3{SListNode::PtrCompressor{}};
  list3.splice(std::move(list2));
  EXPECT_TRUE(list2.empty());
  EXPECT_EQ(list3.size(), 2);
}

TEST_F(SListTest, TestInvalidRestore) {
  SListImpl list1{SListNode::PtrCompressor{}};

  SListNode node1, node2;
  list1.insert(node2);
  list1.insert(node1);

  auto state = list1.saveState();
  auto invalidState = state;
  *invalidState.compressedHead() = 0;
  ASSERT_THROW(SListImpl(invalidState, SListNode::PtrCompressor{}),
               std::invalid_argument);
  invalidState = state;
  *invalidState.compressedTail() = 0;
  ASSERT_THROW(SListImpl(invalidState, SListNode::PtrCompressor{}),
               std::invalid_argument);

  SListImpl list2{state, SListNode::PtrCompressor{}};
  auto it = list2.begin();
  EXPECT_EQ(*it, node1);
  ++it;
  EXPECT_EQ(*it, node2);
  ++it;
  EXPECT_EQ(it, list2.end());
  EXPECT_EQ(list2.size(), 2);

  list2.pop();
  list2.pop();
  EXPECT_EQ(list2.size(), 0);
  ASSERT_EQ(list2.getHead(), nullptr);

  state = list2.saveState();
  invalidState = state;
  *invalidState.size() = 5;
  ASSERT_THROW(SListImpl(invalidState, SListNode::PtrCompressor{}),
               std::invalid_argument);

  SListImpl list3{state, SListNode::PtrCompressor{}};
  ASSERT_EQ(list3.size(), 0);
  ASSERT_EQ(list3.getHead(), nullptr);
}
