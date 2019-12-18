#include "cachelib/navy/block_cache/BTree.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(BTreeNode, InsertAt) {
  int buf[] = {1, 2, 3, 4, 5, -1};
  details::insertFirst(buf + 2, buf + 5, 100);
  EXPECT_EQ(1, buf[0]);
  EXPECT_EQ(2, buf[1]);
  EXPECT_EQ(100, buf[2]);
  EXPECT_EQ(3, buf[3]);
  EXPECT_EQ(4, buf[4]);
  EXPECT_EQ(5, buf[5]);
}

TEST(BTreeNode, RemoveAt) {
  int buf[] = {1, 2, 3, 4, 5};
  details::removeFirst(buf + 2, buf + 5);
  EXPECT_EQ(1, buf[0]);
  EXPECT_EQ(2, buf[1]);
  EXPECT_EQ(4, buf[2]);
  EXPECT_EQ(5, buf[3]);
}

TEST(BTreeNode, Leaf) {
  using Leaf = BTreeNode<BTreeNodeType::Leaf, uint64_t, uint32_t>;

  EXPECT_EQ(92, Leaf::allocSize(6));
  EXPECT_EQ(96, Leaf::allocSlotSize(6));
  auto leaf = Leaf::create(4);
  EXPECT_EQ(0, leaf->size());
  leaf->insert(leaf->find(2), 2, 10);
  leaf->insert(leaf->find(8), 8, 40);
  leaf->insert(leaf->find(6), 6, 30);
  leaf->insert(leaf->find(4), 4, 20);
  EXPECT_EQ(4, leaf->size());
  EXPECT_EQ(2, leaf->getKey(0));
  EXPECT_EQ(4, leaf->getKey(1));
  EXPECT_EQ(6, leaf->getKey(2));
  EXPECT_EQ(8, leaf->getKey(3));
  leaf->remove(leaf->find(2));
  leaf->remove(leaf->find(6));
  EXPECT_EQ(2, leaf->size());
  EXPECT_EQ(4, leaf->getKey(0));
  EXPECT_EQ(8, leaf->getKey(1));

  // Insert those back:
  leaf->insert(leaf->find(2), 2, 10);
  leaf->insert(leaf->find(6), 6, 30);
  EXPECT_EQ(4, leaf->size());

  auto leaf1 = Leaf::create(4);
  auto leaf2 = Leaf::create(4);
  leaf->splitTo(*leaf1, *leaf2, 2);
  EXPECT_EQ(2, leaf1->size());
  EXPECT_EQ(2, leaf1->getKey(0));
  EXPECT_EQ(4, leaf1->getKey(1));

  EXPECT_EQ(2, leaf2->size());
  EXPECT_EQ(6, leaf2->getKey(0));
  EXPECT_EQ(8, leaf2->getKey(1));
  {
    auto temp = Leaf::create(4);
    temp->copyFrom(*leaf2);
    EXPECT_EQ(2, temp->size());
    EXPECT_EQ(6, temp->getKey(0));
    EXPECT_EQ(8, temp->getKey(1));
    temp->destroy();
  }
  {
    auto temp = Leaf::create(4);
    temp->mergeFrom(*leaf1, *leaf2);
    EXPECT_EQ(4, temp->size());
    EXPECT_EQ(2, temp->getKey(0));
    EXPECT_EQ(4, temp->getKey(1));
    EXPECT_EQ(6, temp->getKey(2));
    EXPECT_EQ(8, temp->getKey(3));
    temp->destroy();
  }
  leaf->destroy();
  leaf1->destroy();
  leaf2->destroy();
}

TEST(BTreeNode, Node) {
  using Node = BTreeNode<BTreeNodeType::Node, uint64_t, uint32_t>;

  EXPECT_EQ(92, Node::allocSize(6));
  EXPECT_EQ(96, Node::allocSlotSize(6));
  auto node = Node::create(4);
  EXPECT_EQ(0, node->getValue(0));
  EXPECT_EQ(0, node->size());
  node->getMutableValue(0) = 50; // Check it is preserved
  node->insert(node->find(2), 2, 20);
  node->insert(node->find(4), 4, 40);
  node->insert(node->find(1), 1, 10);
  node->insert(node->find(3), 3, 30);
  EXPECT_EQ(4, node->size());
  EXPECT_EQ(1, node->getKey(0));
  EXPECT_EQ(2, node->getKey(1));
  EXPECT_EQ(3, node->getKey(2));
  EXPECT_EQ(4, node->getKey(3));
  EXPECT_EQ(10, node->getValue(0));
  EXPECT_EQ(20, node->getValue(1));
  EXPECT_EQ(30, node->getValue(2));
  EXPECT_EQ(40, node->getValue(3));
  EXPECT_EQ(50, node->getValue(4));

  auto node1 = Node::create(4);
  auto node2 = Node::create(4);
  node->splitTo(*node1, *node2, 2);

  EXPECT_EQ(2, node1->size());
  EXPECT_EQ(1, node1->getKey(0));
  EXPECT_EQ(2, node1->getKey(1));
  EXPECT_EQ(10, node1->getValue(0));
  EXPECT_EQ(20, node1->getValue(1));
  EXPECT_EQ(0, node1->getValue(2));

  EXPECT_EQ(2, node2->size());
  EXPECT_EQ(3, node2->getKey(0));
  EXPECT_EQ(4, node2->getKey(1));
  EXPECT_EQ(30, node2->getValue(0));
  EXPECT_EQ(40, node2->getValue(1));
  EXPECT_EQ(50, node2->getValue(2));
  {
    auto temp = Node::create(4);
    temp->copyFrom(*node2);
    EXPECT_EQ(2, temp->size());
    EXPECT_EQ(3, temp->getKey(0));
    EXPECT_EQ(4, temp->getKey(1));
    EXPECT_EQ(30, temp->getValue(0));
    EXPECT_EQ(40, temp->getValue(1));
    EXPECT_EQ(50, temp->getValue(2));
    temp->destroy();
  }
  {
    auto temp = Node::create(4);
    temp->mergeFrom(*node1, *node2);
    EXPECT_EQ(4, temp->size());
    EXPECT_EQ(4, temp->size());
    EXPECT_EQ(1, temp->getKey(0));
    EXPECT_EQ(2, temp->getKey(1));
    EXPECT_EQ(3, temp->getKey(2));
    EXPECT_EQ(4, temp->getKey(3));
    EXPECT_EQ(10, temp->getValue(0));
    EXPECT_EQ(20, temp->getValue(1));
    EXPECT_EQ(30, temp->getValue(2));
    EXPECT_EQ(40, temp->getValue(3));
    EXPECT_EQ(50, temp->getValue(4));
    temp->remove(1);
    temp->remove(2);
    EXPECT_EQ(2, temp->size());
    EXPECT_EQ(1, temp->getKey(0));
    EXPECT_EQ(3, temp->getKey(1));
    EXPECT_EQ(10, temp->getValue(0));
    EXPECT_EQ(30, temp->getValue(1));
    EXPECT_EQ(50, temp->getValue(2));
    temp->destroy();
  }
  node->destroy();
  node1->destroy();
  node2->destroy();
}

TEST(BTree, ZeroSize) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 4>>;

  TestBTree bt;
  EXPECT_EQ(0, bt.size());
  EXPECT_EQ(nullptr, bt.root());
  bt.insert(10, 100);
  EXPECT_EQ(1, bt.size());
  EXPECT_TRUE(bt.remove(10));
  EXPECT_EQ(0, bt.size());
  EXPECT_EQ(nullptr, bt.root());
}

TEST(BTree, Insert) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 4>>;
  using Node = TestBTree::Node;
  using Leaf = TestBTree::Leaf;

  TestBTree bt;
  EXPECT_FALSE(bt.insert(10, 100));
  EXPECT_FALSE(bt.insert(20, 200));
  EXPECT_FALSE(bt.insert(30, 300));
  EXPECT_FALSE(bt.insert(40, 400));
  EXPECT_FALSE(bt.insert(50, 500));
  EXPECT_FALSE(bt.insert(60, 600));
  EXPECT_TRUE(bt.insert(10, 1000));
  EXPECT_TRUE(bt.insert(30, 3000));
  EXPECT_TRUE(bt.insert(60, 6000));
  EXPECT_EQ(6, bt.size());
  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 1000);
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);
  EXPECT_TRUE(bt.lookup(30, value) && value == 3000);
  EXPECT_TRUE(bt.lookup(40, value) && value == 400);
  EXPECT_TRUE(bt.lookup(50, value) && value == 500);
  EXPECT_TRUE(bt.lookup(60, value) && value == 6000);
  EXPECT_FALSE(bt.lookup(25, value));
  EXPECT_FALSE(bt.lookup(35, value));
  EXPECT_FALSE(bt.lookup(55, value));
  EXPECT_FALSE(bt.lookup(15, value));

  // Let's look into the internals
  auto root = reinterpret_cast<Node*>(bt.root());
  EXPECT_EQ(1, root->size());
  EXPECT_EQ(20, root->getKey(0));
  {
    auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
    EXPECT_EQ(2, leaf->size());
    EXPECT_EQ(10, leaf->getKey(0));
    EXPECT_EQ(20, leaf->getKey(1));
  }
  {
    auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
    EXPECT_EQ(4, leaf->size());
    EXPECT_EQ(30, leaf->getKey(0));
    EXPECT_EQ(40, leaf->getKey(1));
    EXPECT_EQ(50, leaf->getKey(2));
    EXPECT_EQ(60, leaf->getKey(3));
  }
  {
    auto stats = bt.getMemoryStats();
    EXPECT_EQ(2 * TestBTree::Leaf::allocSlotSize(4), stats.leafMemory);
    EXPECT_EQ(TestBTree::Node::allocSlotSize(4), stats.nodeMemory);
  }

  bt.insert(45, 450);
  EXPECT_EQ(7, bt.size());
  EXPECT_TRUE(bt.lookup(40, value) && value == 400);
  EXPECT_TRUE(bt.lookup(45, value) && value == 450);
  EXPECT_TRUE(bt.lookup(50, value) && value == 500);
  EXPECT_EQ(root, bt.root());
  EXPECT_EQ(2, root->size());
  EXPECT_EQ(20, root->getKey(0));
  EXPECT_EQ(40, root->getKey(1));
  {
    auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
    EXPECT_EQ(2, leaf->size());
    EXPECT_EQ(30, leaf->getKey(0));
    EXPECT_EQ(40, leaf->getKey(1));
  }
  {
    auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
    EXPECT_EQ(3, leaf->size());
    EXPECT_EQ(45, leaf->getKey(0));
    EXPECT_EQ(50, leaf->getKey(1));
    EXPECT_EQ(60, leaf->getKey(2));
  }

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, InsertResize) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;
  using Node = TestBTree::Node;
  using Leaf = TestBTree::Leaf;

  TestBTree bt;
  bt.insert(10, 100);
  bt.insert(20, 200);
  bt.insert(30, 300);
  bt.insert(40, 400);
  bt.insert(50, 500);
  bt.insert(60, 600);
  EXPECT_EQ(6, bt.size());
  {
    auto root = reinterpret_cast<Leaf*>(bt.root());
    EXPECT_EQ(6, root->size());
    EXPECT_EQ(10, root->getKey(0));
    EXPECT_EQ(20, root->getKey(1));
    EXPECT_EQ(30, root->getKey(2));
    EXPECT_EQ(40, root->getKey(3));
    EXPECT_EQ(50, root->getKey(4));
    EXPECT_EQ(60, root->getKey(5));
  }
  bt.insert(70, 700);
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(4, root->capacity());
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(4, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(20, leaf->getKey(1));
      EXPECT_EQ(30, leaf->getKey(2));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(4, leaf->capacity());
      EXPECT_EQ(40, leaf->getKey(0));
      EXPECT_EQ(50, leaf->getKey(1));
      EXPECT_EQ(60, leaf->getKey(2));
      EXPECT_EQ(70, leaf->getKey(3));
    }
  }
  bt.insert(11, 110);
  bt.insert(12, 120);
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(4, root->capacity());
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(11, leaf->getKey(1));
      EXPECT_EQ(12, leaf->getKey(2));
      EXPECT_EQ(20, leaf->getKey(3));
      EXPECT_EQ(30, leaf->getKey(4));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(40, leaf->getKey(0));
      EXPECT_EQ(50, leaf->getKey(1));
      EXPECT_EQ(60, leaf->getKey(2));
      EXPECT_EQ(70, leaf->getKey(3));
    }
  }
  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, SingleValue) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;

  TestBTree bt;
  uint32_t value = 0;
  bt.insert(10, 100);
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_TRUE(bt.remove(10));
  EXPECT_FALSE(bt.lookup(10, value));
  bt.insert(10, 100);
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
}

TEST(BTree, RemoveResize) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;
  using Node = TestBTree::Node;

  TestBTree bt;
  bt.insert(10, 100);
  bt.insert(11, 110);
  bt.insert(12, 120);
  bt.insert(20, 200);
  bt.insert(21, 210);
  bt.insert(22, 230);
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(6, root->size());
  }
  EXPECT_TRUE(bt.remove(12));
  EXPECT_TRUE(bt.remove(20));
  EXPECT_TRUE(bt.remove(21));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(3, root->size());
    EXPECT_EQ(6, root->capacity());
  }
  EXPECT_TRUE(bt.remove(22));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(4, root->capacity());
  }
  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveDecreaseHeight) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;
  using Node = TestBTree::Node;
  using Leaf = TestBTree::Leaf;

  TestBTree bt;
  bt.insert(10, 100);
  bt.insert(11, 110);
  bt.insert(12, 120);
  bt.insert(20, 200);
  bt.insert(21, 210);
  bt.insert(22, 230);
  bt.insert(23, 230);
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(12, root->getKey(0));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(12, leaf->getKey(2));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(20, leaf->getKey(0));
      EXPECT_EQ(23, leaf->getKey(3));
    }
  }
  {
    auto stats = bt.getMemoryStats();
    EXPECT_EQ(2 * TestBTree::Leaf::allocSlotSize(4), stats.leafMemory);
    EXPECT_EQ(TestBTree::Node::allocSlotSize(4), stats.nodeMemory);
  }

  EXPECT_EQ(7, bt.size());
  EXPECT_TRUE(bt.remove(23));
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(12));
  EXPECT_TRUE(bt.remove(11));
  EXPECT_EQ(3, bt.size());
  {
    auto root = reinterpret_cast<Leaf*>(bt.root());
    EXPECT_EQ(3, root->size());
    EXPECT_EQ(10, root->getKey(0));
    EXPECT_EQ(20, root->getKey(1));
    EXPECT_EQ(21, root->getKey(2));
  }
  {
    auto stats = bt.getMemoryStats();
    EXPECT_EQ(TestBTree::Leaf::allocSlotSize(6), stats.leafMemory);
    EXPECT_EQ(0, stats.nodeMemory);
  }
  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

namespace {
template <typename K, typename V>
BTree<K, V, BTreeTraits<4, 6, 8>> makeTreeForLeafRemoveTest() {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  TestBTree bt;
  // 1st node after split
  bt.insert(10, 100);
  bt.insert(11, 110);
  bt.insert(12, 120);
  bt.insert(16, 160);
  // 2nd node after split
  bt.insert(20, 200);
  bt.insert(21, 210);
  bt.insert(22, 220);
  bt.insert(26, 260);
  // Add more into 1st
  bt.insert(13, 130);
  bt.insert(14, 140);
  bt.insert(15, 150);
  // Fill up 2nd with 3x and split
  bt.insert(30, 300);
  bt.insert(31, 310);
  bt.insert(32, 320);
  bt.insert(36, 360);
  // Add more into 2nd
  bt.insert(23, 230);
  bt.insert(24, 240);
  bt.insert(25, 250);
  // Add more into 3rd
  bt.insert(33, 330);
  bt.insert(34, 340);
  bt.insert(35, 350);

  // Check we got expected configuration
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(7, leaf->size());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(16, leaf->getKey(6));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(7, leaf->size());
      EXPECT_EQ(20, leaf->getKey(0));
      EXPECT_EQ(26, leaf->getKey(6));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(7, leaf->size());
      EXPECT_EQ(30, leaf->getKey(0));
      EXPECT_EQ(36, leaf->getKey(6));
    }
  }
  return bt;
}
} // namespace

TEST(BTree, RemoveLeafMergeWithNextLarge) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  EXPECT_TRUE(bt.remove(16));
  EXPECT_TRUE(bt.remove(15));
  EXPECT_TRUE(bt.remove(14));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(13, leaf->getKey(3));
    }
  }
  // This will resize the node down
  EXPECT_TRUE(bt.remove(13));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(11, leaf->getKey(1));
      EXPECT_EQ(12, leaf->getKey(2));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(7, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
    }
  }
  // This will compact node with only the next node
  EXPECT_TRUE(bt.remove(12));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(21, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(11, leaf->getKey(1));
      EXPECT_EQ(20, leaf->getKey(2));
      EXPECT_EQ(21, leaf->getKey(3));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(22, leaf->getKey(0));
      EXPECT_EQ(23, leaf->getKey(1));
      EXPECT_EQ(24, leaf->getKey(2));
      EXPECT_EQ(25, leaf->getKey(3));
      EXPECT_EQ(26, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_TRUE(bt.lookup(11, value) && value == 110);
  EXPECT_FALSE(bt.lookup(12, value));
  EXPECT_FALSE(bt.lookup(13, value));
  EXPECT_FALSE(bt.lookup(14, value));
  EXPECT_FALSE(bt.lookup(15, value));
  EXPECT_FALSE(bt.lookup(16, value));
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);
  EXPECT_TRUE(bt.lookup(21, value) && value == 210);
  EXPECT_TRUE(bt.lookup(22, value) && value == 220);
  EXPECT_TRUE(bt.lookup(23, value) && value == 230);
  EXPECT_TRUE(bt.lookup(24, value) && value == 240);
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_TRUE(bt.lookup(26, value) && value == 260);
  EXPECT_EQ(16, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveLeafMergeWithNextSmall) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  // Make next leaf (2x) small
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(23));
  EXPECT_TRUE(bt.remove(24));
  EXPECT_TRUE(bt.remove(26));
  EXPECT_TRUE(bt.remove(12));
  EXPECT_TRUE(bt.remove(13));
  EXPECT_TRUE(bt.remove(14));
  EXPECT_TRUE(bt.remove(16));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
    }
  }
  EXPECT_TRUE(bt.remove(11));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(26, root->getKey(0));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(15, leaf->getKey(1));
      EXPECT_EQ(20, leaf->getKey(2));
      EXPECT_EQ(21, leaf->getKey(3));
      EXPECT_EQ(25, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_FALSE(bt.lookup(11, value));
  EXPECT_FALSE(bt.lookup(12, value));
  EXPECT_FALSE(bt.lookup(13, value));
  EXPECT_FALSE(bt.lookup(14, value));
  EXPECT_TRUE(bt.lookup(15, value) && value == 150);
  EXPECT_FALSE(bt.lookup(16, value));
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);
  EXPECT_TRUE(bt.lookup(21, value) && value == 210);
  EXPECT_FALSE(bt.lookup(22, value));
  EXPECT_FALSE(bt.lookup(23, value));
  EXPECT_FALSE(bt.lookup(24, value));
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_FALSE(bt.lookup(26, value));
  EXPECT_EQ(12, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveLeafMergeWithPrevLarge) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  EXPECT_TRUE(bt.remove(36));
  EXPECT_TRUE(bt.remove(35));
  EXPECT_TRUE(bt.remove(34));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(30, leaf->getKey(0));
      EXPECT_EQ(33, leaf->getKey(3));
    }
  }
  // This will resize the node down
  EXPECT_TRUE(bt.remove(33));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(7, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(30, leaf->getKey(0));
      EXPECT_EQ(31, leaf->getKey(1));
      EXPECT_EQ(32, leaf->getKey(2));
    }
  }
  // This will compact node with only the prev node
  EXPECT_TRUE(bt.remove(32));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(23, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(20, leaf->getKey(0));
      EXPECT_EQ(21, leaf->getKey(1));
      EXPECT_EQ(22, leaf->getKey(2));
      EXPECT_EQ(23, leaf->getKey(3));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(24, leaf->getKey(0));
      EXPECT_EQ(25, leaf->getKey(1));
      EXPECT_EQ(26, leaf->getKey(2));
      EXPECT_EQ(30, leaf->getKey(3));
      EXPECT_EQ(31, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);
  EXPECT_TRUE(bt.lookup(21, value) && value == 210);
  EXPECT_TRUE(bt.lookup(22, value) && value == 220);
  EXPECT_TRUE(bt.lookup(23, value) && value == 230);
  EXPECT_TRUE(bt.lookup(24, value) && value == 240);
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_TRUE(bt.lookup(26, value) && value == 260);
  EXPECT_TRUE(bt.lookup(30, value) && value == 300);
  EXPECT_TRUE(bt.lookup(31, value) && value == 310);
  EXPECT_FALSE(bt.lookup(32, value));
  EXPECT_FALSE(bt.lookup(33, value));
  EXPECT_FALSE(bt.lookup(34, value));
  EXPECT_FALSE(bt.lookup(35, value));
  EXPECT_FALSE(bt.lookup(36, value));
  EXPECT_EQ(16, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveLeafMergeWithPrevSmall) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  // Make next leaf (2x) small
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(23));
  EXPECT_TRUE(bt.remove(24));
  EXPECT_TRUE(bt.remove(26));
  EXPECT_TRUE(bt.remove(32));
  EXPECT_TRUE(bt.remove(33));
  EXPECT_TRUE(bt.remove(34));
  EXPECT_TRUE(bt.remove(36));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
    }
  }
  EXPECT_TRUE(bt.remove(31));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(16, root->getKey(0));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(20, leaf->getKey(0));
      EXPECT_EQ(21, leaf->getKey(1));
      EXPECT_EQ(25, leaf->getKey(2));
      EXPECT_EQ(30, leaf->getKey(3));
      EXPECT_EQ(35, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);
  EXPECT_TRUE(bt.lookup(21, value) && value == 210);
  EXPECT_FALSE(bt.lookup(22, value));
  EXPECT_FALSE(bt.lookup(23, value));
  EXPECT_FALSE(bt.lookup(24, value));
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_FALSE(bt.lookup(26, value));
  EXPECT_TRUE(bt.lookup(30, value) && value == 300);
  EXPECT_FALSE(bt.lookup(31, value));
  EXPECT_FALSE(bt.lookup(32, value));
  EXPECT_FALSE(bt.lookup(33, value));
  EXPECT_FALSE(bt.lookup(34, value));
  EXPECT_TRUE(bt.lookup(35, value) && value == 350);
  EXPECT_FALSE(bt.lookup(36, value));
  EXPECT_EQ(12, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveChooseSmallLeft) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  // Make the following setup:
  //  L[3] L[7] L[6]
  EXPECT_TRUE(bt.remove(16));
  EXPECT_TRUE(bt.remove(15));
  EXPECT_TRUE(bt.remove(14));
  EXPECT_TRUE(bt.remove(13));
  EXPECT_TRUE(bt.remove(36));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(6, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
    }
  }

  // Remove from the middle node
  EXPECT_TRUE(bt.remove(26));
  EXPECT_TRUE(bt.remove(24));
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(21));
  EXPECT_TRUE(bt.remove(20));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(26, root->getKey(0));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(11, leaf->getKey(1));
      EXPECT_EQ(12, leaf->getKey(2));
      EXPECT_EQ(23, leaf->getKey(3));
      EXPECT_EQ(25, leaf->getKey(4));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(6, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(30, leaf->getKey(0));
      EXPECT_EQ(31, leaf->getKey(1));
      EXPECT_EQ(32, leaf->getKey(2));
      EXPECT_EQ(33, leaf->getKey(3));
      EXPECT_EQ(34, leaf->getKey(4));
      EXPECT_EQ(35, leaf->getKey(5));
    }
  }

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_TRUE(bt.lookup(11, value) && value == 110);
  EXPECT_TRUE(bt.lookup(12, value) && value == 120);
  EXPECT_FALSE(bt.lookup(13, value));
  EXPECT_FALSE(bt.lookup(14, value));
  EXPECT_FALSE(bt.lookup(15, value));
  EXPECT_FALSE(bt.lookup(16, value));
  EXPECT_FALSE(bt.lookup(20, value));
  EXPECT_FALSE(bt.lookup(21, value));
  EXPECT_FALSE(bt.lookup(22, value));
  EXPECT_TRUE(bt.lookup(23, value) && value == 230);
  EXPECT_FALSE(bt.lookup(24, value));
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_FALSE(bt.lookup(26, value));
  EXPECT_EQ(11, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveChooseSmallRight) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  // Make the following setup:
  //  L[6] L[7] L[3]
  EXPECT_TRUE(bt.remove(16));
  EXPECT_TRUE(bt.remove(36));
  EXPECT_TRUE(bt.remove(35));
  EXPECT_TRUE(bt.remove(34));
  EXPECT_TRUE(bt.remove(33));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(6, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(3, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
    }
  }

  // Remove from the middle node
  EXPECT_TRUE(bt.remove(26));
  EXPECT_TRUE(bt.remove(24));
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(21));
  EXPECT_TRUE(bt.remove(20));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(1, root->size());
    EXPECT_EQ(16, root->getKey(0));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(6, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(11, leaf->getKey(1));
      EXPECT_EQ(12, leaf->getKey(2));
      EXPECT_EQ(13, leaf->getKey(3));
      EXPECT_EQ(14, leaf->getKey(4));
      EXPECT_EQ(15, leaf->getKey(5));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(8, leaf->capacity());
      EXPECT_EQ(23, leaf->getKey(0));
      EXPECT_EQ(25, leaf->getKey(1));
      EXPECT_EQ(30, leaf->getKey(2));
      EXPECT_EQ(31, leaf->getKey(3));
      EXPECT_EQ(32, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_FALSE(bt.lookup(20, value));
  EXPECT_FALSE(bt.lookup(21, value));
  EXPECT_FALSE(bt.lookup(22, value));
  EXPECT_TRUE(bt.lookup(23, value) && value == 230);
  EXPECT_FALSE(bt.lookup(24, value));
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_FALSE(bt.lookup(26, value));
  EXPECT_TRUE(bt.lookup(30, value) && value == 300);
  EXPECT_TRUE(bt.lookup(31, value) && value == 310);
  EXPECT_TRUE(bt.lookup(32, value) && value == 320);
  EXPECT_FALSE(bt.lookup(33, value));
  EXPECT_FALSE(bt.lookup(34, value));
  EXPECT_FALSE(bt.lookup(35, value));
  EXPECT_FALSE(bt.lookup(36, value));
  EXPECT_EQ(11, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveChooseLargeLeft) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  // Make the following setup:
  //  L[7] L[7] L[6]
  EXPECT_TRUE(bt.remove(36));
  // Remove from the middle node
  EXPECT_TRUE(bt.remove(26));
  EXPECT_TRUE(bt.remove(24));
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(21));
  EXPECT_TRUE(bt.remove(20));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(13, root->getKey(0));
    EXPECT_EQ(26, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(0));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(10, leaf->getKey(0));
      EXPECT_EQ(11, leaf->getKey(1));
      EXPECT_EQ(12, leaf->getKey(2));
      EXPECT_EQ(13, leaf->getKey(3));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(14, leaf->getKey(0));
      EXPECT_EQ(15, leaf->getKey(1));
      EXPECT_EQ(16, leaf->getKey(2));
      EXPECT_EQ(23, leaf->getKey(3));
      EXPECT_EQ(25, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_TRUE(bt.lookup(11, value) && value == 110);
  EXPECT_TRUE(bt.lookup(12, value) && value == 120);
  EXPECT_TRUE(bt.lookup(13, value) && value == 130);
  EXPECT_TRUE(bt.lookup(14, value) && value == 140);
  EXPECT_TRUE(bt.lookup(15, value) && value == 150);
  EXPECT_TRUE(bt.lookup(16, value) && value == 160);
  EXPECT_FALSE(bt.lookup(20, value));
  EXPECT_FALSE(bt.lookup(21, value));
  EXPECT_FALSE(bt.lookup(22, value));
  EXPECT_TRUE(bt.lookup(23, value) && value == 230);
  EXPECT_FALSE(bt.lookup(24, value));
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_FALSE(bt.lookup(26, value));
  EXPECT_EQ(15, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, RemoveChooseLargeRight) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 6, 8>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto bt = makeTreeForLeafRemoveTest<TestBTree::Key, TestBTree::Value>();
  // Make the following setup:
  //  L[6] L[7] L[7]
  EXPECT_TRUE(bt.remove(16));
  // Remove from the middle node
  EXPECT_TRUE(bt.remove(26));
  EXPECT_TRUE(bt.remove(24));
  EXPECT_TRUE(bt.remove(22));
  EXPECT_TRUE(bt.remove(21));
  EXPECT_TRUE(bt.remove(20));
  {
    auto root = reinterpret_cast<Node*>(bt.root());
    EXPECT_EQ(2, root->size());
    EXPECT_EQ(16, root->getKey(0));
    EXPECT_EQ(31, root->getKey(1));
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(1));
      EXPECT_EQ(4, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(23, leaf->getKey(0));
      EXPECT_EQ(25, leaf->getKey(1));
      EXPECT_EQ(30, leaf->getKey(2));
      EXPECT_EQ(31, leaf->getKey(3));
    }
    {
      auto leaf = reinterpret_cast<Leaf*>(root->getValue(2));
      EXPECT_EQ(5, leaf->size());
      EXPECT_EQ(6, leaf->capacity());
      EXPECT_EQ(32, leaf->getKey(0));
      EXPECT_EQ(33, leaf->getKey(1));
      EXPECT_EQ(34, leaf->getKey(2));
      EXPECT_EQ(35, leaf->getKey(3));
      EXPECT_EQ(36, leaf->getKey(4));
    }
  }

  uint32_t value{};
  EXPECT_FALSE(bt.lookup(20, value));
  EXPECT_FALSE(bt.lookup(21, value));
  EXPECT_FALSE(bt.lookup(22, value));
  EXPECT_TRUE(bt.lookup(23, value) && value == 230);
  EXPECT_FALSE(bt.lookup(24, value));
  EXPECT_TRUE(bt.lookup(25, value) && value == 250);
  EXPECT_FALSE(bt.lookup(26, value));
  EXPECT_TRUE(bt.lookup(30, value) && value == 300);
  EXPECT_TRUE(bt.lookup(31, value) && value == 310);
  EXPECT_TRUE(bt.lookup(32, value) && value == 320);
  EXPECT_TRUE(bt.lookup(33, value) && value == 330);
  EXPECT_TRUE(bt.lookup(34, value) && value == 340);
  EXPECT_TRUE(bt.lookup(35, value) && value == 350);
  EXPECT_TRUE(bt.lookup(36, value) && value == 360);
  EXPECT_EQ(15, bt.size());

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, InsertTinyNode) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<2, 2, 2>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  TestBTree bt;
  bt.insert(40, 400);
  bt.insert(70, 700);
  bt.insert(20, 200);
  bt.insert(50, 500);
  bt.insert(30, 300);
  bt.insert(80, 800);
  bt.insert(10, 100);
  bt.insert(60, 600);
  EXPECT_EQ(8, bt.size());
  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);
  EXPECT_TRUE(bt.lookup(30, value) && value == 300);
  EXPECT_TRUE(bt.lookup(40, value) && value == 400);
  EXPECT_TRUE(bt.lookup(50, value) && value == 500);
  EXPECT_TRUE(bt.lookup(60, value) && value == 600);
  EXPECT_TRUE(bt.lookup(70, value) && value == 700);
  EXPECT_TRUE(bt.lookup(80, value) && value == 800);

  auto root = reinterpret_cast<Node*>(bt.root());
  EXPECT_EQ(2, root->size());
  EXPECT_EQ(20, root->getKey(0));
  EXPECT_EQ(40, root->getKey(1));
  {
    auto node = reinterpret_cast<Node*>(root->getValue(0));
    EXPECT_EQ(1, node->size());
    EXPECT_EQ(20, node->getKey(0));
    auto l0 = reinterpret_cast<Leaf*>(node->getValue(0));
    EXPECT_EQ(2, l0->size());
    EXPECT_EQ(10, l0->getKey(0));
    EXPECT_EQ(20, l0->getKey(1));
    auto l1 = reinterpret_cast<Leaf*>(node->getValue(1));
    EXPECT_EQ(nullptr, l1);
  }
  {
    auto node = reinterpret_cast<Node*>(root->getValue(1));
    EXPECT_EQ(1, node->size());
    EXPECT_EQ(40, node->getKey(0));
    auto l0 = reinterpret_cast<Leaf*>(node->getValue(0));
    EXPECT_EQ(2, l0->size());
    EXPECT_EQ(30, l0->getKey(0));
    EXPECT_EQ(40, l0->getKey(1));
    auto l1 = reinterpret_cast<Leaf*>(node->getValue(1));
    EXPECT_EQ(nullptr, l1);
  }
  {
    auto node = reinterpret_cast<Node*>(root->getValue(2));
    EXPECT_EQ(2, node->size());
    EXPECT_EQ(50, node->getKey(0));
    EXPECT_EQ(70, node->getKey(1));
    auto l0 = reinterpret_cast<Leaf*>(node->getValue(0));
    EXPECT_EQ(1, l0->size());
    EXPECT_EQ(50, l0->getKey(0));
    auto l1 = reinterpret_cast<Leaf*>(node->getValue(1));
    EXPECT_EQ(2, l1->size());
    EXPECT_EQ(60, l1->getKey(0));
    EXPECT_EQ(70, l1->getKey(1));
    auto l2 = reinterpret_cast<Leaf*>(node->getValue(2));
    EXPECT_EQ(1, l2->size());
    EXPECT_EQ(80, l2->getKey(0));
  }

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, Overwrite) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;

  TestBTree bt;
  bt.insert(10, 100);
  bt.insert(20, 200);

  EXPECT_EQ(2, bt.size());
  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 100);
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);

  bt.insert(10, 101);
  EXPECT_EQ(2, bt.size());
  EXPECT_TRUE(bt.lookup(10, value) && value == 101);
  EXPECT_TRUE(bt.lookup(20, value) && value == 200);

  bt.destroy();
  EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
}

TEST(BTree, validate) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto node = Node::create(4);
  {
    auto leaf = Leaf::create(4);
    leaf->insert(0, 10, 11);
    leaf->insert(1, 20, 21);
    leaf->insert(2, 30, 31);
    node->insert(0, 30, leaf);
  }
  {
    auto leaf = Leaf::create(6);
    leaf->insert(0, 40, 41);
    leaf->insert(1, 50, 51);
    leaf->insert(2, 60, 61);
    node->insert(1, 65, leaf);
  }
  {
    auto leaf = Leaf::create(4);
    leaf->insert(0, 70, 71);
    leaf->insert(1, 80, 81);
    leaf->insert(2, 90, 91);
    node->getMutableValue(2) = leaf;
  }
  TestBTree bt{node, 1};
  EXPECT_EQ(9, bt.size());

  uint32_t value{};
  EXPECT_TRUE(bt.lookup(10, value) && value == 11);
  EXPECT_TRUE(bt.lookup(20, value) && value == 21);
  EXPECT_TRUE(bt.lookup(30, value) && value == 31);
  EXPECT_TRUE(bt.lookup(40, value) && value == 41);
  EXPECT_TRUE(bt.lookup(50, value) && value == 51);
  EXPECT_TRUE(bt.lookup(60, value) && value == 61);
  EXPECT_TRUE(bt.lookup(70, value) && value == 71);
  EXPECT_TRUE(bt.lookup(80, value) && value == 81);
  EXPECT_TRUE(bt.lookup(90, value) && value == 91);
}

TEST(BTree, violationOrder1) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto leaf1 = Leaf::create(4);
  leaf1->insert(0, 10, 11);
  leaf1->insert(1, 20, 21);
  leaf1->insert(2, 30, 31);

  auto leaf2 = Leaf::create(6);
  leaf2->insert(0, 40, 41);
  leaf2->insert(1, 50, 51);
  leaf2->insert(2, 60, 61);

  auto leaf3 = Leaf::create(4);
  leaf3->insert(0, 70, 71);
  leaf3->insert(1, 80, 81);
  leaf3->insert(2, 90, 91);

  auto node = Node::create(4);
  node->insert(0, 30, leaf1);
  node->insert(1, 59, leaf2);
  node->getMutableValue(2) = leaf3;

  EXPECT_THROW(TestBTree(node, 1), std::logic_error);

  // Manually destroy or get leaks
  node->destroy();
  leaf3->destroy();
  leaf2->destroy();
  leaf1->destroy();
}

TEST(BTree, violationOrder2) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 6>>;
  using Node = typename TestBTree::Node;
  using Leaf = typename TestBTree::Leaf;

  auto leaf1 = Leaf::create(4);
  leaf1->insert(0, 10, 11);
  leaf1->insert(1, 20, 21);
  leaf1->insert(2, 30, 31);

  auto leaf2 = Leaf::create(6);
  leaf2->insert(0, 40, 41);
  leaf2->insert(1, 50, 51);
  leaf2->insert(2, 60, 61);

  auto leaf3 = Leaf::create(4);
  leaf3->insert(0, 64, 71);
  leaf3->insert(1, 80, 81);
  leaf3->insert(2, 90, 91);

  auto node = Node::create(4);
  node->insert(0, 30, leaf1);
  node->insert(1, 65, leaf2);
  node->getMutableValue(2) = leaf3;

  EXPECT_THROW(TestBTree(node, 1), std::logic_error);

  // Manually destroy or get leaks
  node->destroy();
  leaf3->destroy();
  leaf2->destroy();
  leaf1->destroy();
}

TEST(BTree, RandomOps) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<6, 6, 8>>;
  using std::chrono::system_clock;

  // Test different trees of height 2 (root, node, leaf).
  std::vector<uint32_t> keys;
  uint32_t seed = system_clock::to_time_t(system_clock::now()) % 1000;
  std::minstd_rand rg(seed);
  for (uint32_t i = 0; i < 20'000u; i++) {
    keys.resize(8 * 6 * 6);
    TestBTree bt;
    for (auto& k : keys) {
      k = rg();
      bt.insert(k, k / 5);
    }
    bt.validate();
    EXPECT_LE(3, bt.height());
    std::shuffle(keys.begin(), keys.end(), rg);
    for (size_t removeCount = rg() % (keys.size() / 4) + 3 * keys.size() / 4;
         removeCount > 0;
         removeCount--) {
      bt.remove(keys.back());
      keys.pop_back();
    }
    bt.validate();
    for (auto k : keys) {
      uint32_t value{};
      EXPECT_TRUE(bt.lookup(k, value) && value == k / 5)
          << "run #" << i << " seed " << seed;
    }
    bt.destroy();
    EXPECT_EQ(0, bt.getMemoryStats().totalMemory());
  }
}

struct MockTraverseCallback {
  MOCK_METHOD2(call, void(uint32_t key, uint32_t value));
};

TEST(BTree, Traverse) {
  using TestBTree = BTree<uint32_t, uint32_t, BTreeTraits<4, 4, 4>>;

  TestBTree bt;
  MockTraverseCallback cb;

  { // Guarantee that traversal will be in order
    testing::InSequence inSeq;
    for (int i = 0; i < 10; i++) {
      bt.insert(i, i * 100);
      EXPECT_CALL(cb, call(i, i * 100));
    }
  }

  bt.traverse(bindThis(&MockTraverseCallback::call, cb));
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook
