#pragma once

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <folly/Optional.h>
#include <folly/logging/xlog.h>

#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace details {
// Inserts @what into first position in the range @first, @last.
// Assumes enough memory for the new element.
template <typename T>
void insertFirst(T* first, T* last, NoDeduce<T> what) {
  std::copy_backward(first, last, last + 1);
  *first = what;
}

// Removes first element in the range @first, @last. Doesn't shrink buffer.
template <typename T>
void removeFirst(T* first, T* last) {
  std::copy(first + 1, last, first);
}
} // namespace details

enum class BTreeNodeType : uint32_t {
  Leaf = 0,
  Node = 1,
};

// See data members for layout documentation
template <BTreeNodeType Type, typename Key, typename Value>
class BTreeNode {
 public:
  static constexpr auto kType{Type};

  static size_t allocSize(uint32_t capacity) {
    return sizeof(Key) * capacity + sizeof(Value) * (capacity + 1) +
           sizeof(BTreeNode);
  }

  // Estimated size that malloc actually allocates
  static size_t allocSlotSize(uint32_t capacity) {
    return mallocSlotSize(allocSize(capacity));
  }

  static BTreeNode* create(uint32_t capacity) {
    return new (std::malloc(allocSize(capacity))) BTreeNode{capacity};
  }

  void destroy() {
    this->~BTreeNode();
    std::free(this);
  }

  uint32_t size() const { return size_; }

  uint32_t capacity() const { return capacity_; }

  Key getKey(uint32_t i) const { return keys_[i]; }

  Key getLastKey() const { return keys_[size_ - 1]; }

  Value getValue(uint32_t i) const {
    XDCHECK_LE(i, size_);
    return data_[i];
  }

  Value& getMutableValue(uint32_t i) {
    XDCHECK_LE(i, size_);
    return data_[i];
  }

  // Returns index of @key in node keys (size if not found).
  uint32_t find(Key key) const {
    XDCHECK(isSorted());
    return std::lower_bound(keys_,
                            keys_ + size_,
                            key,
                            [](auto k, auto e) { return k < e; }) -
           keys_;
  }

  void insert(uint32_t where, Key key, Value value) {
    XDCHECK(isSorted());
    details::insertFirst(keys_ + where, keys_ + size_, key);
    details::insertFirst(data_ + where, data_ + size_ + 1, value);
    size_++;
    XDCHECK(isSorted());
  }

  void remove(uint32_t where) {
    XDCHECK(isSorted());
    details::removeFirst(keys_ + where, keys_ + size_);
    details::removeFirst(data_ + where, data_ + size_ + 1);
    size_--;
    XDCHECK(isSorted());
  }

  void splitTo(BTreeNode& out1, BTreeNode& out2, uint32_t where) {
    XDCHECK_EQ(out1.size_, 0u);
    XDCHECK_EQ(out2.size_, 0u);
    XDCHECK_GE(out1.capacity_, where);
    XDCHECK_GE(out2.capacity_, size_ - where);
    std::copy(keys_, keys_ + where, out1.keys_);
    std::copy(data_, data_ + where, out1.data_);
    out1.size_ = where;
    out1.data_[out1.size_] = {};
    std::copy(keys_ + where, keys_ + size_, out2.keys_);
    std::copy(data_ + where, data_ + size_ + 1, out2.data_);
    out2.size_ = size_ - where;
    XDCHECK_GT(out1.size_, 0u);
    XDCHECK_GT(out2.size_, 0u);
    XDCHECK(out1.isSorted());
    XDCHECK(out2.isSorted());
  }

  void mergeFrom(const BTreeNode& in1, const BTreeNode& in2) {
    XDCHECK_GE(capacity_, in1.size_ + in2.size_);
    std::copy(in1.keys_, in1.keys_ + in1.size_, keys_);
    std::copy(in1.data_, in1.data_ + in1.size_, data_);
    std::copy(in2.keys_, in2.keys_ + in2.size_, keys_ + in1.size_);
    std::copy(in2.data_, in2.data_ + in2.size_ + 1, data_ + in1.size_);
    size_ = in1.size_ + in2.size_;
    XDCHECK(isSorted());
  }

  void copyFrom(const BTreeNode& node) {
    std::copy(node.keys_, node.keys_ + node.size_, keys_);
    std::copy(node.data_, node.data_ + node.size_ + 1, data_);
    size_ = node.size_;
    XDCHECK(isSorted());
  }

  // Takes two nodes @in1 and @in2, merges them and then splits so @firstPart
  // keys+data go into @out1 and the rest into @out2. Code below produces same
  // result without temporary allocation.
  static void balance(const BTreeNode& in1,
                      const BTreeNode& in2,
                      uint32_t firstPart,
                      BTreeNode& out1,
                      BTreeNode& out2) {
    if (in1.size_ > firstPart) {
      // Copy @firstPart from node1 to output1, and the rest of node1 and
      // whole node2 to output2.
      std::copy(in1.keys_, in1.keys_ + firstPart, out1.keys_);
      std::copy(in1.data_, in1.data_ + firstPart, out1.data_);
      std::copy(in1.keys_ + firstPart, in1.keys_ + in1.size_, out2.keys_);
      std::copy(in1.data_ + firstPart, in1.data_ + in1.size_ + 1, out2.data_);
      const auto piece = in1.size_ - firstPart;
      std::copy(in2.keys_, in2.keys_ + in2.size_, out2.keys_ + piece);
      std::copy(in2.data_, in2.data_ + in2.size_ + 1, out2.data_ + piece);
    } else {
      // Copy whole node1 and @piece of node2 to output1, and the rest of
      // node2 to output2.
      std::copy(in1.keys_, in1.keys_ + in1.size_, out1.keys_);
      std::copy(in1.data_, in1.data_ + in1.size_, out1.data_);
      const auto piece = firstPart - in1.size_;
      std::copy(in2.keys_, in2.keys_ + piece, out1.keys_ + in1.size_);
      std::copy(in2.data_, in2.data_ + piece, out1.data_ + in1.size_);
      std::copy(in2.keys_ + piece, in2.keys_ + in2.size_, out2.keys_);
      std::copy(in2.data_ + piece, in2.data_ + in2.size_ + 1, out2.data_);
    }
    out1.size_ = firstPart;
    out1.data_[out1.size_] = {};
    out2.size_ = in1.size_ + in2.size_ - firstPart;
  }

  bool isSorted() const noexcept {
    return std::is_sorted(keys_, keys_ + size_);
  }

 private:
  template <typename K, typename V, typename T>
  friend class BTree;

  explicit BTreeNode(uint32_t capacity)
      : capacity_{capacity}, data_{reinterpret_cast<Value*>(keys_ + capacity)} {
    static_assert(sizeof(*this) == 8 + sizeof(void*),
                  "if add new data members, make sure layout is compact");

    XDCHECK_GE(capacity_, 2u);
    data_[0] = {};
  }
  BTreeNode(const BTreeNode&) = delete;
  BTreeNode& operator=(const BTreeNode&) = delete;
  ~BTreeNode() = default;

  const uint32_t capacity_{};
  uint32_t size_{};
  // Layout: capacity keys followed by (capacity + 1) data (value/pointer).
  Value* const data_{};
  Key keys_[];
};

struct BTreeMemoryStats {
  size_t leafMemory{};
  size_t nodeMemory{};

  size_t totalMemory() const { return leafMemory + nodeMemory; }
};

template <uint32_t kNodeSize, uint32_t kSmallLeafSize, uint32_t kLargeLeafSize>
struct BTreeTraits {
  static constexpr uint32_t nodeSize() { return kNodeSize; }

  static constexpr uint32_t smallLeafSize() { return kSmallLeafSize; }

  static constexpr uint32_t largeLeafSize() { return kLargeLeafSize; }
};

// B+ tree (actually, variation of (a,b)-tree).
//
// Use void* in all places where it is not known if the pointer is a node or
// a leaf. It makes all sites where we decide about type explicit.
//
// Traits is a concept defined as follows:
// concept Traits = requires {
//   BTreeTraits();
//   { nodeSize() } -> uint32_t;
//   { smallLeafSize() } -> uint32_t;
//   { largeLeafSize() } -> uint32_t;
// };
// Sizes above are number of keys the node can store.
template <typename K, typename V, typename Traits>
class BTree : public Traits {
 public:
  using Key = K;
  using Value = V;

  static_assert(std::is_integral<Key>::value && std::is_trivial<Value>::value,
                "B tree type requirements fail");
  static_assert(Traits::smallLeafSize() > 1 &&
                    Traits::smallLeafSize() > Traits::largeLeafSize() / 2,
                "leaf sizes invalid");
  static_assert(Traits::nodeSize() > 1, "node size invalid");

  using Node = BTreeNode<BTreeNodeType::Node, Key, void*>;
  using Leaf = BTreeNode<BTreeNodeType::Leaf, Key, Value>;

  BTree() = default;

  // Takes ownership of the BTree. Validates before accepting. See @validate
  // for more comments.
  BTree(void* root, uint32_t height) {
    if (root != nullptr) {
      uint32_t size{};
      Key prevKey{};
      validateRecursive(height, root, prevKey, size);
      root_ = root;
      height_ = height;
      size_ = size;
    }
  }

  BTree(const BTree&) = delete;
  BTree& operator=(const BTree&) = delete;

  BTree(BTree&& other) noexcept
      : root_{other.root_}, height_{other.height_}, size_{other.size_} {
    other.reset();
  }

  BTree& operator=(BTree&& other) noexcept {
    this->~BTree();
    new (this) BTree{std::move(other)};
    return *this;
  }

  ~BTree() { destroy(); }

  void destroy() {
    destroyRecursive(height_, root_);
    reset();
  }

  // Return true if replaced an existing entry
  bool FOLLY_NOINLINE insert(Key key, Value value) {
    if (root_ == nullptr) {
      root_ = Leaf::create(Traits::smallLeafSize());
    }
    InsertOutput r = insertRecursive(key, value, height_, root_);
    // Insert will return two pointers if split happened, in which case we grow
    // tree and create a new root node. It may return only @ptr1, which means
    // the root was reallocated and we have to update it.
    if (r.ptr2 != nullptr) {
      auto root = Node::create(Traits::nodeSize());
      root->size_ = 1;
      root->keys_[0] = r.median;
      root->data_[0] = r.ptr1;
      root->data_[1] = r.ptr2;
      root_ = root;
      height_++;
    } else if (r.ptr1 != nullptr) {
      root_ = r.ptr1;
    }
    return r.replaced;
  }

  folly::Optional<Value> FOLLY_NOINLINE remove(Key key) {
    if (root_ == nullptr) {
      return folly::none;
    }
    auto found = removeRecursive(key, height_, root_, nullptr, 0);
    if (height_ > 0) {
      auto root = reinterpret_cast<Node*>(root_);
      if (root->size() == 0) {
        root_ = root->data_[0];
        root->destroy();
        height_--;
      }
    } else if (size_ == 0) {
      auto root = reinterpret_cast<Leaf*>(root_);
      root->destroy();
      root_ = nullptr;
    }
    return found;
  }

  bool FOLLY_NOINLINE lookup(Key key, Value& value) const {
    if (root_ == nullptr) {
      return false;
    }
    auto p = root_;
    for (auto h = height_; h != 0; h--) {
      XDCHECK_NE(p, nullptr);
      auto node = reinterpret_cast<Node*>(p);
      p = node->data_[node->find(key)];
    }
    auto leaf = reinterpret_cast<Leaf*>(p);
    auto i = leaf->find(key);
    if (i < leaf->size() && leaf->getKey(i) == key) {
      value = leaf->data_[i];
      return true;
    }
    return false;
  }

  void traverse(const std::function<void(Key, Value)>& traverseCB) const {
    if (root_ == nullptr) {
      return;
    }
    traverseRecursive(traverseCB, height_, root_);
  }

  uint32_t size() const { return size_; }

  uint32_t height() const { return height_; }

  BTreeMemoryStats getMemoryStats() const {
    BTreeMemoryStats stats;
    computeStatsRecursive(stats, height_, root_);
    return stats;
  }

  // For debug/test purposes only
  void* root() { return root_; }

  // Check B tree invariants. If violated, throws std::logic_error.
  void validate(Key minKey = Key{}) const {
    if (root_ != nullptr) {
      uint32_t size{};
      validateRecursive(height_, root_, minKey, size);
    } else {
      if (!(height_ == 0 && size_ == 0)) {
        throwInvariantViolation();
      }
    }
  }

 private:
  static void validateRecursive(uint32_t height,
                                void* ptr,
                                Key& inOrder,
                                uint32_t& size);

  static void throwInvariantViolation() {
    throw std::logic_error("btree invariant violation");
  }

  struct InsertOutput {
    bool replaced{false};
    Key median{};
    void* ptr1{};
    void* ptr2{};
  };
  InsertOutput insertRecursive(Key key,
                               Value value,
                               uint32_t height,
                               void* ptr);

  // Returns the address of the entry if found and removed successfully.
  // Returns false otherwise.
  folly::Optional<Value> removeRecursive(
      Key key, uint32_t height, void* ptr, Node* parent, uint32_t child);

  template <typename Child>
  static void balance(Node* node,
                      uint32_t child,
                      uint32_t smallCapacity,
                      uint32_t largeCapacity);

  void computeStatsRecursive(BTreeMemoryStats& stats,
                             uint32_t height,
                             void* ptr) const;

  void destroyRecursive(uint32_t height, void* ptr);

  void traverseRecursive(const std::function<void(Key, Value)>& traverseCB,
                         uint32_t height,
                         void* ptr) const;

  void reset() {
    root_ = nullptr;
    size_ = 0;
    height_ = 0;
  }

  void* root_{};
  uint32_t height_{}; // Grows from leaf: leaf has height of 0
  uint32_t size_{};
};

template <typename K, typename V, typename T>
void BTree<K, V, T>::validateRecursive(uint32_t height,
                                       void* ptr,
                                       Key& inOrder,
                                       uint32_t& size) {
  if (ptr == nullptr) {
    throwInvariantViolation();
  }
  // Do not check for load factor actually, because all operations on the tree
  // work with any load factor.
  if (height == 0) {
    const auto leaf = reinterpret_cast<Leaf*>(ptr);
    const auto capacity = leaf->capacity();
    if (!(capacity == T::smallLeafSize() || capacity == T::largeLeafSize()) ||
        !leaf->isSorted() || inOrder > leaf->getKey(0)) {
      throwInvariantViolation();
    }
    inOrder = leaf->getLastKey();
    size += leaf->size();
  } else {
    const auto node = reinterpret_cast<Node*>(ptr);
    // Don't check if sorted, because for loop below effectively does that
    if (node->capacity() != T::nodeSize()) {
      throwInvariantViolation();
    }
    for (uint32_t i = 0; i < node->size(); i++) {
      validateRecursive(height - 1, node->data_[i], inOrder, size);
      if (inOrder > node->getKey(i)) {
        throwInvariantViolation();
      }
      inOrder = node->getKey(i);
    }
    if (node->data_[node->size()] != nullptr) {
      validateRecursive(height - 1, node->data_[node->size()], inOrder, size);
    }
  }
}

template <typename K, typename V, typename T>
typename BTree<K, V, T>::InsertOutput BTree<K, V, T>::insertRecursive(
    Key key, Value value, uint32_t height, void* ptr) {
  XDCHECK_NE(ptr, nullptr);
  InsertOutput res;
  if (height == 0) {
    auto leaf = reinterpret_cast<Leaf*>(ptr);
    auto i = leaf->find(key);
    if (i < leaf->size() && leaf->getKey(i) == key) {
      leaf->data_[i] = value;
      res.replaced = true;
      return res;
    }
    // Not found, insert if we have enough space in the node. If not, try to
    // expand the node. Otherwise, split on two small and insert median key
    // to the parent node.
    if (leaf->size() < leaf->capacity()) {
      leaf->insert(i, key, value);
    } else if (leaf->capacity() < T::largeLeafSize()) {
      auto largeLeaf = Leaf::create(T::largeLeafSize());
      largeLeaf->copyFrom(*leaf);
      largeLeaf->insert(i, key, value);
      res.ptr1 = largeLeaf;
      leaf->destroy();
    } else {
      auto leaf1 = Leaf::create(T::smallLeafSize());
      auto leaf2 = Leaf::create(T::smallLeafSize());
      leaf->splitTo(*leaf1, *leaf2, leaf->size() / 2);
      leaf->destroy();
      res.median = leaf1->getLastKey();
      res.ptr1 = leaf1;
      res.ptr2 = leaf2;
      if (key <= res.median) {
        leaf1->insert(i, key, value);
      } else {
        leaf2->insert(i - leaf1->size(), key, value);
      }
    }
    size_++;
  } else {
    auto node = reinterpret_cast<Node*>(ptr);
    auto i = node->find(key);
    auto r = insertRecursive(key, value, height - 1, node->data_[i]);
    res.replaced = r.replaced;
    if (r.ptr2 != nullptr) {
      XDCHECK_EQ(node->capacity(), T::nodeSize());
      if (node->size() < node->capacity()) {
        node->insert(i, r.median, r.ptr1);
        node->data_[i + 1] = r.ptr2;
      } else {
        auto node1 = Node::create(T::nodeSize());
        auto node2 = Node::create(T::nodeSize());
        node->splitTo(*node1, *node2, node->size() / 2);
        node->destroy();
        res.median = node1->getLastKey();
        res.ptr1 = node1;
        res.ptr2 = node2;
        if (r.median <= res.median) {
          node1->insert(i, r.median, r.ptr1);
          node1->data_[i + 1] = r.ptr2;
        } else {
          i -= node1->size();
          node2->insert(i, r.median, r.ptr1);
          node2->data_[i + 1] = r.ptr2;
        }
      }
    } else if (r.ptr1 != nullptr) {
      // Leaf was resized
      node->data_[i] = r.ptr1;
    }
  }
  return res;
}

template <typename K, typename V, typename T>
folly::Optional<V> BTree<K, V, T>::removeRecursive(
    Key key, uint32_t height, void* ptr, Node* parent, uint32_t child) {
  if (height == 0) {
    auto leaf = reinterpret_cast<Leaf*>(ptr);
    auto i = leaf->find(key);
    if (i >= leaf->size() || leaf->getKey(i) != key) {
      return folly::none;
    }
    Value value = leaf->data_[i];
    size_--;
    leaf->remove(i);
    if (leaf->size() < T::largeLeafSize() / 2 &&
        leaf->capacity() > T::smallLeafSize()) {
      auto small = Leaf::create(T::smallLeafSize());
      small->copyFrom(*leaf);
      leaf->destroy();
      leaf = small;
      if (parent != nullptr) {
        parent->data_[child] = leaf;
      } else {
        root_ = leaf;
      }
    }
    if (parent != nullptr && leaf->size() < T::smallLeafSize() / 2) {
      balance<Leaf>(parent, child, T::smallLeafSize(), T::largeLeafSize());
    }
    return value;
  } else {
    auto node = reinterpret_cast<Node*>(ptr);
    auto i = node->find(key);
    auto found = removeRecursive(key, height - 1, node->data_[i], node, i);
    // Rebalance if underflow. Note bar to rebalance 1/3 (not classic 1/2) to
    // always keep a room for remove and insert after rebalance. It's okay to
    // have less loaded nodes because it is about 1-3% of total BTree memory.
    // Leaves shrink/expand to increase memory utilization.
    if (parent != nullptr && node->size() < T::nodeSize() / 3) {
      balance<Node>(parent, child, T::nodeSize(), T::nodeSize());
    }
    return found;
  }
}

template <typename K, typename V, typename T>
template <typename Child>
void BTree<K, V, T>::balance(Node* parent,
                             uint32_t child,
                             uint32_t smallCapacity,
                             uint32_t largeCapacity) {
  auto sizeL = child > 0
                   ? reinterpret_cast<Child*>(parent->data_[child - 1])->size()
                   : 0u;
  auto sizeR = child + 1 <= parent->size() && parent->data_[child + 1]
                   ? reinterpret_cast<Child*>(parent->data_[child + 1])->size()
                   : 0u;
  auto merge = false;
  if (sizeL > 0 && sizeR > 0) {
    // If both neighbours present, we can choose a better one.
    if (sizeL < largeCapacity / 2 || sizeR < largeCapacity / 2) {
      // If any of them is smaller than half, choose the smallest and merge
      // into one large node.
      merge = true;
      if (sizeL < sizeR) {
        child--;
      }
    } else {
      // One of two neighbours is more than 50% populated. Pick more populated
      // and replace with two small leaves. Merges are stopped.
      if (sizeL > sizeR) {
        child--;
      }
    }
  } else if (sizeL > 0) {
    // Deal with prev neighbour only
    if (sizeL < largeCapacity / 2) {
      merge = true;
    }
    child--;
  } else {
    XDCHECK_EQ(child, 0u);
    if (sizeR < largeCapacity / 2) {
      // If any of them is smaller than half, choose the smallest and merge
      // into one large node.
      merge = true;
    }
  }
  auto cL = reinterpret_cast<Child*>(parent->data_[child]);
  auto cR = reinterpret_cast<Child*>(parent->data_[child + 1]);
  if (merge) {
    auto large = Child::create(largeCapacity);
    large->mergeFrom(*cL, *cR);
    parent->remove(child);
    parent->data_[child] = large;
  } else {
    auto newL = Child::create(smallCapacity);
    auto newR = Child::create(smallCapacity);
    Child::balance(*cL, *cR, (cL->size() + cR->size()) / 2, *newL, *newR);
    parent->data_[child] = newL;
    parent->keys_[child] = newL->getLastKey();
    parent->data_[child + 1] = newR;
  }
  cL->destroy();
  cR->destroy();
}

template <typename K, typename V, typename T>
void BTree<K, V, T>::destroyRecursive(uint32_t height, void* ptr) {
  if (ptr == nullptr) {
    return;
  }
  if (height == 0) {
    auto leaf = reinterpret_cast<Node*>(ptr);
    leaf->destroy();
  } else {
    auto node = reinterpret_cast<Node*>(ptr);
    for (uint32_t i = 0; i <= node->size_; i++) {
      destroyRecursive(height - 1, node->data_[i]);
    }
    node->destroy();
  }
}

template <typename K, typename V, typename T>
void BTree<K, V, T>::computeStatsRecursive(BTreeMemoryStats& stats,
                                           uint32_t height,
                                           void* ptr) const {
  if (ptr == nullptr) {
    return;
  }
  if (height == 0) {
    const auto leaf = reinterpret_cast<Leaf*>(ptr);
    stats.leafMemory += Leaf::allocSlotSize(leaf->capacity());
  } else {
    const auto node = reinterpret_cast<Node*>(ptr);
    stats.nodeMemory += Node::allocSlotSize(node->capacity());
    for (uint32_t i = 0; i <= node->size(); i++) {
      computeStatsRecursive(stats, height - 1, node->data_[i]);
    }
  }
}

template <typename K, typename V, typename T>
void BTree<K, V, T>::traverseRecursive(
    const std::function<void(K, V)>& traverseCB,
    uint32_t height,
    void* ptr) const {
  if (ptr == nullptr) {
    return;
  }
  if (height == 0) {
    const auto leaf = reinterpret_cast<Leaf*>(ptr);
    for (uint32_t i = 0; i < leaf->size(); i++) {
      traverseCB(leaf->getKey(i), leaf->getValue(i));
    }
  } else {
    const auto node = reinterpret_cast<Node*>(ptr);
    for (uint32_t i = 0; i <= node->size(); i++) {
      traverseRecursive(traverseCB, height - 1, node->data_[i]);
    }
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
