#include <folly/String.h>
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
}
}
}

using namespace facebook::cachelib;
using namespace facebook::cachelib::tests;
using namespace std;

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
