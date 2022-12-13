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
#include <folly/Random.h>

#include <memory>
#include <vector>

namespace facebook {
namespace cachelib {
namespace benchmarks {

constexpr unsigned int kMMTypeBenchMaxThreads = 64;

// Benchmarks for a given MMType. See MMTypeBench.cpp for an example.
template <typename MMType>
struct MMTypeBench {
  // our own implementation of node that will be put inside the container.
  struct Node {
    enum class Flags : uint8_t {
      kMMFlag0 = 0,
      kMMFlag1 = 1,
      kMMFlag2 = 2,
    };

    explicit Node(int id) : id_(id), inContainer_{false} {}

    int getId() const noexcept { return id_; }

    template <Flags flagBit>
    void setFlag() {
      flags_ |= static_cast<uint8_t>(1) << static_cast<uint8_t>(flagBit);
    }

    template <Flags flagBit>
    void unSetFlag() {
      flags_ &= ~(static_cast<uint8_t>(1) << static_cast<uint8_t>(flagBit));
    }

    template <Flags flagBit>
    bool isFlagSet() const {
      return flags_ &
             (static_cast<uint8_t>(1) << static_cast<uint8_t>(flagBit));
    }

   protected:
    bool isInMMContainer() const noexcept { return inContainer_; }

    void markInMMContainer() { inContainer_ = true; }

    void unmarkInMMContainer() { inContainer_ = false; }

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
    bool inContainer_{false};
    uint8_t flags_{0};
    friend typename MMType::template Container<Node, &Node::mmHook_>;
    friend struct MMTypeBench<MMType>;
  };

  // container definition for the benchmark.
  using Container = typename MMType::template Container<Node, &Node::mmHook_>;

  using Config = typename MMType::Config;

  // Pointer to container for all benchmark threads
  std::unique_ptr<Container> c;
  std::array<std::vector<std::unique_ptr<Node>>, kMMTypeBenchMaxThreads> tNodes;

  void createContainer(Config config);
  void deleteContainer();
  void createNodes(unsigned int numNodes,
                   unsigned int numThreads,
                   bool addNodesToContainer);
  void benchAdd(unsigned int threadNum);
  void benchRemove(unsigned int numNodes, unsigned int threadNum);
  void benchRemoveIterator(unsigned int numNodes);
  void benchRecordAccessRead(unsigned int numNodes,
                             unsigned int threadNum,
                             unsigned int numAccess);
  void benchRecordAccessReadSeq(unsigned int threadNum, unsigned int numAccess);
  void benchRecordAccessWrite(unsigned int numNodes,
                              unsigned int threadNum,
                              unsigned int numAccess);

  static void addNodes(Container& c, std::vector<std::unique_ptr<Node>>& nodes);
};

template <typename MMType>
void MMTypeBench<MMType>::createContainer(Config config) {
  c = std::make_unique<Container>(
      config, typename MMTypeBench<MMType>::Node::PtrCompressor{});
}

template <typename MMType>
void MMTypeBench<MMType>::deleteContainer() {
  // Delete the container
  c.reset();
}

template <typename MMType>
void MMTypeBench<MMType>::createNodes(unsigned int numNodes,
                                      unsigned int numThreads,
                                      bool addNodesToContainer) {
  // Create numNodes nodes for each of the numThreads threads/
  for (unsigned int i = 0; i < numThreads; i++) {
    for (unsigned int j = 0; j < numNodes; j++) {
      int nodeNum = i * numNodes + j;
      tNodes[i].emplace_back(new Node{nodeNum});
    }

    if (addNodesToContainer) {
      // Add nodes to the container.
      addNodes(*c, tNodes[i]);
    }
  }
}

template <typename MMType>
void MMTypeBench<MMType>::addNodes(Container& c,
                                   std::vector<std::unique_ptr<Node>>& nodes) {
  // Add nodes to the container.
  for (auto& node : nodes) {
    c.add(*node);
  }
}

template <typename MMType>
void MMTypeBench<MMType>::benchAdd(unsigned int threadNum) {
  // Add specified nodes to the shared container using add() method.
  // Main thread will delete these nodes at the end.
  addNodes(*c, tNodes[threadNum]);
}

template <typename MMType>
void MMTypeBench<MMType>::benchRemove(unsigned int numNodes,
                                      unsigned int threadNum) {
  // Remove our nodes from the shared container in sequential order.
  //
  // Note that the main thread has already preallocated/added our nodes to
  // the container and our nodes are located in "tNodes[threadNum] entry.
  //
  unsigned int count = 0;

  for (auto& node : tNodes[threadNum]) {
    if (count++ < numNodes) {
      c->remove(*node);
    }
  }
}

template <typename MMType>
void MMTypeBench<MMType>::benchRemoveIterator(unsigned int numNodes) {
  // Remove our nodes from the shared container using remove iterator.
  //
  // Since nodes order in the container is not guaranteed and we are simply
  // benchmarking remove operation, we remove desired number of nodes from
  // the container, irrespective of which thread they belong to.. Main thread
  // will deallocate those nodes at the end.
  //
  // no need of iter++ since remove will do that.
  for (unsigned int deleted = 0; deleted < numNodes; deleted++) {
    c->withEvictionIterator([this](auto&& iter) {
      if (iter) {
        c->remove(iter);
      }
    });
  }
}

template <typename MMType>
void MMTypeBench<MMType>::benchRecordAccessRead(unsigned int /* numNodes */,
                                                unsigned int threadNum,
                                                unsigned int numAccess) {
  // Perform basic access operation on our nodes, preallocated by the main
  // thread. Access our nodes in the container randomly with kRead access mode.

  while (numAccess-- > 0) {
    auto& node = *(tNodes[threadNum].begin() +
                   folly::Random::rand32() % tNodes[threadNum].size());
    c->recordAccess(*node, AccessMode::kRead);
  }
}

template <typename MMType>
void MMTypeBench<MMType>::benchRecordAccessReadSeq(unsigned int threadNum,
                                                   unsigned int numAccess) {
  // Perform basic access operation on our nodes, preallocated by the main
  // thread. Access our nodes in the container randomly with kRead access mode.

  uint32_t nodeNum = 0;
  auto size = tNodes[threadNum].size();
  while (numAccess-- > 0) {
    auto& node = *(tNodes[threadNum].begin() + nodeNum);
    if (++nodeNum == size) {
      nodeNum = 0;
    }
    c->recordAccess(*node, AccessMode::kRead);
  }
}

template <typename MMType>
void MMTypeBench<MMType>::benchRecordAccessWrite(unsigned int /* numNodes */,
                                                 unsigned int threadNum,
                                                 unsigned int numAccess) {
  // Perform basic access operation on our nodes, preallocated by the main
  // thread.
  //
  // Access our nodes in the container randomly with the given access mode.

  while (numAccess-- > 0) {
    auto& node = *(tNodes[threadNum].begin() +
                   folly::Random::rand32() % tNodes[threadNum].size());
    c->recordAccess(*node, AccessMode::kWrite);
  }
}
} // namespace benchmarks
} // namespace cachelib
} // namespace facebook
