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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <vector>

#include "cachelib/allocator/datastruct/SList.h"

namespace facebook {
namespace cachelib {

const int kAllocSize = 64;

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

  SListHook<SListNode> hook_{};

  char* getContent() { return &(content[0]); }

 private:
  char content[kAllocSize];
};
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib;
using namespace std;

using folly::BenchmarkSuspender;
using SListImpl = SList<SListNode, &SListNode::hook_>;

vector<SListNode> getInput(unsigned n) noexcept {
  vector<SListNode> input;
  input.reserve(n);
  for (unsigned i = 0; i < n; i++) {
    input.emplace_back();
  }
  return input;
}

BENCHMARK(VectorInsert, n) {
  BenchmarkSuspender suspender;
  vector<SListNode> input = getInput(n), vec;
  suspender.dismiss();

  for (unsigned i = 0; i < n; i++) {
    vec.push_back(std::move(input[i]));
  }
}

BENCHMARK_RELATIVE(SListInsert, n) {
  BenchmarkSuspender suspender;
  vector<SListNode> input = getInput(n);
  SListImpl slist{SListNode::PtrCompressor{}};
  suspender.dismiss();

  for (unsigned i = 0; i < n; i++) {
    slist.insert(input[i]);
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(VectorPop, n) {
  BenchmarkSuspender suspender;
  vector<SListNode> input = getInput(n);
  suspender.dismiss();

  for (unsigned i = 0; i < n; i++) {
    input.pop_back();
  }
}

BENCHMARK_RELATIVE(SListPop, n) {
  BenchmarkSuspender suspender;
  vector<SListNode> input = getInput(n);
  SListImpl slist{SListNode::PtrCompressor{}};
  for (unsigned i = 0; i < n; i++) {
    slist.insert(input[i]);
  }
  suspender.dismiss();

  for (unsigned i = 0; i < n; i++) {
    slist.pop();
  }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(VectorRemove, n) {
  BenchmarkSuspender suspender;
  vector<SListNode> input = getInput(n);
  suspender.dismiss();

  // Keep erasing the second-last element
  for (unsigned i = 1; i < n; i++) {
    input.erase(input.end() - 2);
  }
}

BENCHMARK_RELATIVE(SListRemove, n) {
  BenchmarkSuspender suspender;
  vector<SListNode> input = getInput(n);
  SListImpl slist{SListNode::PtrCompressor{}};
  for (unsigned i = 0; i < n; i++) {
    slist.insert(input[i]);
  }
  suspender.dismiss();

  // Keep erasing the second element
  for (unsigned i = 1; i < n; i++) {
    slist.remove(++slist.begin());
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

/*

Results with
  args = ['-json', '-bm_min_iters=10000', ]

To keep the benchmark fair, we insert / remove near the tail of the vector
and the head of the SList.

kAllocSize = 64
============================================================================
cachelib/allocator/datastruct/benchmarks/SListBench.cpprelative  time/iter
iters/s
============================================================================
VectorInsert                                                70.71ns   14.14M
SListInsert                                      340.37%    20.77ns   48.14M
----------------------------------------------------------------------------
VectorPop                                                   32.38ns   30.89M
SListPop                                         192.78%    16.79ns   59.54M
----------------------------------------------------------------------------
VectorRemove                                               178.46ns    5.60M
SListRemove                                      270.68%    65.93ns   15.17M
============================================================================

kAllocSize = 1024
============================================================================
cachelib/allocator/datastruct/benchmarks/SListBench.cpprelative  time/iter
iters/s
============================================================================
VectorInsert                                               239.88ns    4.17M
SListInsert                                     1122.08%    21.38ns   46.78M
----------------------------------------------------------------------------
VectorPop                                                   32.71ns   30.57M
SListPop                                         186.69%    17.52ns   57.07M
----------------------------------------------------------------------------
VectorRemove                                               230.35ns    4.34M
SListRemove                                      359.18%    64.13ns   15.59M
============================================================================

kAllocSize = 4096
============================================================================
cachelib/allocator/datastruct/benchmarks/SListBench.cpprelative  time/iter
iters/s
============================================================================
VectorInsert                                               708.14ns    1.41M
SListInsert                                     2942.85%    24.06ns   41.56M
----------------------------------------------------------------------------
VectorPop                                                   34.44ns   29.03M
SListPop                                         173.95%    19.80ns   50.51M
----------------------------------------------------------------------------
VectorRemove                                               439.40ns    2.28M
SListRemove                                      643.27%    68.31ns   14.64M
============================================================================

*/
