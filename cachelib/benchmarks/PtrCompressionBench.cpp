#include <set>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"

using namespace facebook::cachelib;
using CompressedPtr = MemoryAllocator::CompressedPtr;
using AllocPair = std::pair<void*, CompressedPtr>;

namespace {

const unsigned int numPools = 4;

std::unique_ptr<MemoryAllocator> m;
std::vector<AllocPair> validAllocs;
std::vector<AllocPair> validAllocsAlt;

std::set<uint32_t> getAllocSizes() {
  // defaults from tao for allocation sizes.

  constexpr double factor = 1.25;
  return util::generateAllocSizes(factor);
}

void buildAllocs(size_t poolSize) {
  std::unordered_map<PoolId, const std::set<uint32_t>> pools;
  std::string poolName = "foo";
  for (unsigned int i = 0; i < numPools; i++) {
    auto sizes = getAllocSizes();
    auto pid =
        m->addPool(poolName + folly::to<std::string>(i), poolSize, sizes);
    pools.insert({pid, sizes});
  }

  auto makeAllocs = [](PoolId pid, MemoryAllocator* ma,
                       const std::set<uint32_t>& sizes) {
    unsigned int numAllocations = 0;
    do {
      numAllocations = 0;
      for (const auto size : sizes) {
        void* alloc = ma->allocate(pid, size);
        XDCHECK_GE(size, CompressedPtr::getMinAllocSize());
        if (alloc != nullptr) {
          validAllocs.push_back({alloc, ma->compress(alloc)});
          validAllocsAlt.push_back({alloc, ma->compressAlt(alloc)});
          numAllocations++;
        }
      }
    } while (numAllocations > 0);
  };
  for (auto pool : pools) {
    makeAllocs(pool.first, m.get(), pool.second);
  }
}
} // namespace

BENCHMARK(CompressionAlt) {
  for (const auto& alloc : validAllocsAlt) {
    CompressedPtr c = m->compressAlt(alloc.first);
    folly::doNotOptimizeAway(c);
  }
}

BENCHMARK_RELATIVE(Compression) {
  for (const auto& alloc : validAllocs) {
    CompressedPtr c = m->compress(alloc.first);
    folly::doNotOptimizeAway(c);
  }
}

BENCHMARK(DeCompressAlt) {
  for (const auto& alloc : validAllocsAlt) {
    void* ptr = m->unCompressAlt(alloc.second);
    folly::doNotOptimizeAway(ptr);
  }
}

BENCHMARK_RELATIVE(DeCompress) {
  for (const auto& alloc : validAllocs) {
    void* ptr = m->unCompress(alloc.second);
    folly::doNotOptimizeAway(ptr);
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  auto allocSizes = getAllocSizes();

  // create enough memory for all the pools and alloc classes.
  const size_t poolSize = allocSizes.size() * 2 * Slab::kSize;
  // allocate enough memory for all the pools plus slab headers.
  const size_t totalSize = numPools * poolSize + 2 * Slab::kSize;

  std::unique_ptr<char[]> alloced(new char[totalSize]);
  MemoryAllocator::Config c(allocSizes, false /* enableZeroedSlabAllocs */,
                            true /* disableCoredump */, true /* lockMemory */);
  m.reset(new MemoryAllocator{c, alloced.get(), totalSize});

  buildAllocs(poolSize);
  folly::runBenchmarks();
  return 0;
}

/*
Current results with -bm_min_iters=10000 on

CLANG and inlining
============================================================================
cachelib/allocator/benchmarks/PtrCompressionBench.cpprelative  time/iter iters/s
============================================================================
Compression                                           2.36us  423.14K
DeCompress                                            1.41us  707.58K
CompressionAlt                                        1.30us  771.84K
DeCompressAlt                                         4.55us  219.96K
============================================================================**

GCC without inlining
============================================================================
cachelib/allocator/benchmarks/PtrCompressionBench.cpprelative  time/iter iters/s
============================================================================
Compression                                           2.25us  443.80K
DeCompress                                          832.93ns    1.20M
CompressionAlt                                      829.69ns    1.21M
DeCompressAlt                                         4.62us  216.64K
============================================================================

GCC with always inlining
============================================================================
cachelib/allocator/benchmarks/PtrCompressionBench.cpprelative  time/iter iters/s
============================================================================
Compression                                           3.34us  299.13K
DeCompress                                          752.00ns    1.33M
CompressionAlt                                      283.99ns    3.52M
DeCompressAlt                                         6.31us  158.42K
============================================================================

*/
