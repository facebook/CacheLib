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
#include <folly/Random.h>
#include <folly/init/Init.h>

#include <cstring>
#include <vector>

#include "cachelib/common/BytesEqual.h"

namespace facebook {
namespace cachelib {

/**
 * $ buck run @mode/opt cachelib/common/tests:bytes_equal_benchmark
 *    -- --bm_min_iters=1000000
 *
 * Parsing buck files: finished in 2.4 sec (100%)
 * Building: finished in 9.4 sec (100%) 722/722 jobs, 2 updated
 *   Total time: 12.1 sec
 * ============================================================================
 * cachelib/common/tests/BytesEqualBenchmark.cpp   relative  time/iter  iters/s
 * ============================================================================
 * runBytesEqual(8bytes)                                        1.57ns  635.49M
 * runMemcmp(8bytes)                                 57.39%     2.74ns  364.72M
 * ----------------------------------------------------------------------------
 * runBytesEqual(16bytes)                                       2.13ns  469.36M
 * runMemcmp(16bytes)                                70.00%     3.04ns  328.55M
 * ----------------------------------------------------------------------------
 * runBytesEqual(32bytes)                                       3.04ns  328.54M
 * runMemcmp(32bytes)                                90.92%     3.35ns  298.72M
 * ----------------------------------------------------------------------------
 * runBytesEqual(64bytes)                                       6.39ns  156.42M
 * runMemcmp(64bytes)                               175.04%     3.65ns  273.79M
 * ----------------------------------------------------------------------------
 * runBytesEqual(128bytes)                                     11.27ns   88.77M
 * runMemcmp(128bytes)                              176.31%     6.39ns  156.50M
 * ----------------------------------------------------------------------------
 * runBytesEqual(256bytes)                                     26.76ns   37.37M
 * runMemcmp(256bytes)                              266.13%    10.06ns   99.45M
 * ----------------------------------------------------------------------------
 * runBytesEqual(1024bytes)                                    29.07ns   34.40M
 * runMemcmp(1024bytes)                             102.47%    28.37ns   35.25M
 * ----------------------------------------------------------------------------
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(8bytes)                                    1.52ns  657.50M
 * runMemcmpRand(8bytes)                             37.71%     4.03ns  247.96M
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(16bytes)                                   1.52ns  657.15M
 * runMemcmpRand(16bytes)                            34.90%     4.36ns  229.34M
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(32bytes)                                   1.52ns  657.44M
 * runMemcmpRand(32bytes)                            32.92%     4.62ns  216.41M
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(64bytes)                                   1.52ns  657.59M
 * runMemcmpRand(64bytes)                            35.68%     4.26ns  234.65M
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(128bytes)                                  1.52ns  657.58M
 * runMemcmpRand(128bytes)                           33.31%     4.56ns  219.06M
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(256bytes)                                  1.52ns  657.71M
 * runMemcmpRand(256bytes)                           33.30%     4.57ns  219.05M
 * ----------------------------------------------------------------------------
 * runBytesEqualRand(1024bytes)                                 5.58ns  179.11M
 * runMemcmpRand(1024bytes)                         129.87%     4.30ns  232.61M
 * ============================================================================
 */

namespace {
template <typename Compare, typename Create>
void benchmarkCompare(Compare compare,
                      std::uint64_t bytes,
                      std::size_t iters,
                      Create create) {
  auto suspender = folly::BenchmarkSuspender{};
  auto one = create(bytes);
  auto two = create(bytes);

  suspender.dismissing([&] {
    for (auto i = std::uint64_t{0}; i < iters; ++i) {
      folly::makeUnpredictable(one);
      folly::makeUnpredictable(two);
      folly::makeUnpredictable(bytes);
      folly::makeUnpredictable(compare);

      folly::doNotOptimizeAway(compare(one.data(), two.data(), bytes));
    }
  });
}

std::vector<std::uint8_t> createMonotonic(std::uint64_t bytes) {
  auto vector = std::vector<std::uint8_t>{};
  for (auto i = std::uint64_t{0}; i < bytes; ++i) {
    vector.push_back(static_cast<std::uint8_t>(i));
  }
  return vector;
}

std::vector<std::uint8_t> createRandom(std::uint64_t bytes) {
  auto vector = std::vector<std::uint8_t>{};
  for (auto i = std::uint64_t{0}; i < bytes; ++i) {
    vector.push_back(static_cast<std::uint8_t>(folly::Random::rand32(0, 256)));
  }
  return vector;
}

void runBytesEqual(std::size_t iters, std::uint64_t bytes) {
  benchmarkCompare([](auto&&... args) { return bytesEqual(args...); },
                   bytes,
                   iters,
                   createMonotonic);
}

void runMemcmp(std::size_t iters, std::uint64_t bytes) {
  benchmarkCompare([](auto&&... args) { return std::memcmp(args...); },
                   bytes,
                   iters,
                   createMonotonic);
}

void runBytesEqualRand(std::size_t iters, std::uint64_t bytes) {
  benchmarkCompare([](auto&&... args) { return bytesEqual(args...); },
                   bytes,
                   iters,
                   createRandom);
}

void runMemcmpRand(std::size_t iters, std::uint64_t bytes) {
  benchmarkCompare([](auto&&... args) { return std::memcmp(args...); },
                   bytes,
                   iters,
                   createRandom);
}

#define BENCH_BASE(...) FB_VA_GLUE(BENCHMARK_NAMED_PARAM, (__VA_ARGS__))
#define BENCH_REL(...) FB_VA_GLUE(BENCHMARK_RELATIVE_NAMED_PARAM, (__VA_ARGS__))

BENCH_BASE(runBytesEqual, 8bytes, 8)
BENCH_REL(runMemcmp, 8bytes, 8)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqual, 16bytes, 16)
BENCH_REL(runMemcmp, 16bytes, 16)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqual, 32bytes, 32)
BENCH_REL(runMemcmp, 32bytes, 32)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqual, 64bytes, 64)
BENCH_REL(runMemcmp, 64bytes, 64)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqual, 128bytes, 128)
BENCH_REL(runMemcmp, 128bytes, 128)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqual, 256bytes, 256)
BENCH_REL(runMemcmp, 256bytes, 256)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqual, 1024bytes, 1024)
BENCH_REL(runMemcmp, 1024bytes, 1024)

BENCHMARK_DRAW_LINE();
BENCHMARK_DRAW_LINE();

BENCH_BASE(runBytesEqualRand, 8bytes, 8)
BENCH_REL(runMemcmpRand, 8bytes, 8)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqualRand, 16bytes, 16)
BENCH_REL(runMemcmpRand, 16bytes, 16)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqualRand, 32bytes, 32)
BENCH_REL(runMemcmpRand, 32bytes, 32)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqualRand, 64bytes, 64)
BENCH_REL(runMemcmpRand, 64bytes, 64)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqualRand, 128bytes, 128)
BENCH_REL(runMemcmpRand, 128bytes, 128)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqualRand, 256bytes, 256)
BENCH_REL(runMemcmpRand, 256bytes, 256)
BENCHMARK_DRAW_LINE();
BENCH_BASE(runBytesEqualRand, 1024bytes, 1024)
BENCH_REL(runMemcmpRand, 1024bytes, 1024)
} // namespace

} // namespace cachelib
} // namespace facebook

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
}
