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

// Benchmark for understanding at which size of data binary search becomes
// slower than a hash table.
// Ran on devvm T1 2021/05/24
//
// ============================================================================
// cachelib/benchmarks/binarysearchvshashtablebench.cpprelative  time/iter
// iters/s
// ============================================================================
// bs_read_10                                                 115.33ms     8.67
// ht_read_10                                       681.12%    16.93ms    59.06
// ----------------------------------------------------------------------------
// bs_read_20                                                 124.97ms     8.00
// ht_read_20                                       520.54%    24.01ms    41.65
// ----------------------------------------------------------------------------
// bs_read_40                                                 134.02ms     7.46
// ht_read_40                                       332.31%    40.33ms    24.80
// ----------------------------------------------------------------------------
// bs_read_60                                                 141.86ms     7.05
// ht_read_60                                       280.15%    50.64ms    19.75
// ----------------------------------------------------------------------------
// bs_read_80                                                 140.64ms     7.11
// ht_read_80                                       297.40%    47.29ms    21.15
// ----------------------------------------------------------------------------
// bs_read_160                                                151.22ms     6.61
// ht_read_160                                      281.46%    53.73ms    18.61
// ----------------------------------------------------------------------------
// bs_read_320                                                162.09ms     6.17
// ht_read_320                                      181.32%    89.39ms    11.19
// ----------------------------------------------------------------------------
// bs_read_640                                                170.98ms     5.85
// ht_read_640                                      268.84%    63.60ms    15.72
// ----------------------------------------------------------------------------
// bs_read_1000                                               173.33ms     5.77
// ht_read_1000                                     264.62%    65.50ms    15.27
// ----------------------------------------------------------------------------
// bs_read_10000                                              213.46ms     4.68
// ht_read_10000                                    441.77%    48.32ms    20.70
// ----------------------------------------------------------------------------
// bs_write_10                                                197.31ms     5.07
// ht_write_10                                      207.09%    95.28ms    10.50
// ----------------------------------------------------------------------------
// bs_write_20                                                405.70ms     2.46
// ht_write_20                                      202.75%   200.10ms     5.00
// ----------------------------------------------------------------------------
// bs_write_40                                                893.37ms     1.12
// ht_write_40                                      199.03%   448.85ms     2.23
// ----------------------------------------------------------------------------
// bs_write_80                                                   1.99s  503.72m
// ht_write_80                                      235.97%   841.29ms     1.19
// ----------------------------------------------------------------------------
// bs_write_160                                               524.97ms     1.90
// ht_write_160                                     253.51%   207.08ms     4.83
// ----------------------------------------------------------------------------
// bs_write_320                                                  1.32s  757.59m
// ht_write_320                                     329.71%   400.34ms     2.50
// ----------------------------------------------------------------------------
// bs_write_640                                                  4.08s  245.08m
// ht_write_640                                     511.53%   797.67ms     1.25
// ----------------------------------------------------------------------------
// bs_write_1000                                              857.78ms     1.17
// ht_write_1000                                    641.85%   133.64ms     7.48
// ----------------------------------------------------------------------------
// bs_write_10000                                                6.85s  146.00m
// ht_write_10000                                  5591.45%   122.50ms     8.16
// ----------------------------------------------------------------------------
// ============================================================================

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <cstdint>

#include "cachelib/datatype/Map.h"

namespace facebook {
namespace cachelib {
template <size_t size, size_t iterations>
void bsRead() {
  using Pair = std::pair<uint32_t, detail::BufferAddr>;
  std::vector<Pair> input;
  std::vector<uint32_t> searchKeys;
  BENCHMARK_SUSPEND {
    input.resize(size);
    for (size_t i = 0; i < size; i++) {
      input.push_back(
          std::make_pair(i, detail::BufferAddr{0, static_cast<uint32_t>(i)}));
    }
    for (size_t curIter = 0; curIter < iterations; curIter++) {
      searchKeys.push_back(folly::Random::rand32(size));
    }
  }

  for (size_t curIter = 0; curIter < iterations; curIter++) {
    const uint32_t key = searchKeys[curIter];
    auto res = std::binary_search(
        input.begin(), input.end(),
        std::make_pair(key, detail::BufferAddr{0, key}),
        [](const auto& a, const auto& b) { return a.first < b.first; });
    folly::doNotOptimizeAway(res);
  }
}

template <size_t size, size_t iterations>
void htRead() {
  using HT = detail::HashTable<uint32_t>;
  std::unique_ptr<uint8_t[]> buffer;
  HT* ht = nullptr;
  std::vector<uint32_t> searchKeys;
  BENCHMARK_SUSPEND {
    const size_t htSize = static_cast<size_t>(size * 1.1);
    const size_t htStorageSize = HT::computeStorageSize(htSize);
    buffer = std::make_unique<uint8_t[]>(htStorageSize);
    ht = new (buffer.get()) HT(htSize);
    for (size_t i = 0; i < size; i++) {
      ht->insertOrReplace(i, detail::BufferAddr{0, static_cast<uint32_t>(i)});
    }
    for (size_t curIter = 0; curIter < iterations; curIter++) {
      searchKeys.push_back(folly::Random::rand32(size));
    }
  }

  for (size_t curIter = 0; curIter < iterations; curIter++) {
    const uint32_t key = searchKeys[curIter];
    auto res = ht->find(key);
    folly::doNotOptimizeAway(res);
  }
}

template <size_t size, size_t iterations>
void bsWrite() {
  std::vector<uint32_t> keys;
  BENCHMARK_SUSPEND {
    for (size_t i = 0; i < size; i++) {
      keys.push_back(folly::Random::rand32(size));
    }
  }

  auto fn = [&] {
    using Pair = std::pair<uint32_t, detail::BufferAddr>;
    std::vector<Pair> input;
    for (size_t i = 0; i < size; i++) {
      const uint32_t key = keys[i];
      auto entry = std::make_pair(key, detail::BufferAddr{0, key});
      auto itr = std::lower_bound(
          input.begin(), input.end(), entry,
          [](const auto& a, const auto& b) { return a.first < b.first; });
      if (itr != input.end() && itr->first == entry.first) {
        itr->second = entry.second;
      }
      input.insert(itr, entry);
    }
  };

  for (size_t i = 0; i < iterations; i++) {
    fn();
  }
}

template <size_t size, size_t iterations>
void htWrite() {
  std::vector<uint32_t> keys;
  BENCHMARK_SUSPEND {
    for (size_t i = 0; i < size; i++) {
      keys.push_back(folly::Random::rand32(size));
    }
  }

  auto fn = [&] {
    using HT = detail::HashTable<uint32_t>;
    size_t htSize = 2;
    auto buffer = std::make_unique<uint8_t[]>(HT::computeStorageSize(htSize));
    auto* ht = new (buffer.get()) HT(htSize);
    for (size_t i = 0; i < size; i++) {
      if (ht->overLimit()) {
        htSize *= 2;
        auto buffer2 =
            std::make_unique<uint8_t[]>(HT::computeStorageSize(htSize));
        auto* ht2 = new (buffer2.get()) HT(htSize, *ht);
        buffer.reset(buffer2.release());
        ht = ht2;
      }

      const uint32_t key = keys[i];
      ht->insertOrReplace(key, detail::BufferAddr{0, key});
    }
  };

  for (size_t i = 0; i < iterations; i++) {
    fn();
  }
}
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib;

#define BENCH_READ(size, iterations)                                 \
  BENCHMARK(bs_read_##size) { bsRead<size, iterations>(); }          \
  BENCHMARK_RELATIVE(ht_read_##size) { htRead<size, iterations>(); } \
  BENCHMARK_DRAW_LINE()

#define BENCH_WRITE(size, iterations)                                  \
  BENCHMARK(bs_write_##size) { bsWrite<size, iterations>(); }          \
  BENCHMARK_RELATIVE(ht_write_##size) { htWrite<size, iterations>(); } \
  BENCHMARK_DRAW_LINE()

BENCH_READ(10, 100000);
BENCH_READ(20, 100000);
BENCH_READ(40, 100000);
BENCH_READ(60, 100000);
BENCH_READ(80, 100000);
BENCH_READ(160, 100000);
BENCH_READ(320, 100000);
BENCH_READ(640, 100000);
BENCH_READ(1000, 100000);
BENCH_READ(10000, 100000);

BENCH_WRITE(10, 10000);
BENCH_WRITE(20, 10000);
BENCH_WRITE(40, 10000);
BENCH_WRITE(80, 10000);
BENCH_WRITE(160, 1000);
BENCH_WRITE(320, 1000);
BENCH_WRITE(640, 1000);
BENCH_WRITE(1000, 100);
BENCH_WRITE(10000, 10);

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
}
