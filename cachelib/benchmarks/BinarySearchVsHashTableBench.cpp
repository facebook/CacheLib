// Benchmark for understanding at which size of data binary search becomes
// slower than a hash table.
//
// ============================================================================
// bs_read_10                                                  17.80ms    56.19
// ht_read_10                                        80.08%    22.22ms    45.00
// ----------------------------------------------------------------------------
// bs_read_20                                                  23.13ms    43.24
// ht_read_20                                        84.39%    27.40ms    36.49
// ----------------------------------------------------------------------------
// bs_read_40                                                  29.59ms    33.80
// ht_read_40                                        76.32%    38.77ms    25.79
// ----------------------------------------------------------------------------
// bs_read_60                                                  32.14ms    31.11
// ht_read_60                                        70.32%    45.71ms    21.88
// ----------------------------------------------------------------------------
// bs_read_80                                                  35.40ms    28.25
// ht_read_80                                        78.69%    44.98ms    22.23
// ----------------------------------------------------------------------------
// bs_read_160                                                 42.57ms    23.49
// ht_read_160                                       87.07%    48.89ms    20.45
// ----------------------------------------------------------------------------
// bs_read_320                                                 48.37ms    20.67
// ht_read_320                                       68.57%    70.54ms    14.18
// ----------------------------------------------------------------------------
// bs_read_640                                                 55.00ms    18.18
// ht_read_640                                       98.06%    56.09ms    17.83
// ----------------------------------------------------------------------------
// bs_read_1000                                                57.88ms    17.28
// ht_read_1000                                     104.54%    55.36ms    18.06
// ----------------------------------------------------------------------------
// bs_read_10000                                               77.05ms    12.98
// ht_read_10000                                    174.07%    44.26ms    22.59
// ----------------------------------------------------------------------------
// bs_write_10                                                 15.08ms    66.32
// ht_write_10                                       67.94%    22.19ms    45.06
// ----------------------------------------------------------------------------
// bs_write_20                                                 27.34ms    36.58
// ht_write_20                                       54.70%    49.98ms    20.01
// ----------------------------------------------------------------------------
// bs_write_40                                                 56.87ms    17.59
// ht_write_40                                       47.10%   120.74ms     8.28
// ----------------------------------------------------------------------------
// bs_write_80                                                138.45ms     7.22
// ht_write_80                                       57.77%   239.63ms     4.17
// ----------------------------------------------------------------------------
// bs_write_160                                                52.29ms    19.13
// ht_write_160                                      94.92%    55.09ms    18.15
// ----------------------------------------------------------------------------
// bs_write_320                                               218.25ms     4.58
// ht_write_320                                     163.38%   133.58ms     7.49
// ----------------------------------------------------------------------------
// bs_write_640                                               716.00ms     1.40
// ht_write_640                                     178.21%   401.76ms     2.49
// ----------------------------------------------------------------------------
// bs_write_1000                                              154.00ms     6.49
// ht_write_1000                                    231.55%    66.51ms    15.04
// ----------------------------------------------------------------------------
// bs_write_10000                                               21.37s   46.80m
// ht_write_10000                                  6984.40%   305.92ms     3.27
// ----------------------------------------------------------------------------
// ============================================================================

#include <algorithm>
#include <cstdint>

#include <folly/Benchmark.h>
#include <folly/Random.h>

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
    const size_t htSize = size * 1.1;
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
    for (size_t i = 0; i < iterations; i++) {
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
    for (size_t i = 0; i < iterations; i++) {
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

BENCH_READ(10, 1000000);
BENCH_READ(20, 1000000);
BENCH_READ(40, 1000000);
BENCH_READ(60, 1000000);
BENCH_READ(80, 1000000);
BENCH_READ(160, 1000000);
BENCH_READ(320, 1000000);
BENCH_READ(640, 1000000);
BENCH_READ(1000, 1000000);
BENCH_READ(10000, 1000000);

BENCH_WRITE(10, 100000);
BENCH_WRITE(20, 100000);
BENCH_WRITE(40, 100000);
BENCH_WRITE(80, 100000);
BENCH_WRITE(160, 10000);
BENCH_WRITE(320, 10000);
BENCH_WRITE(640, 10000);
BENCH_WRITE(1000, 1000);
BENCH_WRITE(10000, 1000);

int main() { folly::runBenchmarks(); }
