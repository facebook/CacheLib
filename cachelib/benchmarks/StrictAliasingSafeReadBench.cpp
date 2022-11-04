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

// clang-format off
//
// buck run @mode/opt  cachelib/benchmarks:strict_aliasing_safe_read_bench
//
// ============================================================================
// cachelib/benchmarks/StrictAliasingSafeReadBench.cpprelative  time/iter  iters/s
// ============================================================================
// unsafe_read32                                                 1.12s  895.27m
// strict_aliasing_safe_read32                      104.34%      1.07s  934.14m
// unsafe_read64                                                 1.25s  798.04m
// strict_aliasing_safe_read64                      105.52%      1.19s  842.09m
//
// clang-format on

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "cachelib/common/Utils.h"

std::vector<std::string> generateKeys(int numKeys, uint8_t keySize) {
  std::vector<std::string> keys;
  for (int i = 0; i < numKeys; i++) {
    std::string key;
    for (uint8_t j = 0; j < keySize; j++) {
      key.push_back(static_cast<char>(j));
    }
    keys.push_back(std::move(key));
  }
  return keys;
}

BENCHMARK(unsafe_read32) {
  std::vector<std::string> keys;
  BENCHMARK_SUSPEND { keys = generateKeys(10'000'000, 128); }
  for (auto& key : keys) {
    int numReads = key.size() / sizeof(uint32_t);
    const char* data = key.data();
    for (int i = 0; i < numReads; i++) {
      uint32_t val = *reinterpret_cast<const uint32_t*>(data);
      data += sizeof(uint32_t);
      folly::doNotOptimizeAway(val);
    }
  }
}

BENCHMARK_RELATIVE(strict_aliasing_safe_read32) {
  std::vector<std::string> keys;
  BENCHMARK_SUSPEND { keys = generateKeys(10'000'000, 128); }
  for (auto& key : keys) {
    int numReads = key.size() / sizeof(uint32_t);
    const char* data = key.data();
    for (int i = 0; i < numReads; i++) {
      uint32_t val =
          facebook::cachelib::util::strict_aliasing_safe_read32(data);
      data += sizeof(uint32_t);
      folly::doNotOptimizeAway(val);
    }
  }
}

BENCHMARK(unsafe_read64) {
  std::vector<std::string> keys;
  BENCHMARK_SUSPEND { keys = generateKeys(10'000'000, 128); }
  for (auto& key : keys) {
    int numReads = key.size() / sizeof(uint64_t);
    const char* data = key.data();
    for (int i = 0; i < numReads; i++) {
      uint64_t val = *reinterpret_cast<const uint64_t*>(data);
      data += sizeof(uint64_t);
      folly::doNotOptimizeAway(val);
    }
  }
}

BENCHMARK_RELATIVE(strict_aliasing_safe_read64) {
  std::vector<std::string> keys;
  BENCHMARK_SUSPEND { keys = generateKeys(10'000'000, 128); }
  for (auto& key : keys) {
    int numReads = key.size() / sizeof(uint64_t);
    const char* data = key.data();
    for (int i = 0; i < numReads; i++) {
      uint64_t val =
          facebook::cachelib::util::strict_aliasing_safe_read64(data);
      data += sizeof(uint64_t);
      folly::doNotOptimizeAway(val);
    }
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
}
