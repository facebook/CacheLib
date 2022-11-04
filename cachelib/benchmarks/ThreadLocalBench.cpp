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

/*
============================================================================
cachelib/benchmarks/ThreadLocalBench.cpp        relative  time/iter  iters/s
============================================================================
VanillaAtomicStats                                            5.48s  182.34m
TestAtomicCounter                                102.18%      5.37s  186.31m
VanillaThreadLocalStats                         8392.73%    65.35ms    15.30
TestTLCounter                                   6814.13%    80.48ms    12.42
============================================================================
*/
#include <folly/Benchmark.h>
#include <folly/ThreadLocal.h>
#include <folly/init/Init.h>

#include <thread>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/FastStats.h"

DEFINE_uint64(num_threads, 32, "Number of threads to be run concurrently");
DEFINE_uint64(num_ops, 1e7, "Number of operations to be run per thread");

using namespace facebook::cachelib;

namespace {
std::atomic<uint64_t> val;
AtomicCounter atomicCounter;
class DummyTag;
folly::ThreadLocal<uint64_t, DummyTag> tlVal;
TLCounter tlCounter;
} // namespace

BENCHMARK(VanillaAtomicStats) {
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < FLAGS_num_threads; i++) {
    // bind captures the variable by copy.
    threads.push_back(std::thread([&]() {
      for (uint64_t j = 0; j < FLAGS_num_ops; j++) {
        val.fetch_add(1, std::memory_order_relaxed);
      }
    }));
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

BENCHMARK_RELATIVE(TestAtomicCounter) {
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < FLAGS_num_threads; i++) {
    // bind captures the variable by copy.
    threads.push_back(std::thread([&]() {
      for (uint64_t j = 0; j < FLAGS_num_ops; j++) {
        atomicCounter.inc();
      }
    }));
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

BENCHMARK_RELATIVE(VanillaThreadLocalStats) {
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < FLAGS_num_threads; i++) {
    // bind captures the variable by copy.
    threads.push_back(std::thread([&]() {
      for (uint64_t j = 0; j < FLAGS_num_ops; j++) {
        ++(*tlVal);
      }
    }));
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

BENCHMARK_RELATIVE(TestTLCounter) {
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < FLAGS_num_threads; i++) {
    // bind captures the variable by copy.
    threads.push_back(std::thread([&]() {
      for (uint64_t j = 0; j < FLAGS_num_ops; j++) {
        tlCounter.inc();
      }
    }));
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
