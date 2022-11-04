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

#include <atomic>
#include <thread>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/FastStats.h"

/* Current results
============================================================================
cachelib/benchmarks/tl-bench/main.cpp           relative  time/iter  iters/s
============================================================================
atomic_inc_single                                          168.31ms     5.94
fast_stats_single                               5360.73%     3.14ms   318.50
----------------------------------------------------------------------------
fast_stats_many                                              3.23ms   309.83
fast_stats_many_repeated                          75.29%     4.29ms   233.28
tl_counter_many                                   74.01%     4.36ms   229.30
atomic_many                                        0.37%   870.86ms     1.15
============================================================================
*/

size_t nThreads = 64;
size_t nOps = 100000;

using facebook::cachelib::AtomicCounter;
using facebook::cachelib::TLCounter;
using facebook::cachelib::util::FastStats;

void runInThreads(size_t numThreads, const std::function<void()>& f) {
  std::vector<std::thread> threads;
  for (size_t i = 0; i < numThreads; i++) {
    threads.push_back(std::thread(f));
  }

  for (auto& t : threads) {
    t.join();
  }
}

BENCHMARK(atomic_inc_single) {
  AtomicCounter v{0};
  auto f = [&]() {
    for (size_t i = 0; i < nOps; i++) {
      v.inc();
    }
  };

  runInThreads(nThreads, f);
}

BENCHMARK_RELATIVE(fast_stats_single) {
  FastStats<uint64_t> v;
  auto f = [&]() {
    for (size_t i = 0; i < nOps; i++) {
      v.tlStats()++;
    }
  };
  runInThreads(nThreads, f);
}

BENCHMARK_DRAW_LINE();

namespace {
struct ManyFastStats {
  FastStats<uint64_t> a{};
  FastStats<uint64_t> b{};
  FastStats<uint64_t> c{};
  FastStats<uint64_t> d{};
};

struct ManyStats {
  uint64_t a{0};
  uint64_t b{0};
  uint64_t c{0};
  uint64_t d{0};

  ManyStats& operator+=(const ManyStats& o) {
    a += o.a;
    b += o.b;
    c += o.c;
    d += o.d;
    return *this;
  }
};
using FastManyStats = FastStats<ManyStats>;

struct ManyTLCounter {
  TLCounter a{};
  TLCounter b{};
  TLCounter c{};
  TLCounter d{};
};

struct ManyAtomicStats {
  AtomicCounter a{0};
  AtomicCounter b{0};
  AtomicCounter c{0};
  AtomicCounter d{0};
};
} // namespace

BENCHMARK(fast_stats_many) {
  FastManyStats v;
  auto f = [&]() {
    for (size_t i = 0; i < nOps; i++) {
      auto& l = v.tlStats();
      l.a++;
      l.b++;
      l.c++;
      l.d++;
    }
  };
  runInThreads(nThreads, f);
}

BENCHMARK_RELATIVE(fast_stats_many_repeated) {
  FastManyStats v;
  auto f = [&]() {
    for (size_t i = 0; i < nOps; i++) {
      v.tlStats().a++;
      v.tlStats().b++;
      v.tlStats().c++;
      v.tlStats().d++;
    }
  };
  runInThreads(nThreads, f);
}

BENCHMARK_RELATIVE(tl_counter_many) {
  ManyTLCounter v;
  auto f = [&]() {
    for (size_t i = 0; i < nOps; i++) {
      v.a.inc();
      v.b.inc();
      v.c.inc();
      v.d.inc();
    }
  };
  runInThreads(nThreads, f);
}

BENCHMARK_RELATIVE(atomic_many) {
  ManyAtomicStats v;
  auto f = [&]() {
    for (size_t i = 0; i < nOps; i++) {
      v.a.inc();
      v.b.inc();
      v.c.inc();
      v.d.inc();
    }
  };

  runInThreads(nThreads, f);
}

int main() { folly::runBenchmarks(); }
