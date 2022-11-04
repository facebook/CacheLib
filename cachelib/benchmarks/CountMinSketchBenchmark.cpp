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

// This benchmark was introduced when evaluating different implementations of
// cachelib's CountMinSketch.reset(). The data stored in CMS is in an array of
// unsigned integers. Three implementations were considered:
// 1. array decay: multiply every element of the array to 0.0 and narrow_cast.
// This was the implementation before the proposed change.
// 2. set: set every element of the array to 0. This was the proposed change.
// 3. memset: use memset to set all bytes to 0. This was not being implemented
// based on the results. CMS has three different types of unsigned int
// supported. The performance of each are below:
// ============================================================================
// [...]enchmarks/CountMinSketchBenchmark.cpp     relative  time/iter   iters/s
// ============================================================================
// cms32_decay                                                 34.80s    28.74m
// cms32_clear                                     244.25%     14.25s    70.19m
// cms32_memset                                    220.91%     15.75s    63.48m
// cms16_decay                                                 33.33s    30.01m
// cms16_clear                                     254.19%     13.11s    76.27m
// cms16_memset                                    424.47%      7.85s   127.37m
// cms8_decay                                                  33.51s    29.84m
// cms8_clear                                       283.4%     11.82s    84.57m
// cms8_memset                                     1179.6%      2.84s   352.02m
// In production, we are only using uint32_t and using "2. set" has the best
// performance. The code of memset (implementation and benchmark) is provided in
// comments below.

/*
Implementation of resetmemset:

  void CountMinSketch::resetmemset() {
    uint64_t tableSize = width_ * depth_;
    memset(table_.get(), 0, tableSize * sizeof(UINT));
  }

Benchmark code:

template <typename CMS>
void benchMemsetReset() {
  auto cms = createCMS<CMS>();
  for (int i = 0; i < FLAGS_num_ops; i++) {
    cms.resetmemset();
  }
  folly::doNotOptimizeAway(cms);
}

BENCHMARK_RELATIVE(cms8_memset) {
  facebook::cachelib::benchMemsetReset<
      facebook::cachelib::util::CountMinSketch8>();
}

BENCHMARK_RELATIVE(cms16_memset) {
  facebook::cachelib::benchMemsetReset<
      facebook::cachelib::util::CountMinSketch16>();
}
BENCHMARK_RELATIVE(cms32_memset) {
  facebook::cachelib::benchMemsetReset<
      facebook::cachelib::util::CountMinSketch>();
}
*/
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "cachelib/common/CountMinSketch.h"
DEFINE_int32(num_ops, 1000, "number of operations");
DEFINE_int32(max_width, 8 * 1000 * 1000, "max width of CMS");
DEFINE_int32(max_depth, 8, "depth of CMS");
DEFINE_double(max_err, 0.0000005, "max error probablity");
DEFINE_double(error_certainty, 0.99, "max certainty");

namespace facebook {
namespace cachelib {
template <typename CMS>
CMS createCMS() {
  return CMS(FLAGS_max_err, FLAGS_error_certainty, FLAGS_max_width,
             FLAGS_max_depth);
}

template <typename CMS>
void benchDecayReset() {
  auto cms = createCMS<CMS>();
  for (int i = 0; i < FLAGS_num_ops; i++) {
    cms.decayCountsBy(0);
  }
  folly::doNotOptimizeAway(cms);
}
template <typename CMS>
void benchClearReset() {
  auto cms = createCMS<CMS>();
  for (int i = 0; i < FLAGS_num_ops; i++) {
    cms.reset();
  }
  folly::doNotOptimizeAway(cms);
}

} // namespace cachelib
} // namespace facebook

BENCHMARK(cms32_decay) {
  facebook::cachelib::benchDecayReset<
      facebook::cachelib::util::CountMinSketch>();
}
BENCHMARK_RELATIVE(cms32_clear) {
  facebook::cachelib::benchClearReset<
      facebook::cachelib::util::CountMinSketch>();
}

BENCHMARK(cms16_decay) {
  facebook::cachelib::benchDecayReset<
      facebook::cachelib::util::CountMinSketch16>();
}
BENCHMARK_RELATIVE(cms16_clear) {
  facebook::cachelib::benchClearReset<
      facebook::cachelib::util::CountMinSketch16>();
}

BENCHMARK(cms8_decay) {
  facebook::cachelib::benchDecayReset<
      facebook::cachelib::util::CountMinSketch8>();
}
BENCHMARK_RELATIVE(cms8_clear) {
  facebook::cachelib::benchClearReset<
      facebook::cachelib::util::CountMinSketch8>();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
}
