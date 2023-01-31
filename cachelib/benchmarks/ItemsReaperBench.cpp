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
#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <iomanip>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/TestUtils.h"

using namespace facebook::cachelib;

DEFINE_uint64(num_keys, 1 << 20, "Number of keys to populate the cache with");
DEFINE_uint64(size_gb, 10, "Size of the cache in gb");
DEFINE_uint32(sleep_ms, 10, "reaper throttler sleep time");
DEFINE_uint32(work_ms, 5, "reaper throttler work interval");
DEFINE_uint32(print_interval_s,
              100,
              "interval at which to print reaper speed. 0 means disabled");
DEFINE_uint32(benchmark_duration_s, 300, "how long to run the benchmark");

namespace {

constexpr unsigned int kMaxKeySize = 255;
constexpr unsigned int kMinKeySize = 50;
constexpr unsigned int kMaxAllocSize = Slab::kSize - 500;
constexpr unsigned int kMinAllocSize = 100;
constexpr unsigned int kAllocSizeMean = 4096;
constexpr unsigned int kAllocSizeDev = 10 * 4096;

static uint32_t getRandomAllocSize(double val) {
  if (val < kMinAllocSize) {
    return kMinAllocSize;
  } else if (val > kMaxAllocSize) {
    return kMaxAllocSize;
  } else {
    return static_cast<uint32_t>(std::round(val));
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  XLOG(INFO) << "initializing cache";
  LruAllocator::AccessConfig accessConfig(
      folly::findLastSet(FLAGS_num_keys) + 1, 10);
  LruAllocator::Config lruConfig;
  lruConfig.setCacheSize(FLAGS_size_gb * 1024 * 1024 * 1024);
  lruConfig.setAccessConfig(accessConfig);
  lruConfig.enableItemReaperInBackground(
      std::chrono::milliseconds{FLAGS_sleep_ms},
      util::Throttler::Config{FLAGS_sleep_ms, FLAGS_work_ms});
  assert(lruConfig.itemsReaperEnabled());
  LruAllocator cache(lruConfig);
  const auto poolId =
      cache.addPool("default", cache.getCacheMemoryStats().ramCacheSize);

  XLOG(INFO) << "allocating items";
  std::mt19937 gen(folly::Random::rand32());
  std::normal_distribution<> sizeDis(kAllocSizeMean, kAllocSizeDev);
  std::uniform_int_distribution<uint32_t> keySize(kMinKeySize, kMaxKeySize);
  std::uniform_int_distribution<uint32_t> ttlDist(0,
                                                  FLAGS_benchmark_duration_s);
  std::uniform_int_distribution<uint8_t> charDis(0, 25);
  for (uint64_t i = 0; i < FLAGS_num_keys; ++i) {
    util::allocateAccessible(
        cache, poolId,
        facebook::cachelib::test_util::getRandomAsciiStr(keySize(gen)),
        getRandomAllocSize(sizeDis(gen)), ttlDist(gen));
  }

  auto reaperStatStr = [](const facebook::cachelib::ReaperStats& stats) {
    auto str = folly::sformat(
        "numTraversals: {:8d}, numVisits: {:12d}, lastTraversalMs: {:6d}ms, "
        "avgTraversalMs: {:6d}ms, maxTraversalMs: {:6d}",
        stats.numTraversals, stats.numVisitedItems, stats.lastTraversalTimeMs,
        stats.avgTraversalTimeMs, stats.maxTraversalTimeMs);
    return str;
  };

  struct rusage rUsageBefore = {};
  struct rusage rUsageAfter = {};

  XLOG(INFO) << "Gathering reaper metrics";
  if (::getrusage(RUSAGE_SELF, &rUsageBefore)) {
    XLOG(INFO) << "Error getting rusage" << errno;
    return 0;
  }
  const auto start = std::chrono::steady_clock::now();
  const auto startStats = cache.getReaperStats();
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (FLAGS_print_interval_s) {
      XLOG_EVERY_MS(INFO, FLAGS_print_interval_s * 1000)
          << reaperStatStr(cache.getReaperStats());
    }

    if (std::chrono::steady_clock::now() - start >
        std::chrono::seconds(FLAGS_benchmark_duration_s)) {
      break;
    }
  }

  if (::getrusage(RUSAGE_SELF, &rUsageAfter)) {
    XLOG(INFO) << "Error getting rusage" << errno;
    return 0;
  }

  auto finalStats = cache.getReaperStats();
  double memoryScanned =
      FLAGS_size_gb * (finalStats.numTraversals - startStats.numTraversals);
  auto timeTaken = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - start);

  XLOG(INFO) << reaperStatStr(finalStats);
  XLOG(INFO) << folly::sformat("bytes scanned  : {:12.2f}gb/sec",
                               memoryScanned / timeTaken.count());

  auto getTimeRUsage = [](const struct rusage& r) {
    double userSeconds = r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6;
    double sysSeconds = r.ru_stime.tv_sec + r.ru_stime.tv_usec / 1e6;
    return std::make_tuple(userSeconds, sysSeconds, userSeconds + sysSeconds);
  };

  auto [beforeUser, beforeSys, beforeTot] = getTimeRUsage(rUsageBefore);
  auto [afterUser, afterSys, afterTot] = getTimeRUsage(rUsageAfter);

  // compute core cpu util by divinding the time spent on a core with overall
  // time spent to complete the operation.
  XLOG(INFO) << folly::sformat(
      "cpu util: user: {:3.6}s sys: {:3.6f}s total: {:3.6f}s  util-pct: "
      "{:2.2f}%",
      afterUser - beforeUser, afterSys - beforeSys, afterTot - beforeTot,
      100.0 * (afterTot - beforeTot) / timeTaken.count());

  return 0;
}
