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
#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/SpinLock.h>
#include <folly/init/Init.h>
#include <folly/portability/Asm.h>
#include <gflags/gflags.h>
#include <sys/resource.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cachelib/common/Mutex.h"
#include "cachelib/common/Time.h"

// compares the locking performance for chained hashtable or any bucketed lock
// data structure where the critical section is small and we have an option of
// using many locks to shard the data structure. The comparison here is for
// choosing between a large number of mutex vs a smaller number of rw locks
// under different workloads
//
// Example result : https://phabricator.internmc.facebook.com/P59882384

using namespace facebook::cachelib;

DEFINE_uint64(sizepower, 16, "Array size in power of 2");
DEFINE_uint64(lockpower, 16, "num of locks in power of 2");
DEFINE_uint64(num_threads,
              0,
              "Number of threads to be run concurrently. 0 means "
              "hardware_concurrency on the platform");
DEFINE_double(hot_access_pct,
              0.0,
              "percentage of access that should go to a small set of hot keys");
DEFINE_uint64(
    num_hot_keys,
    1000,
    "number of keys that fall under hot access if hot access is enabled");
DEFINE_uint64(
    opspower,
    23,
    "Number of operations to be performed in each thread in power of 2");
DEFINE_uint64(spin_nano,
              0,
              "Number of nano secs to spin after each operation, to increase "
              "the critical section");
DEFINE_double(reads, 1.0, "Weight for acquisitions in shared mode");
DEFINE_double(writes, 0.0, "Weight for acquisitions in exclusive mode");
DEFINE_bool(print_stats, true, "Print the stats to stdout");

namespace {

struct LoadInfo {
 private:
  double totalWeight{};

 public:
  LoadInfo() {}
  LoadInfo(double read, double write)
      : totalWeight(read + write),
        readRatio(read / totalWeight),
        writeRatio(write / totalWeight) {}

  double readRatio{0.5};
  double writeRatio{0.2};
};

struct Stats {
  uint64_t numReads{0};
  uint64_t numWrites{0};
  uint64_t numInvCsw{0};
  uint64_t numVCsw{0};
  uint64_t elapsedNSecs{0};

  Stats& operator+=(const Stats& other) {
    numReads += other.numReads;
    numWrites += other.numWrites;
    elapsedNSecs = std::max(elapsedNSecs, other.elapsedNSecs);
    return *this;
  }

  double elapsedSecs() const noexcept {
    return static_cast<double>(elapsedNSecs) / 1e9;
  }

  void render() const {
    std::cout
        << folly::sformat("{:30}: {:16.4f}", "Elapsed time", elapsedSecs())
        << std::endl;

    std::cout
        << folly::sformat("{:30}: {:16,}", "Vol Context Switches", numVCsw)
        << std::endl;

    std::cout
        << folly::sformat("{:30}: {:16,}", "Inv Context Switches", numInvCsw)
        << std::endl;

    auto rate = [&](uint64_t num) {
      return static_cast<uint64_t>(num / elapsedSecs());
    };

    std::cout << folly::sformat("{:30}: {:16,}/sec", "Reads", rate(numReads))
              << std::endl;
    std::cout << folly::sformat("{:30}: {:16,}/sec", "Writes", rate(numWrites))
              << std::endl;

    const auto total = numReads + numWrites;

    std::cout << folly::sformat("{:30}: {:16,}/sec", "Total", rate(total))
              << std::endl;
  }
};

// since we always hash integer offsets, lets just use the integer
struct IntegerHash : public facebook::cachelib::Hash {
  uint32_t operator()(const void* buf, size_t) const noexcept override {
    return reinterpret_cast<size_t>(buf) % (1ULL << 32);
  }
  int getMagicId() const noexcept override { return 1; }
};

void spinIfNeeded() {
  if (FLAGS_spin_nano) {
    unsigned int spins = FLAGS_spin_nano / 40;
    while (--spins) {
      // this pause takes up to 40 clock cycles on intel and the lock
      // cmpxchgl
      // above should take about 100 clock cycles. we pause once every 400
      // cycles or so if we are extremely unlucky.
      folly::asm_volatile_pause();
    }
  }
}

template <typename Mutex>
class ArrayCounters {
 public:
  ArrayCounters(size_t sizePower, size_t lockPower)
      : nums_(1ULL << sizePower, 0),
        locks_(lockPower, std::make_shared<IntegerHash>()) {}

  void doRead(folly::ThreadLocalPRNG& rng, bool hot) {
    const auto pos = getSomePos(rng, hot);
    auto l = locks_.lockShared(pos);
    const auto val = nums_[pos];
    folly::doNotOptimizeAway(val);
    spinIfNeeded();
  }

  void doWrite(folly::ThreadLocalPRNG& rng, bool hot) {
    const auto pos = getSomePos(rng, hot);
    auto l = locks_.lockExclusive(pos);
    ++nums_[pos];
    spinIfNeeded();
  }

 private:
  size_t getSomePos(folly::ThreadLocalPRNG& rng, bool hot) {
    return folly::Random::rand32(
        0, hot ? FLAGS_num_hot_keys : nums_.size(), rng);
  }

  std::vector<uint64_t> nums_;
  Mutex locks_;
};

// global lru that every thread will access.
LoadInfo gLoadInfo{};

template <typename Mutex>
void runBench(Stats& global) {
  std::vector<std::thread> threads;
  ArrayCounters<Mutex> counters{FLAGS_sizepower, FLAGS_lockpower};

  // thread local stats.
  std::vector<Stats> stats(FLAGS_num_threads);

  // count of number of threads completed
  std::atomic<unsigned int> nCompleted{0};

  // main thread will wait on this to figure out when the benchmark is
  // complete
  std::mutex benchFinishMutex;
  bool benchFinished = false;
  std::condition_variable benchFinishCV;

  // all benchmark threads will wait on this after completing the benchmark
  // and before doing the thread cleanup.
  bool cleanUpStart = false;
  std::condition_variable cleanupCV;
  std::mutex cleanupMutex;

  auto runInThread = [&](unsigned int index) {
    auto rng = folly::ThreadLocalPRNG();
    std::mt19937 gen(folly::Random::rand32(rng));
    std::uniform_real_distribution<> opDis(0, 1);
    auto& s = stats[index];

    const auto now = util::getCurrentTimeNs();

    const size_t numOps = 1ULL << FLAGS_opspower;
    for (size_t i = 0; i < numOps; i++) {
      const auto r = opDis(gen);

      if (r < gLoadInfo.readRatio) {
        ++s.numReads;
        counters.doRead(rng, r < FLAGS_hot_access_pct);
      }

      if (r < gLoadInfo.writeRatio) {
        ++s.numWrites;
        counters.doWrite(rng, r < FLAGS_hot_access_pct);
      }
    }

    s.elapsedNSecs += (util::getCurrentTimeNs() - now);

    if (++nCompleted == FLAGS_num_threads) {
      {
        std::unique_lock<std::mutex> l(benchFinishMutex);
        benchFinished = true;
      }
      benchFinishCV.notify_one();
    }

    std::unique_lock<std::mutex> l(cleanupMutex);
    cleanupCV.wait(l, [&] { return cleanUpStart; });
  };

  struct rusage rUsageBefore = {};
  struct rusage rUsageAfter = {};
  BENCHMARK_SUSPEND {
    getrusage(RUSAGE_SELF, &rUsageBefore);
    for (size_t i = 0; i < FLAGS_num_threads; i++) {
      threads.push_back(std::thread{runInThread, i});
    }
  }

  {
    // wait for benchmark to finish.
    std::unique_lock<std::mutex> l(benchFinishMutex);
    benchFinishCV.wait(l, [&] { return benchFinished; });
  }

  BENCHMARK_SUSPEND {
    {
      std::unique_lock<std::mutex> l(cleanupMutex);
      cleanUpStart = true;
    }

    cleanupCV.notify_all();
    for (auto& thread : threads) {
      thread.join();
    }

    getrusage(RUSAGE_SELF, &rUsageAfter);
    for (auto& stat : stats) {
      global += stat;
    }
    global.numVCsw += rUsageAfter.ru_nvcsw - rUsageBefore.ru_nvcsw;
    global.numInvCsw += rUsageAfter.ru_nivcsw - rUsageBefore.ru_nivcsw;
  }
}

enum class MutexType : int { kSharedMutexBuckets, kMockSpinBuckets };

struct EnumHash {
  std::size_t operator()(MutexType t) const noexcept {
    return static_cast<size_t>(t);
  }
};

std::unordered_map<MutexType, std::pair<Stats, std::string>, EnumHash> stats = {
    {MutexType::kSharedMutexBuckets, {{}, "shared-mutex-buckets"}},
    {MutexType::kMockSpinBuckets, {{}, "folly-spin-buckets"}},
};

BENCHMARK(SharedMutexBuckets) {
  runBench<facebook::cachelib::SharedMutexBuckets>(
      stats[MutexType::kSharedMutexBuckets].first);
}

BENCHMARK_RELATIVE(MockSpinLockBuckets) {
  runBench<facebook::cachelib::SpinBuckets>(
      stats[MutexType::kMockSpinBuckets].first);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gLoadInfo = {FLAGS_reads, FLAGS_writes};

  if (FLAGS_num_threads == 0) {
    FLAGS_num_threads = std::thread::hardware_concurrency();
    if (FLAGS_num_threads == 0) {
      FLAGS_num_threads = 32;
    }
  }

  folly::runBenchmarks();

  if (FLAGS_print_stats) {
    auto printInt = [](folly::StringPiece key, size_t val) {
      std::cout << folly::sformat("{:20}: {:16,} ", key, val) << std::endl;
    };

    auto printDouble = [](folly::StringPiece key, double val) {
      std::cout << folly::sformat("{:20}: {:16.2f} ", key, val) << std::endl;
    };

    printInt("num_threads", FLAGS_num_threads);
    printInt("ops", 1ULL << FLAGS_opspower);
    printInt("size", 1ULL << FLAGS_sizepower);
    printInt("locks", 1ULL << FLAGS_lockpower);
    printInt("spin_nano", FLAGS_spin_nano);
    printDouble("ReadRatio", gLoadInfo.readRatio);
    printDouble("WriteRatio", gLoadInfo.writeRatio);

    for (auto& stat : stats) {
      std::cout << folly::sformat("===={:16}===", stat.second.second)
                << std::endl;
      stat.second.first.render();
      std::cout << std::endl;
    }
  }
  return 0;
}
