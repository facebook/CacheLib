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

#include "cachelib/allocator/datastruct/DList.h"
#include "cachelib/common/Mutex.h"
#include "cachelib/common/Time.h"

using namespace facebook::cachelib;

DEFINE_uint64(size, 1 << 16, "Number of nodes to populate the list with");
DEFINE_uint64(num_threads,
              0,
              "Number of threads to be run concurrently. 0 means "
              "hardware_concurrency on the platform");
DEFINE_uint64(num_thread_ops,
              1 << 19,
              "Number of operations to be performed in each thread");
DEFINE_uint64(sleep_nano,
              0,
              "Number of nano seconds to sleep between each operation");
DEFINE_uint64(spin_nano,
              100,
              "Number of nano secs to spin after each operation");
DEFINE_double(updates, 0.9, "Weight for lru updates");
DEFINE_double(evicts, 0.5, "Weight for lru evictions");
DEFINE_double(deletes, 0.01, "Weight for deletes");
DEFINE_uint64(max_search_len, 50, "Maximum search depth for evicts");
DEFINE_bool(print_stats, true, "Print the stats to stdout");

namespace {

struct LoadInfo {
 private:
  double totalWeight{};

 public:
  LoadInfo() {}
  LoadInfo(double update, double evict, double dels)
      : totalWeight(update + evict + dels),
        updateRatio(update / totalWeight),
        deleteRatio(dels / totalWeight),
        evictRatio(evict / totalWeight) {}

  double updateRatio{0.5};
  double deleteRatio{0.2};
  double evictRatio{0.1};
};

struct Stats {
  uint64_t numUpdates{0};
  uint64_t numDeletes{0};
  uint64_t numEvicts{0};
  uint64_t numInvCsw{0};
  uint64_t numVCsw{0};
  uint64_t elapsedNSecs{0};

  Stats& operator+=(const Stats& other) {
    numUpdates += other.numUpdates;
    numDeletes += other.numDeletes;
    numEvicts += other.numEvicts;
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

    std::cout
        << folly::sformat("{:30}: {:16,}/sec", "Updates", rate(numUpdates))
        << std::endl;

    std::cout
        << folly::sformat("{:30}: {:16,}/sec", "Deletes", rate(numDeletes))
        << std::endl;

    std::cout
        << folly::sformat("{:30}: {:16,}/sec", "Evictions", rate(numEvicts))
        << std::endl;

    const auto total = numUpdates + numDeletes + numEvicts;
    std::cout << folly::sformat("{:30}: {:16,}/sec", "Throughput", rate(total))
              << std::endl;
  }
};

class Lru;

// our own implementation of node that will be put inside the container.
struct Node {
 public:
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  Node(Node&&) = default;
  Node& operator=(Node&&) = default;

  // Node does not perform pointer compression, but it needs to supply a dummy
  // PtrCompressor
  using CompressedPtr = Node*;
  struct PtrCompressor {
    constexpr CompressedPtr compress(Node* uncompressed) const noexcept {
      return uncompressed;
    }

    constexpr Node* unCompress(CompressedPtr compressed) const noexcept {
      return compressed;
    }
  };

  explicit Node() noexcept {
    XDCHECK(hook_.getNext(PtrCompressor()) == nullptr);
    XDCHECK(hook_.getPrev(PtrCompressor()) == nullptr);
  }

 protected:
  bool isInMMContainer() const noexcept { return inContainer_; }
  void markInMMContainer() noexcept { inContainer_ = true; }
  void unmarkInMMContainer() noexcept { inContainer_ = false; }
  void setUpdateTime(uint32_t time) noexcept { hook_.setUpdateTime(time); }
  uint32_t getUpdateTime() const noexcept { return hook_.getUpdateTime(); }

 public:
  DListHook<Node> hook_{};

 private:
  bool inContainer_{false};
  friend Lru;
};

class Lru {
 public:
  explicit Lru(unsigned int numNodes)
      : numNodes_(numNodes), lru_{Node::PtrCompressor{}} {
    for (size_t i = 0; i < numNodes_; i++) {
      nodes_.emplace_back(Node{});
    }

    // add all the nodes once the vector is sized appropriately.
    for (auto& node : nodes_) {
      add(node, 0);
    }

    XDCHECK_EQ(nodes_.size(), numNodes_);
  }

  unsigned int getRandomNodeIdx(folly::ThreadLocalPRNG& rng) const noexcept {
    return folly::Random::rand32(0, numNodes_, rng);
  }

  void doUpdate(unsigned int nodeIdx, uint32_t time) {
    XDCHECK_LT(nodeIdx, numNodes_);
    XDCHECK(!nodes_.empty());
    auto& node = nodes_[nodeIdx];
    lru_.moveToHead(node);
    node.setUpdateTime(time);
  }

  void doEvict(unsigned int iterationLength, uint32_t time) {
    unsigned int i = 0;
    for (auto it = lru_.rbegin(); it != lru_.rend(); ++it) {
      auto& node = *it;
      if (++i == iterationLength) {
        remove(node);
        add(node, time);
        break;
      }
    }
  }

 private:
  void add(Node& node, uint32_t time) {
    lru_.linkAtHead(node);
    node.markInMMContainer();
    node.setUpdateTime(time);
  }

  void remove(Node& node) {
    lru_.remove(node);
    node.unmarkInMMContainer();
  }

  using LruImpl = DList<Node, &Node::hook_>;

  const unsigned int numNodes_;
  std::vector<Node> nodes_;
  LruImpl lru_;
};

// global lru that every thread will access.
std::unique_ptr<Lru> gLru;
LoadInfo gLoadInfo{};

template <typename Mutex>
void runBench(Stats& global) {
  // mutex for our lru
  Mutex mutex;
  std::vector<std::thread> threads;

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

    using LockHolder = std::unique_lock<Mutex>;
    for (size_t i = 0; i < FLAGS_num_thread_ops; i++) {
      const auto r = opDis(gen);
      const uint32_t currTime = util::getCurrentTimeSec();

      if (r < gLoadInfo.updateRatio) {
        const auto idx = gLru->getRandomNodeIdx(rng);
        ++s.numUpdates;
        LockHolder l(mutex);
        gLru->doUpdate(idx, currTime);
      }

      if (r < gLoadInfo.evictRatio) {
        const auto searchLen = folly::Random::rand32(FLAGS_max_search_len, rng);
        ++s.numEvicts;
        LockHolder l(mutex);
        gLru->doEvict(searchLen, currTime);
      }

      if (r < gLoadInfo.deleteRatio) {
        ++s.numEvicts;
        LockHolder l(mutex);
        // for now, deletes also do evicts
        gLru->doEvict(1, currTime);
      }

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

      if (FLAGS_sleep_nano) {
        /* sleep override */ std::this_thread::sleep_for(
            std::chrono::nanoseconds(FLAGS_sleep_nano));
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

enum class MutexType : int {
  kStdMutex,
  kFollySpin,
  kPThreadMutex,
  kPThreadAdaptiveMutex,
  kPThreadSpinMutex,
  kPThreadSpinLock
};

struct EnumHash {
  std::size_t operator()(MutexType t) const noexcept {
    return static_cast<size_t>(t);
  }
};

std::unordered_map<MutexType, std::pair<Stats, std::string>, EnumHash> stats = {
    {MutexType::kStdMutex, {{}, "std::mutex"}},
    {MutexType::kFollySpin, {{}, "folly SpinLock"}},
    {MutexType::kPThreadMutex, {{}, "PThreadMutex"}},
    {MutexType::kPThreadAdaptiveMutex, {{}, "PThreadAdaptiveMutex"}},
    {MutexType::kPThreadSpinMutex, {{}, "PThreadSpinMutex"}},
    {MutexType::kPThreadSpinLock, {{}, "PThreadSpinLock"}},
};

BENCHMARK(StandardMutex) {
  runBench<std::mutex>(stats[MutexType::kStdMutex].first);
}

BENCHMARK_RELATIVE(SpinLock) {
  runBench<folly::SpinLock>(stats[MutexType::kFollySpin].first);
}

BENCHMARK_RELATIVE(PThreadMutex) {
  runBench<facebook::cachelib::PThreadMutex>(
      stats[MutexType::kPThreadMutex].first);
}

BENCHMARK_RELATIVE(PThreadAdaptiveMutex) {
  runBench<facebook::cachelib::PThreadAdaptiveMutex>(
      stats[MutexType::kPThreadAdaptiveMutex].first);
}

BENCHMARK_RELATIVE(PThreadSpinMutex) {
  runBench<facebook::cachelib::PThreadSpinMutex>(
      stats[MutexType::kPThreadSpinMutex].first);
}

BENCHMARK_RELATIVE(PThreadSpinLock) {
  runBench<facebook::cachelib::PThreadSpinLock>(
      stats[MutexType::kPThreadSpinLock].first);
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gLru = std::make_unique<Lru>(FLAGS_size);
  gLoadInfo = {FLAGS_updates, FLAGS_evicts, FLAGS_deletes};

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
    printInt("num_thread_ops", FLAGS_num_thread_ops);
    printInt("size", FLAGS_size);
    printInt("sleep_nano", FLAGS_sleep_nano);
    printInt("spin_nano", FLAGS_spin_nano);
    printDouble("evict ratio", gLoadInfo.evictRatio);
    printDouble("delete ratio", gLoadInfo.deleteRatio);
    printDouble("update ratio", gLoadInfo.updateRatio);
    std::cout << std::endl;

    for (auto& stat : stats) {
      std::cout << folly::sformat("==={:16}===", stat.second.second)
                << std::endl;
      stat.second.first.render();
      std::cout << std::endl;
    }
  }
  return 0;
}
