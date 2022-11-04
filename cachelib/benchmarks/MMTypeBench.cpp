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

#include "cachelib/benchmarks/MMTypeBench.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <condition_variable>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "cachelib/allocator/MM2Q.h"
#include "cachelib/allocator/MMLru.h"
#include "cachelib/common/Mutex.h"

DEFINE_uint32(num_nodes, 10000, "Number of nodes to populate the list with");
DEFINE_uint32(num_threads,
              4,
              "Number of threads to be run concurrently. 0 means "
              "hardware_concurrency on the platform");
DEFINE_uint32(num_access,
              1000,
              "Number of operations to perform for 'recordAccess' benchmarks");
DEFINE_bool(print_stats, true, "Print the stats to stdout");

namespace facebook {
namespace cachelib {
namespace benchmarks {

enum BenchType : int {
  tAdd,
  tRemove,
  tRemoveIterator,
  tRecordAccessRead,
  tRecordAccessWrite,
};

template <typename MMType>
void runBench(typename MMType::Config config, BenchType benchType) {
  // mutex for our lru
  // Mutex mutex;
  std::vector<std::thread> threads;

  // thread local stats.
  // std::vector<Stats> stats(FLAGS_num_threads);

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

  // Create comman container and nodes for all threads.
  // Also add nodes to the container except for tAdd benchmark.
  std::unique_ptr<MMTypeBench<MMType>> myBench;
  BENCHMARK_SUSPEND {
    bool addNodesToContainer = (benchType != tAdd);
    myBench = std::make_unique<MMTypeBench<MMType>>();
    myBench->createContainer(config);
    myBench->createNodes(FLAGS_num_nodes, FLAGS_num_threads,
                         addNodesToContainer);
  }

  auto runInThread = [&](unsigned int index) {
    switch (benchType) {
    case tAdd:
      myBench->benchAdd(index);
      break;
    case tRemove:
      myBench->benchRemove(FLAGS_num_nodes, index);
      break;
    case tRemoveIterator:
      myBench->benchRemoveIterator(FLAGS_num_nodes);
      break;
    case tRecordAccessRead:
      myBench->benchRecordAccessRead(FLAGS_num_nodes, index, FLAGS_num_access);
      break;
    case tRecordAccessWrite:
      myBench->benchRecordAccessWrite(FLAGS_num_nodes, index, FLAGS_num_access);
      break;
    }

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

  BENCHMARK_SUSPEND {
    for (unsigned int i = 0; i < FLAGS_num_threads; i++) {
      threads.push_back(std::thread{runInThread, i});
    }
  }

  BENCHMARK_SUSPEND {
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
    myBench->deleteContainer();
    myBench.reset();
  }
}

BENCHMARK(MMLruAdd) { runBench<MMLru>(MMLru::Config{}, BenchType::tAdd); }

BENCHMARK_RELATIVE(MM2QAdd) { runBench<MM2Q>(MM2Q::Config{}, BenchType::tAdd); }

BENCHMARK(MMLruRemove) { runBench<MMLru>(MMLru::Config{}, BenchType::tRemove); }

BENCHMARK_RELATIVE(MM2QRemove) {
  runBench<MM2Q>(MM2Q::Config{}, BenchType::tRemove);
}

BENCHMARK(MMLruRemoveIterator) {
  runBench<MMLru>(MMLru::Config{}, BenchType::tRemoveIterator);
}

BENCHMARK_RELATIVE(MM2QRemoveIterator) {
  runBench<MM2Q>(MM2Q::Config{}, BenchType::tRemoveIterator);
}

BENCHMARK(MMLruRecordAccessRead) {
  runBench<MMLru>(MMLru::Config{}, BenchType::tRecordAccessRead);
}

BENCHMARK_RELATIVE(MM2QRecordAccessRead) {
  runBench<MM2Q>(MM2Q::Config{}, BenchType::tRecordAccessRead);
}

BENCHMARK(MMLruRecordAccessWriteUpdateNone) {
  MMLru::Config config{/* lruRefreshTime */ 0,
                       /* updateOnWrite */ false,
                       /* updateOnRead */ false,
                       /* tryLockUpdate */ false,
                       /* lruInsertionPointSpec */ 0};
  runBench<MMLru>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK_RELATIVE(MM2QRecordAccessWriteUpdateNone) {
  MM2Q::Config config{/* lruRefreshTime */ 0,
                      /* updateOnWrite */ false,
                      /* updateOnRead */ false,
                      /* tryLockUpdate */ false,
                      /* updateOnRecordAccess */ true,
                      /* hotSizePercent */ 30,
                      /* coldSizePercent */ 30};
  runBench<MM2Q>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK(MMLruRecordAccessWriteUpdateRd) {
  MMLru::Config config{/* lruRefreshTime */ 0,
                       /* updateOnWrite */ false,
                       /* updateOnRead */ true,
                       /* tryLockUpdate */ false,
                       /* lruInsertionPointSpec */ 0};
  runBench<MMLru>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK_RELATIVE(MM2QRecordAccessWriteUpdateRd) {
  MM2Q::Config config{/* lruRefreshTime */ 0,
                      /* updateOnWrite */ false,
                      /* updateOnRead */ true,
                      /* tryLockUpdate */ false,
                      /* updateOnRecordAccess */ true,
                      /* hotSizePercent */ 30,
                      /* coldSizePercent */ 30};
  runBench<MM2Q>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK(MMLruRecordAccessWriteUpdateWr) {
  MMLru::Config config{/* lruRefreshTime */ 0,
                       /* updateOnWrite */ true,
                       /* updateOnRead */ false,
                       /* tryLockUpdate */ false,
                       /* lruInsertionPointSpec */ 0};
  runBench<MMLru>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK_RELATIVE(MM2QRecordAccessWriteUpdateWr) {
  MM2Q::Config config{/* lruRefreshTime */ 0,
                      /* updateOnWrite */ true,
                      /* updateOnRead */ false,
                      /* tryLockUpdate */ false,
                      /* updateOnRecordAccess */ true,
                      /* hotSizePercent */ 30,
                      /* coldSizePercent */ 30};
  runBench<MM2Q>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK(MMLruRecordAccessWriteUpdateRdWr) {
  MMLru::Config config{/* lruRefreshTime */ 0,
                       /* updateOnWrite */ true,
                       /* updateOnRead */ true,
                       /* tryLockUpdate */ false,
                       /* lruInsertionPointSpec */ 0};
  runBench<MMLru>(config, BenchType::tRecordAccessWrite);
}

BENCHMARK_RELATIVE(MM2QRecordAccessWriteUpdateRdWr) {
  MM2Q::Config config{/* lruRefreshTime */ 0,
                      /* updateOnWrite */ true,
                      /* updateOnRead */ true,
                      /* tryLockUpdate */ false,
                      /* updateOnRecordAccess */ true,
                      /* hotSizePercent */ 30,
                      /* coldSizePercent */ 30};
  runBench<MM2Q>(config, BenchType::tRecordAccessWrite);
}
} // namespace benchmarks
} // namespace cachelib
} // namespace facebook

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  if (FLAGS_num_threads == 0) {
    FLAGS_num_threads = std::thread::hardware_concurrency();
    if (FLAGS_num_threads == 0) {
      FLAGS_num_threads = 32;
    }
  }

  if (FLAGS_num_threads >
      facebook::cachelib::benchmarks::kMMTypeBenchMaxThreads) {
    FLAGS_num_threads = facebook::cachelib::benchmarks::kMMTypeBenchMaxThreads;
  }

  if (FLAGS_print_stats) {
    std::cout << std::setw(15) << "num_threads :" << std::setw(15)
              << FLAGS_num_threads << std::endl;
    std::cout << std::setw(15) << "num_nodes :" << std::setw(15)
              << FLAGS_num_nodes << std::endl;
    std::cout << std::setw(15) << "num_access :" << std::setw(15)
              << FLAGS_num_access << std::endl;
  }
  folly::runBenchmarks();

  return 0;
}
