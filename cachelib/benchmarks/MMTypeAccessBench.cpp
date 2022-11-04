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
#include <folly/init/Init.h>

#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "cachelib/allocator/MMLru.h"
#include "cachelib/benchmarks/MMTypeBench.h"
#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {
namespace benchmarks {

uint32_t kNumThreads = 0;

template <typename MMType>
uint64_t runBench(typename MMType::Config config,
                  uint32_t numNodes,
                  uint32_t numAccess) {
  std::vector<std::thread> threads;

  // count of number of threads completed
  std::atomic<uint64_t> totalTime{0};

  // Create comman container and nodes for all threads.
  // Also add nodes to the container except for tAdd benchmark.
  std::unique_ptr<MMTypeBench<MMType>> myBench;
  myBench = std::make_unique<MMTypeBench<MMType>>();
  myBench->createContainer(config);
  myBench->createNodes(numNodes, kNumThreads, true);

  auto runInThread = [&](unsigned int index) {
    auto st = util::getCurrentTimeNs();
    myBench->benchRecordAccessReadSeq(index, numAccess);
    totalTime += (util::getCurrentTimeNs() - st);
  };

  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.push_back(std::thread{runInThread, i});
  }

  for (auto& thread : threads) {
    thread.join();
  }
  myBench->deleteContainer();
  myBench.reset();

  return totalTime;
}

uint64_t MMLruRecordAccessRead(uint32_t numNodes, uint32_t numAccess) {
  return runBench<MMLru>(MMLru::Config{}, numNodes, numAccess);
}

} // namespace benchmarks
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib::benchmarks;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  if (kNumThreads == 0) {
    kNumThreads = std::thread::hardware_concurrency();
    if (kNumThreads == 0) {
      kNumThreads = 32;
    }
  }

  if (kNumThreads > kMMTypeBenchMaxThreads) {
    kNumThreads = kMMTypeBenchMaxThreads;
  }

  std::cout << std::setw(15) << "num_threads :" << std::setw(15) << kNumThreads
            << std::endl;

  std::cout << "=============================================" << std::endl;
  std::cout << std::setw(9) << "naccess" << std::setw(8) << "nnodes"
            << std::setw(14) << "time(ns)" << std::endl;
  std::cout << "=============================================" << std::endl;
  for (uint32_t numAccess = 1000; numAccess <= 10 * 1000 * 1000;
       numAccess *= 10) {
    for (uint32_t numNodes = 10; numNodes <= 10 * 1000; numNodes *= 10) {
      uint64_t totalTime = 0;
      uint32_t run_count = 0;
      for (; run_count < 5; run_count++) {
        totalTime += MMLruRecordAccessRead(numNodes, numAccess);
      }
      std::cout << std::setw(9) << numAccess << std::setw(8) << numNodes
                << std::setw(14) << totalTime / run_count << std::endl;
    }
  }

  return 0;
}
