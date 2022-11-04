# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_thrift_file(DATATYPEBENCH DataTypeBench.thrift frozen2)

if (BUILD_TESTS)
  add_library (benchmark_test_support
    ${DATATYPEBENCH_THRIFT_FILES}
    )

  add_dependencies(benchmark_test_support thrift_generated_files)
  target_link_libraries (benchmark_test_support PUBLIC
    cachelib_datatype
    cachelib_allocator
    Folly::follybenchmark
    glog::glog
    gflags
    GTest::gtest
    GTest::gmock
  )

  function (ADD_TEST SOURCE_FILE)
     generic_add_test("benchmark-test" "${SOURCE_FILE}"
                      benchmark_test_support "${ARGN}")
  endfunction()

  add_test (BinarySearchVsHashTableBench.cpp)
  add_test (BucketMutexBench.cpp)
  add_test (BytesEqualBenchmark.cpp)
  add_test (CachelibMapOperationBench.cpp)
  add_test (CachelibMapWorkloadBench.cpp)
  add_test (CachelibRangeMapWorkloadBench.cpp)
  add_test (CachelibTickerClockBench.cpp)
  add_test (CompactCacheBench.cpp)
  add_test (HashMapBenchmark.cpp)
  add_test (ItemsReaperBench.cpp allocator_test_support)
  add_test (tl-bench/main.cpp)
  add_test (MMTypeAccessBench.cpp)
  add_test (MMTypeBench.cpp)
  add_test (MutexBench.cpp)
  add_test (PtrCompressionBench.cpp)
  add_test (SListBench.cpp)
  add_test (ThreadLocalBench.cpp)
  add_test (EventTrackerPerf.cpp)
  add_test (StrictAliasingSafeReadBench.cpp)
  # Temporarily disabled test: require __rdstc()
  #add_test (CacheAllocatorOpsMicroBench.cpp)
  #add_test (SmallOperationMicroBench.cpp)
  #add_test (SpeedUpExistenceCheckBenchmark.cpp)
endif()
