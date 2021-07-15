add_thrift_file(DATATYPEBENCH DataTypeBench.thrift frozen2)

if (BUILD_TESTS)
  add_library (benchmark_test_support
    ${DATATYPEBENCH_THRIFT_FILES}
    )

  add_dependencies(benchmark_test_support thrift_generated_files)
  target_link_libraries (benchmark_test_support
    cachelib_datatype
    cachelib_allocator
    Folly::follybenchmark
    gmock
    glog
    gflags
    gtest
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
  add_test (CachelibSteadyTickerBench.cpp)
  add_test (CompactCacheBench.cpp)
  add_test (HashMapBenchmark.cpp)
  add_test (ItemsReaperBench.cpp)
  add_test (tl-bench/main.cpp)
  add_test (MMTypeAccessBench.cpp)
  add_test (MMTypeBench.cpp)
  add_test (MutexBench.cpp)
  add_test (PtrCompressionBench.cpp)
  add_test (SListBench.cpp)
  add_test (ThreadLocalBench.cpp)
  add_test (CacheAllocatorOpsMicroBench.cpp)
  add_test (EventTrackerPerf.cpp)
  add_test (SmallOperationMicroBench.cpp)
  add_test (SpeedUpExistenceCheckBenchmark.cpp)
  add_test (TempBenchmark.cpp)
endif()
