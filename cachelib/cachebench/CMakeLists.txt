add_library (cachelib_cachebench
  ./cache/Cache.cpp
  ./cache/TimeStampTicker.cpp
  ./consistency/LogEventStream.cpp
  ./consistency/ShortThreadId.cpp
  ./consistency/ValueHistory.cpp
  ./consistency/ValueTracker.cpp
  ./runner/FastShutdown.cpp
  ./runner/IntegrationStressor.cpp
  ./runner/ProgressTracker.cpp
  ./runner/Runner.cpp
  ./runner/Stressor.cpp
  ./util/CacheConfig.cpp
  ./util/Config.cpp
  ./util/NandWrites.cpp
  ./workload/PieceWiseCache.cpp
  ./workload/OnlineGenerator.cpp
  ./workload/WorkloadGenerator.cpp
  ./workload/PieceWiseReplayGenerator.cpp
  )
add_dependencies(cachelib_cachebench thrift_generated_files)
target_link_libraries(cachelib_cachebench
  cachelib_datatype
  cachelib_allocator
  gflags
)

add_executable (cachebench main.cpp)
target_link_libraries(cachebench cachelib_cachebench)

add_executable (cachebench-util util/main.cpp)
target_link_libraries(cachebench-util cachelib_cachebench)

install(
  TARGETS
     cachebench
     cachebench-util
  DESTINATION ${BIN_INSTALL_DIR}
)


if (BUILD_TESTS)
  add_library (cachebench_test_support INTERFACE )
  target_link_libraries (cachebench_test_support INTERFACE
    cachelib_cachebench
    gmock
    glog
    gflags
    gtest
    gtest_main
  )


  function (ADD_TEST SOURCE_FILE)
     generic_add_test("cachebench-test" "${SOURCE_FILE}"
                      cachebench_test_support "${ARGN}")
  endfunction()

  add_test (workload/tests/WorkloadGeneratorTest.cpp)
  add_test (workload/tests/PieceWiseCacheTest.cpp)
  add_test (consistency/tests/RingBufferTest.cpp)
  add_test (consistency/tests/ShortThreadIdTest.cpp)
  add_test (consistency/tests/ValueHistoryTest.cpp)
  add_test (consistency/tests/ValueTrackerTest.cpp)
  add_test (util/tests/NandWritesTest.cpp)
  add_test (cache/tests/TimeStampTickerTest.cpp)
endif()
