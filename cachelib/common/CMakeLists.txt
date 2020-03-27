add_library(
  cachelib_common
  ApproxSplitSet.h
  AtomicCounter.h
  BytesEqual.h
  Cohort.h
  CompilerUtils.h
  CountMinSketch.h
  Exceptions.h
  FastStats.h
  Hash.h
  Iterators.h
  MurmurHash.h
  Mutex.h
  PercentileStats.h
  PeriodicWorker.h
  Serialization.h
  TestUtils.h
  Throttler.h
  Time.h
  Utils.h
  Cohort.cpp
  CountMinSketch.cpp
  FurcHash.cpp
  PercentileStats.cpp
  PeriodicWorker.cpp
  Serialization.cpp
  TestUtils.cpp
  Utils.cpp

  hothash/HotHashDetector.h
  hothash/HotHashDetector.cpp
  hothash/HotHashDetectorTest.cpp

  piecewise/GenericPieces.h
  piecewise/RequestRange.h
  piecewise/GenericPieces.cpp
  piecewise/GenericPiecesTest.cpp
  piecewise/RequestRange.cpp
  piecewise/RequestRangeTest.cpp
)

target_link_libraries(
  cachelib_common
  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
        cachelib_common_test_bin

        tests/ApproxSplitSetTest.cpp
        tests/BytesEqualTest.cpp
        tests/CohortTests.cpp
        tests/CountMinSketchTest.cpp
        tests/CounterTests.cpp
        tests/HashTests.cpp
        tests/IteratorsTests.cpp
        tests/MutexTests.cpp
        tests/PeriodicWorkerTest.cpp
        tests/SerializationTest.cpp
        tests/UtilTests.cpp
)

target_link_libraries(
        cachelib_common_test_bin

        PUBLIC
            navy
            cachelib_allocator
            gtest_main
            gmock
)

add_test(
        NAME cachelib_common_tests
        COMMAND cachelib_common_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
