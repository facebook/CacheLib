add_library(
  cachelib_cachebench

  cache/Cache-inl.h
  cache/Cache.h
  cache/CacheStats.h

  consistency/LogEventStream.h
  consistency/RingBuffer.h
  consistency/ShortThreadId.h
  consistency/ValueHistory.h
  consistency/ValueTracker.h
  consistency/LogEventStream.cpp
  consistency/ShortThreadId.cpp
  consistency/ValueHistory.cpp
  consistency/ValueTracker.cpp

  runner/CacheStressor.h
  runner/FastShutdown.h
  runner/IntegrationStressor.h
  runner/OutputStressor.h
  runner/ProgressTracker.h
  runner/Runner.h
  runner/Stressor.h
  runner/TestStopper.h
  runner/FastShutdown.cpp
  runner/IntegrationStressor.cpp
  runner/ProgressTracker.cpp
  runner/Runner.cpp
  runner/Stressor.cpp
  runner/TestStopper.cpp

  util/CacheConfig.h
  util/Config.h
  util/Exceptions.h
  util/JSONConfig.h
  util/NandWrites.h
  util/Parallel.h
  util/Parallel.cpp
  util/Request.h
  util/CacheConfig.cpp
  util/Config.cpp
  util/NandWrites.cpp


  workload/GeneratorBase.h
  workload/OnlineGenerator-inl.h
  workload/OnlineGenerator.h
  workload/PieceWiseReplayGenerator-inl.h
  workload/PieceWiseReplayGenerator.h
  workload/ReplayGenerator.h
  workload/ReplayGeneratorBase.h
  workload/WorkloadGenerator-inl.h
  workload/WorkloadGenerator.h

  workload/distributions/DiscreteDistribution.h
  workload/distributions/FastDiscrete.h
  workload/distributions/NormalDistribution.h
  workload/distributions/RangeDistribution.h

  )

add_executable(
nand_writes_util
      
  util/main.cpp
  util/NandWrites.cpp
  util/NandWrites.h

)

target_link_libraries(
  cachelib_cachebench
  PUBLIC
    cachelib_shm
    cachelib_common
    navy
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
    memcached
)
target_link_libraries(
  nand_writes_util
  PUBLIC
    ${FOLLY_LIBRARIES}
)

add_executable(
        cachebench_test_bin

        consistency/tests/RingBufferTest.cpp
        consistency/tests/ShortThreadIdTest.cpp
        consistency/tests/ValueHistoryTest.cpp
        consistency/tests/ValueTrackerTest.cpp

        workload/tests/WorkloadGeneratorTest.cpp
    )

target_link_libraries(
        cachebench_test_bin

        PUBLIC
            cachelib_cachebench
            cachelib_datatype
            navy
            cachelib_allocator
            cachelib_common
            gtest_main
            gmock
            ${FOLLY_LIBRARIES}
)

add_test(
        NAME cachebench_tests
        COMMAND cachebench_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(
    cachebench_bin

    main.cpp
    )

target_link_libraries(
    cachebench_bin
    
    cachelib_cachebench
    cachelib_common
    cachelib_datatype
    ${FOLLY_LIBRARIES}
    )
