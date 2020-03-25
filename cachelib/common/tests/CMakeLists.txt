add_executable(
        cachelib_common_test_bin
        ApproxSplitSetTest.cpp
        BytesEqualTest.cpp
        CohortTests.cpp
        CountMinSketchTest.cpp
        CounterTests.cpp
        HashTests.cpp
        IteratorsTests.cpp
        MutexTests.cpp
        PeriodicWorkerTest.cpp
        SerializationTest.cpp
        UtilTests.cpp
)

target_link_libraries(
        cachelib_common_test_bin

        PUBLIC
            navy_common
            navy_bighash
            navy_serialization
            navy_scheduler
            navy_testing
            cachelib_allocator
            gtest_main
            gmock
)

add_test(
        NAME cachelib_common_tests
        COMMAND cacehlib_common_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
