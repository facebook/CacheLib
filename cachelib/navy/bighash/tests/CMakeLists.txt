add_executable(
        bighash_test_bin

        BigHashTest.cpp
        BloomFilterTest.cpp
        BucketStorageTest.cpp
        BucketTest.cpp
)

target_link_libraries(
        bighash_test_bin

        PUBLIC
            navy_common
            navy_bighash
            navy_serialization
            navy_scheduler
            navy_testing
            gtest
            gmock
)

add_test(
        NAME bighash_tests
        COMMAND bighash_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
