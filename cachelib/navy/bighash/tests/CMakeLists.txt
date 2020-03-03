add_executable(
        bighash_test_bin

        BigHashTest.cpp
        BloomFilterTest.cpp
        BucketStorageTest.cpp
        BucketTest.cpp
)

target_link_libraries(
        bighash_test_bin

        ${FOLLY_LIBRARIES}
        common
        bighash
        gtest_main
)

add_test(
        NAME bighash_tests
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/bighash_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)