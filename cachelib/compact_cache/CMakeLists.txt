add_executable(
        cachelib_cc_test_bin

        tests/CCacheTests.h
        tests/CCacheTests.cpp
        )

target_link_libraries(
        cachelib_cc_test_bin

        PUBLIC
            navy
            cachelib_allocator
            cachelib_common
            GTest::GTest
            GTest::Main
            ${GMOCK_LIBRARIES}
            ${FOLLY_LIBRARIES}
)

add_test(
        NAME cachelib_cc_tests
        COMMAND cachelib_cc_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
