add_executable(
        navy_common_test_bin

        BufferTest.cpp
        DeviceTest.cpp
        HashTest.cpp
        UtilsTest.cpp

)

target_link_libraries(
        navy_common_test_bin

        PUBLIC
            navy_common
            navy_bighash
            navy_serialization
            navy_scheduler
            navy_testing
            cachelib_common
            gtest_main
            gmock
)

add_test(
        NAME common_tests
        COMMAND navy_common_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
