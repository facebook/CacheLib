#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121
thrift_object("shm" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
add_library(
  cachelib_shm

  PosixShmSegment.h
  Shm.h
  ShmCommon.h
  ShmManager.h
  SysVShmSegment.h
  PosixShmSegment.cpp
  ShmCommon.cpp
  ShmManager.cpp
  SysVShmSegment.cpp
  CMakeLists.txt
  $<TARGET_OBJECTS:shm-cpp2-obj> 
  )

target_link_libraries(
  cachelib_shm

  PUBLIC
    cachelib_common
    ${RT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
)

add_executable(
        cachelib_shm_test_bin

        tests/common.h
        tests/common.cpp
        tests/test_page_size.cpp
        tests/test_posix.cpp
        tests/test_shm.cpp
        tests/test_shm_manager.cpp
        tests/test_sysv.cpp
        )

target_link_libraries(
        cachelib_shm_test_bin

        PUBLIC
            navy
            cachelib_allocator
            cachelib_common
            gtest_main
            gmock
            ${FOLLY_LIBRARIES}
)

add_test(
        NAME cachelib_shm_tests
        COMMAND cachelib_shm_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
