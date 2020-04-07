cmake_minimum_required(VERSION 3.8 FATAL_ERROR)

# package information
set(PACKAGE_NAME      "cachelib")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_VERBOSE_MAKEFILE ON)

project(${PACKAGE_NAME} C CXX)

set(BIN_INSTALL_DIR bin CACHE STRING
    "The subdirectory where the compiler binary should be installed")
set(INCLUDE_INSTALL_DIR include CACHE STRING
    "The subdirectory where include files should be installed")
set(LIB_INSTALL_DIR lib CACHE STRING
    "The subdirectory where libraries should be installed")
set(CMAKE_INSTALL_DIR lib/cmake/${PACKAGE_NAME} CACHE STRING
    "The subdirectory where CMake package config files should be installed")

SET(CMAKE_CXX_FLAGS "-O2")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

#link_directories("/usr/local/include")

# includes
set(CMAKE_MODULE_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}/CMake"
  # for in-fbsource builds
  "${CMAKE_CURRENT_SOURCE_DIR}/../opensource/fbcode_builder/CMake"
  # For shipit-transformed builds
  "${CMAKE_CURRENT_SOURCE_DIR}/build/fbcode_builder/CMake"
  ${CMAKE_MODULE_PATH})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMake")

# Find required dependencies

find_package(fizz CONFIG REQUIRED)
find_package(wangle CONFIG REQUIRED)
find_package(LibEvent REQUIRED)
find_package(RT REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(Gflags REQUIRED)
find_package(yarpl CONFIG REQUIRED)
find_package(rsocket CONFIG REQUIRED)
find_package(FBThrift CONFIG REQUIRED)
set(FBTHRIFT_LIBRARIES FBThrift::thriftcpp2)
find_file(
    FBTHRIFT_LIBRARY_FILE
    NAMES "ThriftLibrary.cmake"
    HINTS
        "/usr/local/include/thrift"
)
include(${FBTHRIFT_LIBRARY_FILE})
find_file(
    THRIFT1
    NAMES "thrift1"
    HINTS
        "/usr/local/bin"
)
find_package(Boost REQUIRED)
find_package (Threads REQUIRED)
find_package (Libiberty REQUIRED)
find_package(Folly CONFIG REQUIRED)
set(FOLLY_LIBRARIES Folly::folly)
# Glog FBThrift ${CMAKE_DL_LIBS} Threads Gflags fmt)

#set(GTEST_ROOT "/usr/local/" CACHE PATH "Path to googletest")
enable_testing()
find_package(GTest REQUIRED)
find_package(Gmock REQUIRED)
find_package(glog REQUIRED)

# So that all subsequent directories have access to folly and thrift
include_directories(${FOLLY_INCLUDE_DIR} ${FBTHRIFT_INCLUDE_DIR} ${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS} ${fmt_INCLUDE_DIR})

# Download and unpack googletest at configure time
#configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
#execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
#        RESULT_VARIABLE result
#        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
#if(result)
#    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
#endif()
#execute_process(COMMAND ${CMAKE_COMMAND} --build .
#        RESULT_VARIABLE result
#        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
#if(result)
#    message(FATAL_ERROR "Build step for googletest failed: ${result}")
#endif()
#
## Prevent overriding the parent project's compiler/linker
## settings on Windows
#set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
#
## Add googletest directly to our build. This defines
## the gtest and gtest_main targets.
#add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
#        ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
#        EXCLUDE_FROM_ALL)


# Now simply link your own targets against gtest, gmock,
# etc. as appropriate

add_subdirectory(cachelib/shm)
add_subdirectory(cachelib/navy)
add_subdirectory(cachelib/common)
add_subdirectory(cachelib/allocator)
#add_subdirectory(cachelib/benchmarks)
add_subdirectory(cachelib/datatype)
add_subdirectory(cachelib/compact_cache)
add_subdirectory(cachelib/cachebench)
add_subdirectory(cachelib/external/memcached)
