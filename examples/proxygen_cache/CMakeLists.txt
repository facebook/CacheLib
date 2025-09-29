cmake_minimum_required(VERSION 3.20)
project(proxygen_cache CXX)

# ---- C++ standard ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Prefer Config packages; avoid registry noise
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)
set(CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY ON)

set(PROXYGEN_CACHE_HOME ${CMAKE_CURRENT_SOURCE_DIR} CACHE PATH "Path to the proxygen_cache source directory")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROXYGEN_CACHE_HOME}/cmake")

# Toggle QUIC deps (default OFF)
option(USE_QUIC "Enable QUIC (requires fizz, mvfst, liboqs, c-ares)" OFF)

# ---- Core deps ----
find_package(folly CONFIG REQUIRED PATHS "/home/byrnedan/CacheLib/opt/folly/lib/cmake/folly")
# Wangle may export 'wangle' or 'wangle::wangle'
find_package(wangle CONFIG QUIET)
if(NOT wangle_FOUND)
  find_package(Wangle CONFIG REQUIRED)
endif()

# Your install exports lowercase 'proxygen-config.cmake'
find_package(proxygen CONFIG REQUIRED PATHS "/home/byrnedan/CacheLib/opt/proxygen/lib/cmake/proxygen")       # defines proxygenlib, proxygenhttpserver
#find_package(fbthrift CONFIG REQUIRED)      # fbthrift::thrift (some exports define 'thrift')
# Cachelib (prefer config target; fallback later if needed)
find_package(cachelib CONFIG QUIET PATHS "/home/byrnedan/CacheLib/opt/cachelib/lib/cmake/cachelib")

find_package(glog   CONFIG REQUIRED)         # glog::glog
find_package(gflags CONFIG QUIET)            # gflags::gflags (optional)
find_package(GTest  CONFIG REQUIRED)         # GTest::gtest, GTest::gmock
find_package(NUMA   REQUIRED)         # NUMA::NUMA

# ---- QUIC deps (only if enabled) ----
if(USE_QUIC)
  find_package(fizz  CONFIG REQUIRED)        # fizz::fizz
  find_package(mvfst CONFIG REQUIRED)        # mvfst::mvfst
  # liboqs and c-ares are pulled by fizz/mvfst/proxygen configs; pass *_DIR at configure time
endif()

# ---- Target ----
add_executable(proxygen_cache src/main.cpp)

# Normalize target names across exports
set(FOLLY_TGT folly::folly)
if(NOT TARGET ${FOLLY_TGT} AND TARGET folly)
  set(FOLLY_TGT folly)
endif()

set(WANGLE_TGT wangle)
if(NOT TARGET ${WANGLE_TGT} AND TARGET wangle::wangle)
  set(WANGLE_TGT wangle::wangle)
endif()

set(CACHELIB_TGT cachelib::cachelib)
if(NOT TARGET ${CACHELIB_TGT} AND TARGET cachelib)
  set(CACHELIB_TGT cachelib)
endif()

# Link
target_link_libraries(proxygen_cache PRIVATE
  Folly::folly
  ${WANGLE_TGT}
  proxygen::proxygen
  proxygen::proxygenhttpserver
  #glog::glog
  $<$<TARGET_EXISTS:gflags::gflags>:gflags::gflags>
  #${CACHELIB_TGT}
  #cachelib::cachelib
  cachelib
  GTest::gmock
  NUMA::NUMA
  $<$<BOOL:${USE_QUIC}>:fizz::fizz>
  $<$<BOOL:${USE_QUIC}>:mvfst::mvfst>
)

target_include_directories(proxygen_cache PUBLIC
    /home/byrnedan/CacheLib/opt/cachelib/include
)

# Ensure -std=c++20 ends up last in case a dep leaks -std=gnu++17
target_compile_options(proxygen_cache PUBLIC "$<$<COMPILE_LANGUAGE:CXX>:-std=c++20>")

# Diagnostics (only print for targets that exist)
foreach(tgt IN ITEMS proxygenhttpserver proxygenlib ${FOLLY_TGT} ${WANGLE_TGT} ${CACHELIB_TGT})
  if(TARGET ${tgt})
    get_target_property(_inc ${tgt} INTERFACE_INCLUDE_DIRECTORIES)
    message(STATUS "${tgt} include dirs: ${_inc}")
  endif()
endforeach()

