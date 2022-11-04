# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library (cachelib_cachebench
  ./cache/Cache.cpp
  ./cache/TimeStampTicker.cpp
  ./consistency/LogEventStream.cpp
  ./consistency/ShortThreadId.cpp
  ./consistency/ValueHistory.cpp
  ./consistency/ValueTracker.cpp
  ./runner/FastShutdown.cpp
  ./runner/IntegrationStressor.cpp
  ./runner/ProgressTracker.cpp
  ./runner/Runner.cpp
  ./runner/Stressor.cpp
  ./util/CacheConfig.cpp
  ./util/Config.cpp
  ./util/NandWrites.cpp
  ./workload/PieceWiseCache.cpp
  ./workload/OnlineGenerator.cpp
  ./workload/WorkloadGenerator.cpp
  ./workload/PieceWiseReplayGenerator.cpp
  )
add_dependencies(cachelib_cachebench thrift_generated_files)
target_link_libraries(cachelib_cachebench PUBLIC
  cachelib_datatype
  cachelib_allocator
  gflags
)

if ((CMAKE_SYSTEM_NAME STREQUAL Linux) AND
    (CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64))
else()
  target_compile_definitions(cachelib_cachebench PRIVATE SKIP_OPTION_SIZE_VERIFY)
endif()


add_executable (cachebench main.cpp)
target_link_libraries(cachebench cachelib_cachebench)

install(
  TARGETS
     cachebench
  DESTINATION ${BIN_INSTALL_DIR}
)


if (BUILD_SHARED_LIBS)
  install(
    TARGETS cachelib_cachebench
    EXPORT cachelib-exports
    DESTINATION ${LIB_INSTALL_DIR}
  )
endif ()


if (BUILD_TESTS)
  add_library (cachebench_test_support INTERFACE )
  target_link_libraries (cachebench_test_support INTERFACE
    cachelib_cachebench
    glog::glog
    gflags
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
  )


  function (ADD_TEST SOURCE_FILE)
     generic_add_test("cachebench-test" "${SOURCE_FILE}"
                      cachebench_test_support "${ARGN}")
  endfunction()

  add_test (workload/tests/WorkloadGeneratorTest.cpp)
  add_test (workload/tests/PieceWiseCacheTest.cpp)
  add_test (consistency/tests/RingBufferTest.cpp)
  add_test (consistency/tests/ShortThreadIdTest.cpp)
  add_test (consistency/tests/ValueHistoryTest.cpp)
  add_test (consistency/tests/ValueTrackerTest.cpp)
  add_test (util/tests/NandWritesTest.cpp)
  add_test (cache/tests/TimeStampTickerTest.cpp)
endif()
