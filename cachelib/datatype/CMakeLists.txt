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

# Create a library called "Hello" which includes the source file "hello.cxx".
# The extension is already found. Any number of sources could be listed here.
add_library (cachelib_datatype
  Buffer.cpp
  )
add_dependencies(cachelib_datatype thrift_generated_files)
target_link_libraries(cachelib_datatype PUBLIC
  cachelib_common
)

install(TARGETS cachelib_datatype
        EXPORT cachelib-exports
        DESTINATION ${LIB_INSTALL_DIR} )

if (BUILD_TESTS)
  add_library (datatype_test_support
    tests/DataTypeTest.cpp)
  add_dependencies(datatype_test_support cachelib_datatype)

  target_link_libraries (datatype_test_support
    cachelib_datatype
    cachelib_allocator
    glog::glog
    gflags
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
  )

  function (ADD_TEST SOURCE_FILE)
     generic_add_test("datatype-test" "${SOURCE_FILE}"
                      datatype_test_support "${ARGN}")
  endfunction()

  add_test (tests/RangeMapTest.cpp)
  add_test (tests/BufferTest.cpp)
  add_test (tests/FixedSizeArrayTest.cpp)
  add_test (tests/MapTest.cpp)
  # Temporary disabled due to compilation error with GCC
  # add_test (tests/MapViewTest.cpp)
endif()
