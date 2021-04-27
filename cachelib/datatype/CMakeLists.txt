# Create a library called "Hello" which includes the source file "hello.cxx".
# The extension is already found. Any number of sources could be listed here.
add_library (cachelib_datatype
  Buffer.cpp
  DataTypes.cpp
  )
add_dependencies(cachelib_datatype thrift_generated_files)

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
    gmock
    glog
    gflags
    gtest
    gtest_main
  )

  function (ADD_TEST SOURCE_FILE)
     generic_add_test("datatype-test" "${SOURCE_FILE}"
                      datatype_test_support "${ARGN}")
  endfunction()

  add_test (tests/RangeMapTest.cpp)
  add_test (tests/BufferTest.cpp)
  add_test (tests/FixedSizeArrayTest.cpp)
  add_test (tests/MapTest.cpp)
  add_test (tests/MapViewTest.cpp)
endif()
