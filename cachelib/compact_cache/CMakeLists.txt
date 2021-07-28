add_library (compact_cache_test_support INTERFACE)
add_dependencies(compact_cache_test_support INTERFACE thrift_generated_files)

if (BUILD_TESTS)
  target_link_libraries (compact_cache_test_support INTERFACE
    cachelib_allocator
    glog::glog
    gflags
    GTest::gtest
    GTest::gmock
  )

  function (ADD_TEST SOURCE_FILE)
     generic_add_test("compact-cache-test" "${SOURCE_FILE}"
                      compact_cache_test_support "${ARGN}")
  endfunction()

  add_test (tests/CCacheTests.cpp)
endif()
