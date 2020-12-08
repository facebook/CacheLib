add_library (compact_cache_test_support INTERFACE)
add_dependencies(compact_cache_test_support thrift_generated_files)
target_link_libraries (compact_cache_test_support INTERFACE
  cachelib_allocator
  gmock
  glog
  gflags
  gtest
)

function (ADD_TEST SOURCE_FILE)
   generic_add_test("compact-cache-test" "${SOURCE_FILE}"
                    compact_cache_test_support "${ARGN}")
endfunction()

add_test (tests/CCacheTests.cpp)
