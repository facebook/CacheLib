add_thrift_file(SHM shm.thrift frozen2)

add_library (cachelib_shm
  ${SHM_THRIFT_FILES}
  PosixShmSegment.cpp
  ShmCommon.cpp
  ShmManager.cpp
  SysVShmSegment.cpp
  )
add_dependencies(cachelib_shm thrift_generated_files)
target_link_libraries(cachelib_shm PUBLIC
  cachelib_common
)

install(TARGETS cachelib_shm
        EXPORT cachelib-exports
        DESTINATION ${LIB_INSTALL_DIR} )

if (BUILD_TESTS)
  add_library (shm_test_support
    tests/common.cpp)
  target_link_libraries (shm_test_support PUBLIC
    cachelib_shm
    glog::glog
    gflags
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
  )

  function (ADD_TEST SOURCE_FILE)
     generic_add_test("shm-test" "${SOURCE_FILE}" shm_test_support "${ARGN}")
  endfunction()

  add_test (tests/test_page_size.cpp)
  add_test (tests/test_posix.cpp)
  add_test (tests/test_shm.cpp)
  add_test (tests/test_shm_manager.cpp)
  add_test (tests/test_sysv.cpp)
endif()
