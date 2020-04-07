add_library(
  cachelib_datatype

  Buffer-inl.h
  Buffer.h
  DataTypes.h
  FixedSizeArray.h
  Map-inl.h
  Map.h
  RangeMap-inl.h
  RangeMap.h
  Buffer.cpp
  DataTypes.cpp
  )

target_link_libraries(
  cachelib_datatype
  PUBLIC

    cachelib_shm
    navy
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
)

add_executable(
        datatype_test_bin

        tests/DataTypeTest.h
        tests/BufferTest.cpp
        tests/DataTypeTest.cpp
        tests/FixedSizeArrayTest.cpp
        tests/MapTest.cpp
        tests/RangeMapTest.cpp
    )

target_link_libraries(
        datatype_test_bin

        PUBLIC
            cachelib_datatype
            navy
            cachelib_allocator
            GTest::GTest
            GTest::Main
            ${GMOCK_LIBRARIES}
            ${FOLLY_LIBRARIES}
)

add_test(
        NAME datatype_tests
        COMMAND datatype_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
