#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121
thrift_object("datastruct_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})

add_library(
  cachelib_allocator_datastruct

  DList-inl.h
  DList.h
  MultiDList-inl.h
  MultiDList.h
  SList-inl.h
  SList.h
  $<TARGET_OBJECTS:allocator_objects-cpp2-obj> 
  )

target_link_libraries(
  cachelib_allocator_datastruct
  PUBLIC
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
    cachelib_allocator_memory
)

#add_subdirectory(tests)
