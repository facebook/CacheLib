#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121
thrift_object("memory_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
message("HERE ${CMAKE_CURRENT_SOURCE_DIR}/")
add_library(
  cachelib_allocator_memory

  AllocationClass.h
  CompressedPtr.h
  MemoryAllocator.h
  MemoryAllocatorStats.h
  MemoryPool.h
  MemoryPoolManager.h
  Slab.h
  SlabAllocator.h
  AllocationClass.cpp
  MemoryAllocator.cpp
  MemoryPool.cpp
  MemoryPoolManager.cpp
  Slab.cpp
  SlabAllocator.cpp
  $<TARGET_OBJECTS:memory_objects-cpp2-obj> 
  )

target_link_libraries(
  cachelib_allocator_memory
  PUBLIC
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
    cachelib_allocator_datastruct
)



#add_subdirectory(tests)
