#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121
thrift_object("datastruct_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
add_library(
  cachelib_allocator_datastruct

  datastruct/DList-inl.h
  datastruct/DList.h
  datastruct/MultiDList-inl.h
  datastruct/MultiDList.h
  datastruct/SList-inl.h
  datastruct/SList.h
  $<TARGET_OBJECTS:datastruct_objects-cpp2-obj> 
  )

target_link_libraries(
  cachelib_allocator_datastruct
  PUBLIC
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
)

thrift_object("allocator_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})

add_library(
  cachelib_allocator

  CCacheAllocator.h
  CCacheManager.h
  Cache.h
  CacheAllocator-inl.h
  CacheAllocator.h
  CacheAllocatorConfig.h
  CacheChainedItemIterator.h
  CacheDetails.h
  CacheItem-inl.h
  CacheItem.h
  CacheStats.h
  CacheStatsInternal.h
  CacheTraits.h
  CacheVersion.h
  ChainedAllocs.h
  ChainedHashTable-inl.h
  ChainedHashTable.h
  EventInterface.h
  FreeMemStrategy.h
  Handle.h
  HitsPerSlabStrategy.h
  ICompactCache.h
  KAllocation.h
  LruTailAgeStrategy.h
  MM2Q-inl.h
  MM2Q.h
  MMLru-inl.h
  MMLru.h
  MMTinyLFU-inl.h
  MMTinyLFU.h
  MarginalHitsOptimizeStrategy.h
  MarginalHitsState-inl.h
  MarginalHitsState.h
  MarginalHitsStrategy.h
  MemoryMappedFile.h
  MemoryMonitor.h
  NvmAdmissionPolicy.h
  NvmCacheState.h
  PoolOptimizeStrategy.h
  PoolOptimizer.h
  PoolRebalancer.h
  PoolResizeStrategy.h
  PoolResizer.h
  RandomStrategy.h
  ReadOnlySharedCacheView.h
  Reaper-inl.h
  Reaper.h
  RebalanceInfo.h
  RebalanceStrategy.h
  Refcount-inl.h
  Refcount.h
  SlabReleaseStats.h
  TempShmMapping.h
  TlsActiveItemRing.h
  TypedHandle.h
  Util.h
  CCacheAllocator.cpp
  CCacheManager.cpp
  Cache.cpp
  CacheAllocator.cpp
  CacheDetails.cpp
  CacheStats.cpp
  ContainerTypes.cpp
  FreeMemStrategy.cpp
  HitsPerSlabStrategy.cpp
  LruTailAgeStrategy.cpp
  MarginalHitsOptimizeStrategy.cpp
  MarginalHitsStrategy.cpp
  MemoryMonitor.cpp
  NvmCacheState.cpp
  PoolOptimizeStrategy.cpp
  PoolOptimizer.cpp
  PoolRebalancer.cpp
  PoolResizer.cpp
  RebalanceStrategy.cpp
  SlabReleaseStats.cpp
  TempShmMapping.cpp
  $<TARGET_OBJECTS:allocator_objects-cpp2-obj> 
  )

target_link_libraries(
  cachelib_allocator
  PUBLIC
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
    cachelib_allocator_datastruct
    cachelib_allocator_memory
)


#add_subdirectory(datastruct)


#add_subdirectory(memory)
thrift_object("memory_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/memory/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/memory/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/memory/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
message("HERE ${CMAKE_CURRENT_SOURCE_DIR}/")
add_library(
  cachelib_allocator_memory

  memory/AllocationClass.h
  memory/CompressedPtr.h
  memory/MemoryAllocator.h
  memory/MemoryAllocatorStats.h
  memory/MemoryPool.h
  memory/MemoryPoolManager.h
  memory/Slab.h
  memory/SlabAllocator.h
  memory/AllocationClass.cpp
  memory/MemoryAllocator.cpp
  memory/MemoryPool.cpp
  memory/MemoryPoolManager.cpp
  memory/Slab.cpp
  memory/SlabAllocator.cpp
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
