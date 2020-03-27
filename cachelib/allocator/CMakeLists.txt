thrift_object("datastruct_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
thrift_object("allocator_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
thrift_object("memory_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/memory/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/memory/serialize" "${CMAKE_CURRENT_SOURCE_DIR}/memory/serialize" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})
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

  datastruct/DList-inl.h
  datastruct/DList.h
  datastruct/MultiDList-inl.h
  datastruct/MultiDList.h
  datastruct/SList-inl.h
  datastruct/SList.h
  $<TARGET_OBJECTS:datastruct_objects-cpp2-obj> 

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

  memory/tests/TestBase.h
  memory/tests/AllocationClassTest.cpp
  memory/tests/MemoryAllocatorTest.cpp
  memory/tests/MemoryPoolManagerTest.cpp
  memory/tests/MemoryPoolTest.cpp
  memory/tests/SlabAllocatorTest.cpp
  memory/tests/TestBase.cpp

  nvmcache/DipperItem.h
  nvmcache/InFlightPuts.h
  nvmcache/NavySetup.h
  nvmcache/NvmCache-inl.h
  nvmcache/NvmCache.h
  nvmcache/ReqContexts.h
  nvmcache/TombStones.h
  nvmcache/WaitContext.h
  nvmcache/DipperItem.cpp
  nvmcache/NavySetup.cpp

  tests/AccessTypeTest.h
  tests/AllocatorHitStatsTest.h
  tests/AllocatorResizeTest.h
  tests/AllocatorTestUtils.h
  tests/BaseAllocatorTest.h
  tests/EventInterfaceTest.h
  tests/MMTypeTest.h
  tests/MultiAllocatorTest.h
  tests/SimplePoolOptimizationTest.h
  tests/SimpleRebalancingTest.h
  tests/TestBase-inl.h
  tests/TestBase.h
  tests/AllocatorHitStatsTypeTest.cpp
  tests/AllocatorResizeTypeTest.cpp
  tests/AllocatorTypeTest.cpp
  tests/CacheBaseTest.cpp
  tests/ChainedHashTest.cpp
  tests/EventInterfaceTest.cpp
  tests/ItemHandleTest.cpp
  tests/ItemTest.cpp
  tests/MM2QTest.cpp
  tests/MMLruTest.cpp
  tests/MMTinyLFUTest.cpp
  tests/MarginalHitsStateTest.cpp
  tests/MultiAllocatorTest.cpp
  tests/NvmCacheStateTest.cpp
  tests/PoolOptimizeStrategyTest.cpp
  tests/RebalanceStrategyTest.cpp
  tests/RefCountTest.cpp
  tests/SimplePoolOptimizationTest.cpp
  tests/SimpleRebalancingTest.cpp
  )

target_link_libraries(
  cachelib_allocator
  PUBLIC
    cachelib_shm
    navy
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}
)

thrift_object("test_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/tests" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/tests" "${CMAKE_CURRENT_SOURCE_DIR}/datastruct/tests" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})


add_executable(
        allocator_test_bin

        tests/AccessTypeTest.h
        tests/AllocatorHitStatsTest.h
        tests/AllocatorResizeTest.h
        tests/AllocatorTestUtils.h
        tests/BaseAllocatorTest.h
        tests/EventInterfaceTest.h
        tests/MMTypeTest.h
        tests/MultiAllocatorTest.h
        tests/SimplePoolOptimizationTest.h
        tests/SimpleRebalancingTest.h
        tests/TestBase-inl.h
        tests/TestBase.h
        tests/AllocatorHitStatsTypeTest.cpp
        tests/AllocatorResizeTypeTest.cpp
        tests/AllocatorTypeTest.cpp
        tests/CacheBaseTest.cpp
        tests/ChainedHashTest.cpp
        tests/EventInterfaceTest.cpp
        tests/ItemHandleTest.cpp
        tests/ItemTest.cpp
        tests/MM2QTest.cpp
        tests/MMLruTest.cpp
        tests/MMTinyLFUTest.cpp
        tests/MarginalHitsStateTest.cpp
        tests/MultiAllocatorTest.cpp
        tests/NvmCacheStateTest.cpp
        tests/PoolOptimizeStrategyTest.cpp
        tests/RebalanceStrategyTest.cpp
        tests/RefCountTest.cpp
        tests/SimplePoolOptimizationTest.cpp
        tests/SimpleRebalancingTest.cpp

        memory/tests/TestBase.h
        memory/tests/AllocationClassTest.cpp
        memory/tests/MemoryAllocatorTest.cpp
        memory/tests/MemoryPoolManagerTest.cpp
        memory/tests/MemoryPoolTest.cpp
        memory/tests/SlabAllocatorTest.cpp
        memory/tests/TestBase.cpp

        nvmcache/tests/NvmTestBase-inl.h
        nvmcache/tests/NvmTestBase.h
        nvmcache/tests/DipperItemTests.cpp
        nvmcache/tests/InFlightPutsTest.cpp
        nvmcache/tests/NvmCacheTests.cpp
        nvmcache/tests/TombStoneTests.cpp

        datastruct/tests/DListTest.cpp
        datastruct/tests/MultiDListTest.cpp
        datastruct/tests/SListTest.cpp
        $<TARGET_OBJECTS:test_objects-cpp2-obj> 

    )

target_link_libraries(
        allocator_test_bin

        PUBLIC
            navy
            cachelib_allocator
            gtest_main
            gmock
            ${FOLLY_LIBRARIES}
)

add_test(
        NAME allocator_tests
        COMMAND allocator_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
