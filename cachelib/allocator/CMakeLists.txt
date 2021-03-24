add_thrift_file(SERIALIZE serialize/objects.thrift frozen2)

add_thrift_file(DATASTRUCT_SERIALIZE
                datastruct/serialize/objects.thrift frozen2)

add_thrift_file(DATASTRUCT_TESTS
                datastruct/tests/test_objects.thrift frozen2)

add_thrift_file(MEMORY_SERIALIZE
                memory/serialize/objects.thrift frozen2)

add_library (cachelib_allocator
  ${SERIALIZE_THRIFT_FILES}
  ${DATASTRUCT_SERIALIZE_THRIFT_FILES}
  ${MEMORY_SERIALIZE_THRIFT_FILES}
    CacheAllocator.cpp
    Cache.cpp
    CacheDetails.cpp
    CacheStats.cpp
    CCacheAllocator.cpp
    CCacheManager.cpp
    ContainerTypes.cpp
    FreeMemStrategy.cpp
    HitsPerSlabStrategy.cpp
    LruTailAgeStrategy.cpp
    MarginalHitsOptimizeStrategy.cpp
    MarginalHitsStrategy.cpp
    memory/AllocationClass.cpp
    memory/MemoryAllocator.cpp
    memory/MemoryPool.cpp
    memory/MemoryPoolManager.cpp
    MemoryMonitor.cpp
    memory/SlabAllocator.cpp
    memory/Slab.cpp
    nvmcache/DipperItem.cpp
    nvmcache/NavyConfig.cpp
    nvmcache/NavySetup.cpp
    NvmCacheState.cpp
    PoolOptimizer.cpp
    PoolOptimizeStrategy.cpp
    PoolRebalancer.cpp
    PoolResizer.cpp
    RebalanceStrategy.cpp
    SlabReleaseStats.cpp
    TempShmMapping.cpp
)
add_dependencies(cachelib_allocator thrift_generated_files)
target_link_libraries(cachelib_allocator
  cachelib_navy
  cachelib_common
  cachelib_shm
  )

install(TARGETS cachelib_allocator
        EXPORT cachelib-exports
        DESTINATION ${LIB_INSTALL_DIR} )

if (BUILD_TESTS)
  add_library (allocator_test_support
    ${DATASTRUCT_TESTS_THRIFT_FILES}
    ./nvmcache/tests/NvmTestBase.cpp
    ./memory/tests/TestBase.cpp
    )
  add_dependencies(allocator_test_support thrift_generated_files)
  target_link_libraries (allocator_test_support INTERFACE
    cachelib_allocator
    gmock
    glog
    gflags
    gtest
    gtest_main
  )


  function (ADD_TEST SOURCE_FILE)
    generic_add_test("allocator-test" "${SOURCE_FILE}"
                     allocator_test_support "${ARGN}")
  endfunction()


  add_test (tests/CacheBaseTest.cpp)
  add_test (tests/ItemHandleTest.cpp)
  add_test (tests/ItemTest.cpp)
  add_test (tests/MarginalHitsStateTest.cpp)
  add_test (tests/MM2QTest.cpp)
  add_test (tests/MMLruTest.cpp)
  add_test (tests/MMTinyLFUTest.cpp)
  add_test (tests/NvmCacheStateTest.cpp)
  add_test (tests/RefCountTest.cpp)
  add_test (tests/SimplePoolOptimizationTest.cpp)
  add_test (tests/SimpleRebalancingTest.cpp)
  add_test (tests/PoolOptimizeStrategyTest.cpp)
  add_test (tests/RebalanceStrategyTest.cpp)
  add_test (tests/AllocatorTypeTest.cpp)
  add_test (tests/ChainedHashTest.cpp)
  add_test (tests/AllocatorResizeTypeTest.cpp)
  add_test (tests/AllocatorHitStatsTypeTest.cpp)
  add_test (tests/MultiAllocatorTest.cpp)
  add_test (nvmcache/tests/DipperItemTests.cpp)
  add_test (nvmcache/tests/InFlightPutsTest.cpp)
  add_test (nvmcache/tests/TombStoneTests.cpp)
  add_test (nvmcache/tests/NavySetupTest.cpp)
  add_test (nvmcache/tests/NvmCacheTests.cpp)
  add_test (memory/tests/AllocationClassTest.cpp )
  add_test (memory/tests/MemoryAllocatorTest.cpp )
  add_test (memory/tests/MemoryPoolManagerTest.cpp )
  add_test (memory/tests/MemoryPoolTest.cpp  )
  add_test (memory/tests/SlabAllocatorTest.cpp )
  add_test (datastruct/tests/DListTest.cpp )
  add_test (datastruct/tests/MultiDListTest.cpp )
  add_test (datastruct/tests/SListTest.cpp )
endif()
