# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    nvmcache/NvmItem.cpp
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
target_link_libraries(cachelib_allocator PUBLIC
  cachelib_navy
  cachelib_common
  cachelib_shm
  )

if ((CMAKE_SYSTEM_NAME STREQUAL Linux) AND
    (CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64))
else()
  target_compile_definitions(cachelib_allocator PRIVATE SKIP_SIZE_VERIFY)
endif()


install(TARGETS cachelib_allocator
        EXPORT cachelib-exports
        DESTINATION ${LIB_INSTALL_DIR} )

if (BUILD_TESTS)
  add_library (allocator_test_support OBJECT
    ${DATASTRUCT_TESTS_THRIFT_FILES}
    ./nvmcache/tests/NvmTestBase.cpp
    ./memory/tests/TestBase.cpp
    )
  add_dependencies(allocator_test_support thrift_generated_files)
  target_link_libraries (allocator_test_support PUBLIC
    cachelib_allocator
    common_test_utils
    glog::glog
    gflags
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
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
  add_test (tests/NvmAdmissionPolicyTest.cpp)
  add_test (tests/CacheAllocatorConfigTest.cpp)
  add_test (nvmcache/tests/NvmItemTests.cpp)
  add_test (nvmcache/tests/InFlightPutsTest.cpp)
  add_test (nvmcache/tests/TombStoneTests.cpp)
  add_test (nvmcache/tests/NavySetupTest.cpp)
  add_test (nvmcache/tests/NvmCacheTests.cpp)
  add_test (nvmcache/tests/NavyConfigTest.cpp)
  add_test (memory/tests/AllocationClassTest.cpp )
  add_test (memory/tests/MemoryAllocatorTest.cpp )
  add_test (memory/tests/MemoryPoolManagerTest.cpp )
  add_test (memory/tests/MemoryPoolTest.cpp  )
  add_test (memory/tests/SlabAllocatorTest.cpp )
  add_test (datastruct/tests/DListTest.cpp )
  add_test (datastruct/tests/MultiDListTest.cpp )
  add_test (datastruct/tests/SListTest.cpp )

endif()
