add_thrift_file(SERIALIZATION serialization/objects.thrift json)

add_library (cachelib_navy
  ${SERIALIZATION_THRIFT_FILES}
  admission_policy/DynamicRandomAP.cpp
  admission_policy/RejectRandomAP.cpp
  bighash/BigHash.cpp
  bighash/Bucket.cpp
  bighash/BucketStorage.cpp
  block_cache/Allocator.cpp
  block_cache/BlockCache.cpp
  block_cache/FifoPolicy.cpp
  block_cache/HitsReinsertionPolicy.cpp
  block_cache/Index.cpp
  block_cache/LruPolicy.cpp
  block_cache/Region.cpp
  block_cache/RegionManager.cpp
  common/Buffer.cpp
  common/Device.cpp
  common/Hash.cpp
  common/SizeDistribution.cpp
  common/Types.cpp
  driver/Driver.cpp
  Factory.cpp
  kangaroo/ChainedLogIndex.cpp
  kangaroo/Kangaroo.cpp
  kangaroo/KangarooBucketStorage.cpp
  kangaroo/KangarooLog.cpp
  kangaroo/KangarooLogSegment.cpp
  kangaroo/LogBucket.cpp
  kangaroo/RripBucket.cpp
  kangaroo/RripBitVector.cpp
  kangaroo/RripBucketStorage.cpp
  kangaroo/KangarooSizeDistribution.cpp
  scheduler/ThreadPoolJobScheduler.cpp
  scheduler/ThreadPoolJobQueue.cpp
  serialization/RecordIO.cpp
  )
add_dependencies(cachelib_navy thrift_generated_files)

target_link_libraries(cachelib_navy
  cachelib_common
  )

install(TARGETS cachelib_navy
        EXPORT cachelib-exports
        DESTINATION ${LIB_INSTALL_DIR} )

if (BUILD_TESTS)
  add_library(navy_test_support
    testing/BufferGen.cpp
    testing/MockDevice.cpp
    testing/MockJobScheduler.cpp
    testing/SeqPoints.cpp
    )
  add_dependencies(navy_test_support thrift_generated_files)
  target_link_libraries(navy_test_support
    cachelib_navy
    gmock
    glog
    gflags
    gtest
    gtest_main
  )


  function (ADD_TEST SOURCE_FILE)
     generic_add_test("navy-test" "${SOURCE_FILE}" navy_test_support "${ARGN}")
  endfunction()

  add_test (common/tests/BufferTest.cpp)
  add_test (common/tests/HashTest.cpp)
  add_test (common/tests/UtilsTest.cpp)
  add_test (bighash/tests/BucketStorageTest.cpp)
  add_test (bighash/tests/BucketTest.cpp)
  add_test (admission_policy/tests/DynamicRandomAPTest.cpp)
  add_test (admission_policy/tests/RejectRandomAPTest.cpp)
  add_test (block_cache/tests/FifoPolicyTest.cpp)
  add_test (block_cache/tests/HitsReinsertionPolicyTest.cpp)
  add_test (block_cache/tests/IndexTest.cpp)
  add_test (block_cache/tests/LruPolicyTest.cpp)
  add_test (block_cache/tests/RegionTest.cpp)
  add_test (kangaroo/tests/ChainedLogIndexTest.cpp)
  add_test (kangaroo/tests/KangarooLogSegmentTest.cpp)
  add_test (kangaroo/tests/KangarooTest.cpp)
  add_test (kangaroo/tests/KangarooLogTest.cpp)
  add_test (kangaroo/tests/RripBucketStorageTest.cpp)
  add_test (kangaroo/tests/RripBitVectorTest.cpp)
  add_test (kangaroo/tests/RripBucketTest.cpp)
  add_test (kangaroo/tests/LogBucketTest.cpp)
  add_test (kangaroo/tests/KangarooBucketStorageTest.cpp)
  add_test (serialization/tests/RecordIOTest.cpp)
  add_test (serialization/tests/SerializationTest.cpp)
  add_test (scheduler/tests/OrderedThreadPoolJobSchedulerTest.cpp)
  add_test (scheduler/tests/ThreadPoolJobSchedulerTest.cpp)
  add_test (driver/tests/DriverTest.cpp)
  add_test (common/tests/DeviceTest.cpp)
  add_test (block_cache/tests/AllocatorTest.cpp)
  add_test (block_cache/tests/RegionManagerTest.cpp)
  add_test (testing/tests/BufferGenTest.cpp)
  add_test (testing/tests/MockJobSchedulerTest.cpp)
  add_test (testing/tests/SeqPointsTest.cpp)
  add_test (block_cache/tests/BlockCacheTest.cpp)
  add_test (bighash/tests/BigHashTest.cpp)
endif()
