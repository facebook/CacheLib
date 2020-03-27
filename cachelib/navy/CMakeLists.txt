#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121
thrift_object("navy_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/serialization" "${CMAKE_CURRENT_SOURCE_DIR}/serialization" "${CMAKE_CURRENT_SOURCE_DIR}/serialization")

add_library(
    navy
    
    AbstractCache.h
    Factory.h
    Factory.cpp

    admission_policy/AdmissionPolicy.h
    admission_policy/DynamicRandomAP.h
    admission_policy/RejectRandomAP.h
    admission_policy/DynamicRandomAP.cpp
    admission_policy/RejectRandomAP.cpp

    bighash/BigHash.cpp
    bighash/BloomFilter.cpp
    bighash/Bucket.cpp
    bighash/BucketStorage.cpp
    bighash/BigHash.h
    bighash/BloomFilter.h
    bighash/Bucket.h
    bighash/BucketStorage.h
    $<TARGET_OBJECTS:navy_objects-cpp2-obj> 

    block_cache/Allocator.h
    block_cache/BTree.h
    block_cache/BlockCache.h
    block_cache/EvictionPolicy.h
    block_cache/FifoPolicy.h
    block_cache/HitsReinsertionPolicy.h
    block_cache/Index.h
    block_cache/LruPolicy.h
    block_cache/ProbabilisticReinsertionPolicy.h
    block_cache/Region.h
    block_cache/RegionManager.h
    block_cache/ReinsertionPolicy.h
    block_cache/Types.h
    block_cache/Allocator.cpp
    block_cache/BlockCache.cpp
    block_cache/FifoPolicy.cpp
    block_cache/HitsReinsertionPolicy.cpp
    block_cache/Index.cpp
    block_cache/LruPolicy.cpp
    block_cache/Region.cpp
    block_cache/RegionManager.cpp

    common/Buffer.cpp
    common/Buffer.h
    common/CompilerUtils.h
    common/Device.cpp
    common/Device.h
    common/Hash.cpp
    common/Hash.h
    common/SizeDistribution.cpp
    common/SizeDistribution.h
    common/Types.cpp
    common/Types.h
    common/Utils.h

    driver/Driver.h
    driver/NoopEngine.h
    driver/Driver.cpp

    serialization/Serialization.h
    serialization/RecordIO.cpp
    serialization/RecordIO.h
    serialization/navy_objects.thrift

    scheduler/JobScheduler.h
    scheduler/ThreadPoolJobScheduler.h
    scheduler/ThreadPoolJobScheduler.cpp

    testing/BufferGen.cpp
    testing/BufferGen.h
    testing/Callbacks.h
    testing/MockDevice.h
    testing/MockDevice.cpp
    testing/MockJobScheduler.cpp
    testing/MockJobScheduler.h
    testing/SeqPoints.cpp
    testing/SeqPoints.h
)
target_link_libraries(
        navy
        PUBLIC
        ${FOLLY_LIBRARIES}
        ${FBTHRIFT_LIBRARIES}

)
add_executable(
        navy_test_bin

        admission_policy/tests/DynamicRandomAPTest.cpp
        admission_policy/tests/RejectRandomAPTest.cpp

        bighash/tests/BigHashTest.cpp
        bighash/tests/BloomFilterTest.cpp
        bighash/tests/BucketStorageTest.cpp
        bighash/tests/BucketTest.cpp

        block_cache/tests/MockPolicy.h
        block_cache/tests/AllocatorTest.cpp
        block_cache/tests/BTreeTest.cpp
        block_cache/tests/BlockCacheTest.cpp
        block_cache/tests/FifoPolicyTest.cpp
        block_cache/tests/HitsReinsertionPolicyTest.cpp
        block_cache/tests/IndexTest.cpp
        block_cache/tests/LruPolicyTest.cpp
        block_cache/tests/RegionManagerTest.cpp
        block_cache/tests/RegionTest.cpp

        common/tests/BufferTest.cpp
        common/tests/DeviceTest.cpp
        common/tests/HashTest.cpp
        common/tests/UtilsTest.cpp

        driver/tests/DriverTest.cpp

        scheduler/tests/OrderedThreadPoolJobSchedulerTest.cpp
        scheduler/tests/ThreadPoolJobSchedulerTest.cpp
        serialization/tests/RecordIOTest.cpp
        serialization/tests/SerializationTest.cpp

        testing/tests/BufferGenTest.cpp
        testing/tests/MockJobSchedulerTest.cpp
        testing/tests/SeqPointsTest.cpp
)

target_link_libraries(
        navy_test_bin

        PUBLIC
            navy
            cachelib_common
            gtest_main
            gmock
            ${FOLLY_LIBRARIES}
            ${FBTHRIFT_LIBRARIES}
)

add_test(
        NAME navy_tests
        COMMAND navy_test_bin
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
