add_library(
  navy_common
  
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
)

target_link_libraries(
  navy_common
  PUBLIC
    ${FOLLY_LIBRARIES}
)

#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121


thrift_object("navy_objects" "" "cpp2" "" "${CMAKE_CURRENT_SOURCE_DIR}/serialization" "${CMAKE_CURRENT_SOURCE_DIR}/serialization" "${CMAKE_CURRENT_SOURCE_DIR}/serialization")
add_library(
  navy_bighash

  bighash/BigHash.cpp
  bighash/BloomFilter.cpp
  bighash/Bucket.cpp
  bighash/BucketStorage.cpp
  bighash/BigHash.h
  bighash/BloomFilter.h
  bighash/Bucket.h
  bighash/BucketStorage.h
  $<TARGET_OBJECTS:navy_objects-cpp2-obj> 
)

target_link_libraries(
  navy_bighash
  PUBLIC
    navy_common
#    thrift
    ${FOLLY_LIBRARIES}
    ${FBTHRIFT_LIBRARIES}

)
add_library(
  navy_serialization

  serialization/Serialization.h
  serialization/RecordIO.cpp
  serialization/RecordIO.h
  serialization/navy_objects.thrift
  $<TARGET_OBJECTS:navy_objects-cpp2-obj> 
)

target_link_libraries(
  navy_serialization
  PUBLIC
    navy_common
    ${FOLLY_LIBRARIES}
)

add_library(
        navy_scheduler

        scheduler/JobScheduler.h
        scheduler/ThreadPoolJobScheduler.h
        scheduler/ThreadPoolJobScheduler.cpp
)

target_link_libraries(
        navy_scheduler
        PUBLIC
            navy_common
            ${FOLLY_LIBRARIES}

)

add_library(
        navy_testing

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
        navy_testing
        PUBLIC
            navy_common
            navy_scheduler
            ${FOLLY_LIBRARIES}

)

add_subdirectory(bighash/tests)
add_subdirectory(common/tests)
