add_library(
  common
  
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
  common
  PUBLIC
    thrift
    ${FOLLY_LIBRARIES}
)

add_library(
  bighash

  bighash/BigHash.cpp
  bighash/BloomFilter.cpp
  bighash/Bucket.cpp
  bighash/BucketStorage.cpp
  bighash/BigHash.h
  bighash/BloomFilter.h
  bighash/Bucket.h
  bighash/BucketStorage.h
)

target_link_libraries(
  bighash
  PUBLIC
    common
    thrift
    ${FOLLY_LIBRARIES}
)

add_library(
  serialization

  serialization/Serialization.h
  serialization/RecordIO.cpp
  serialization/RecordIO.h
  serialization/objects.thrift
)

target_link_libraries(
  serialization
  PUBLIC
    common
    thrift
    ${FOLLY_LIBRARIES}
)

add_subdirectory(bighash/tests)
