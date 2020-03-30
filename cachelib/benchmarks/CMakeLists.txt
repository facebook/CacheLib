thrift_object("DataTypeBench" "" "cpp2" "frozen" "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" THRIFT_INCLUDE_DIRECTORIES ${PROJECT_SOURCE_DIR})


add_executable(
  benchmarks_mmtype

  MMTypeBench.h
  MMTypeBench.cpp
)
target_link_libraries(
  benchmarks_mmtype

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_binary_vs_hash_table   

  BinarySearchVsHashTableBench.cpp
    )
target_link_libraries(
    benchmarks_binary_vs_hash_table   

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_bucket_mutex

  BucketMutexBench.cpp
  
  )
target_link_libraries(
    benchmarks_bucket_mutex

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_bytes_equal

  BytesEqualBenchmark.cpp
  )
target_link_libraries(
    benchmarks_bytes_equal

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_map_operation

  CachelibMapOperationBench.cpp 
  )
target_link_libraries(
    benchmarks_map_operation

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_map_workload

  CachelibMapWorkloadBench.cpp
  $<TARGET_OBJECTS:DataTypeBench-cpp2-obj>
  )
target_link_libraries(
    benchmarks_map_workload

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_range_map_workload

  CachelibRangeMapWorkloadBench.cpp
  $<TARGET_OBJECTS:DataTypeBench-cpp2-obj>
  )
target_link_libraries(
    benchmarks_range_map_workload

  PUBLIC
  cachelib_allocator
  cachelib_common
  cachelib_datatype
  ${FOLLY_LIBRARIES}
  ${FBTHRIFT_LIBRARIES}
)

add_executable(
    benchmarks_compact_cache

  CompactCacheBench.cpp
  )
target_link_libraries(
    benchmarks_compact_cache

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_items_reaper

  ItemsReaperBench.cpp
  )
target_link_libraries(
    benchmarks_items_reaper

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_mmtype_access

  MMTypeAccessBench.cpp
  )
target_link_libraries(
    benchmarks_mmtype_access

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_mutex

  MutexBench.cpp
  )
target_link_libraries(
    benchmarks_mutex

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_ptr_compression

  PtrCompressionBench.cpp
  )
 target_link_libraries(
    benchmarks_ptr_compression

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
) 

  add_executable(
    benchmarks_slist

  SListBench.cpp
  )
target_link_libraries(
    benchmarks_slist

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

add_executable(
    benchmarks_thread_local

  ThreadLocalBench.cpp
  )
target_link_libraries(
    benchmarks_thread_local

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)

  add_executable(
    benchmarks_tl-bench

  tl-bench/main.cpp
)
target_link_libraries(
    benchmarks_tl-bench

  PUBLIC
  ${FOLLY_LIBRARIES}
  cachelib_allocator
)
